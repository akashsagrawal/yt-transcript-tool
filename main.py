"""
YT Transcript Tool
==================

A small FastAPI service that pulls transcripts / subtitles out of YouTube
videos and returns them as plain text or SRT.

Why there is more here than "call the library":

  PROXY  YouTube blocks requests coming from datacenter IP addresses (Render,
         AWS, Google Cloud, ...). Run this on a cloud host with no proxy and
         you get intermittent "blocked" errors. Routing through a residential
         proxy is the standard fix. Configured via environment variables.

  CACHE  Every uncached request costs one proxy request (money) and a couple
         of seconds. The same video asked for twice is served from memory.

  LIMIT  One visitor -- or one bot -- hammering the URL should not be able to
         run up the proxy bill. Requests are capped per client IP.

Nothing secret is hard-coded: all configuration comes from environment
variables, so the same file is safe to keep in a public git repo.
"""

import os
import re
import threading
import time
import zipfile
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO, StringIO

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from youtube_transcript_api import (
    IpBlocked,
    NoTranscriptFound,
    RequestBlocked,
    TranscriptsDisabled,
    VideoUnavailable,
    YouTubeTranscriptApi,
)
from youtube_transcript_api.proxies import GenericProxyConfig, WebshareProxyConfig


# ---------------------------------------------------------------------------
# Configuration (all via environment variables)
# ---------------------------------------------------------------------------

def _env_int(name: str, default: int) -> int:
    """Read an int from the environment, falling back if unset or garbage."""
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


# Webshare (recommended -- the transcript library has first-class support).
WEBSHARE_USERNAME = os.environ.get("WEBSHARE_PROXY_USERNAME", "").strip()
WEBSHARE_PASSWORD = os.environ.get("WEBSHARE_PROXY_PASSWORD", "").strip()

# Any other proxy provider, as full URLs e.g. http://user:pass@host:port
GENERIC_HTTP_PROXY = os.environ.get("HTTP_PROXY_URL", "").strip()
GENERIC_HTTPS_PROXY = os.environ.get("HTTPS_PROXY_URL", "").strip()

CACHE_TTL_SECONDS = _env_int("CACHE_TTL_SECONDS", 60 * 60 * 24)  # 24 hours
CACHE_MAX_ENTRIES = _env_int("CACHE_MAX_ENTRIES", 500)

RATE_LIMIT_REQUESTS = _env_int("RATE_LIMIT_REQUESTS", 20)
RATE_LIMIT_WINDOW_SECONDS = _env_int("RATE_LIMIT_WINDOW_SECONDS", 60)

# Bulk mode: how many links one request may carry, and how many of them to
# fetch at the same time. Keep concurrency modest -- hammering YouTube through
# the proxy with 20 parallel requests is exactly how you get blocked.
BULK_MAX_URLS = _env_int("BULK_MAX_URLS", 25)
BULK_CONCURRENCY = max(1, _env_int("BULK_CONCURRENCY", 4))

# Comma-separated list, or "*" for any origin.
ALLOWED_ORIGINS = [
    o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()
]


def build_proxy_config():
    """Pick a proxy configuration from the environment, or None for direct."""
    if WEBSHARE_USERNAME and WEBSHARE_PASSWORD:
        return WebshareProxyConfig(
            proxy_username=WEBSHARE_USERNAME,
            proxy_password=WEBSHARE_PASSWORD,
        )
    if GENERIC_HTTP_PROXY or GENERIC_HTTPS_PROXY:
        return GenericProxyConfig(
            http_url=GENERIC_HTTP_PROXY or GENERIC_HTTPS_PROXY,
            https_url=GENERIC_HTTPS_PROXY or GENERIC_HTTP_PROXY,
        )
    return None


PROXY_CONFIG = build_proxy_config()
PROXY_MODE = (
    "webshare" if WEBSHARE_USERNAME and WEBSHARE_PASSWORD
    else "generic" if (GENERIC_HTTP_PROXY or GENERIC_HTTPS_PROXY)
    else "none"
)

api = YouTubeTranscriptApi(proxy_config=PROXY_CONFIG)


# ---------------------------------------------------------------------------
# A tiny TTL + LRU cache
#
# Deliberately dependency-free and in-memory. That means the cache is empty
# after every restart/redeploy and is not shared between instances -- which is
# fine for a single small service. If this ever needs to scale out, the swap
# is to Redis and only this class changes.
# ---------------------------------------------------------------------------

class TTLCache:
    def __init__(self, ttl_seconds: int, max_entries: int):
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self._data: "OrderedDict[tuple, tuple[float, object]]" = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key):
        now = time.time()
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                self.misses += 1
                return None
            stored_at, value = entry
            if now - stored_at > self.ttl:
                del self._data[key]
                self.misses += 1
                return None
            self._data.move_to_end(key)  # mark as recently used
            self.hits += 1
            return value

    def set(self, key, value):
        with self._lock:
            self._data[key] = (time.time(), value)
            self._data.move_to_end(key)
            while len(self._data) > self.max_entries:
                self._data.popitem(last=False)  # drop least recently used

    def stats(self):
        with self._lock:
            return {
                "entries": len(self._data),
                "max_entries": self.max_entries,
                "ttl_seconds": self.ttl,
                "hits": self.hits,
                "misses": self.misses,
            }


transcript_cache = TTLCache(CACHE_TTL_SECONDS, CACHE_MAX_ENTRIES)
language_cache = TTLCache(CACHE_TTL_SECONDS, CACHE_MAX_ENTRIES)


# ---------------------------------------------------------------------------
# Per-IP rate limiting (sliding window, in-memory)
# ---------------------------------------------------------------------------

class RateLimiter:
    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window = window_seconds
        self._hits: "dict[str, deque]" = {}
        self._lock = threading.Lock()

    def check(self, key: str, cost: int = 1) -> tuple[bool, int]:
        """Return (allowed, seconds_until_retry).

        `cost` lets one HTTP request consume several slots -- a bulk job for 10
        videos costs 10, because it does 10 YouTube fetches. Without this, bulk
        mode would be a hole straight through the rate limit."""
        if self.max_requests <= 0:  # 0 or negative disables the limiter
            return True, 0
        cost = max(1, cost)
        now = time.time()
        cutoff = now - self.window
        with self._lock:
            bucket = self._hits.setdefault(key, deque())
            while bucket and bucket[0] < cutoff:
                bucket.popleft()

            if len(bucket) + cost > self.max_requests:
                retry_after = int(bucket[0] + self.window - now) + 1 if bucket else 1
                return False, max(retry_after, 1)

            bucket.extend([now] * cost)

            # Occasional housekeeping so idle IPs don't accumulate forever.
            if len(self._hits) > 5000:
                for stale_key in [k for k, v in self._hits.items() if not v]:
                    del self._hits[stale_key]
            return True, 0


rate_limiter = RateLimiter(RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW_SECONDS)


def client_ip(request: Request) -> str:
    """Real client IP. On Render (and most hosts) we sit behind a proxy, so
    request.client.host is the load balancer -- X-Forwarded-For is the client."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def charge_rate_limit(request: Request, cost: int = 1) -> None:
    """Consume `cost` slots for this caller, or raise 429."""
    allowed, retry_after = rate_limiter.check(client_ip(request), cost=cost)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=(
                f"Too many requests. Limit is {RATE_LIMIT_REQUESTS} per "
                f"{RATE_LIMIT_WINDOW_SECONDS} seconds"
                + (f" and this job needs {cost}" if cost > 1 else "")
                + f". Try again in {retry_after} seconds."
            ),
            headers={"Retry-After": str(retry_after)},
        )


def enforce_rate_limit(request: Request) -> None:
    """FastAPI dependency for single-video routes. Attached to the API routes
    only, so the static frontend is never rate limited."""
    charge_rate_limit(request, cost=1)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="YT Transcript Tool", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET"],
    allow_headers=["*"],
)


VIDEO_ID_PATTERNS = [
    r"(?:v=|/videos/|embed/|youtu\.be/|/v/|/shorts/|/live/)([0-9A-Za-z_-]{11})",
    r"^([0-9A-Za-z_-]{11})$",  # bare video id
]


def extract_video_id(url_or_id: str) -> str:
    """Pull an 11-character YouTube video ID out of any common URL format,
    or pass a bare ID straight through."""
    url_or_id = (url_or_id or "").strip()
    for pattern in VIDEO_ID_PATTERNS:
        match = re.search(pattern, url_or_id)
        if match:
            return match.group(1)
    raise HTTPException(
        status_code=400,
        detail="Could not find a YouTube video ID in that input.",
    )


def format_timestamp(seconds: float) -> str:
    ms = int(round((seconds - int(seconds)) * 1000))
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02}:{m:02}:{s:02},{ms:03}"


def to_srt(snippets) -> str:
    buf = StringIO()
    for i, snip in enumerate(snippets, start=1):
        start = format_timestamp(snip["start"])
        end = format_timestamp(snip["start"] + snip["duration"])
        buf.write(f"{i}\n{start} --> {end}\n{snip['text']}\n\n")
    return buf.getvalue()


def to_plain_text(snippets) -> str:
    return "\n".join(snip["text"] for snip in snippets)


def as_plain_snippets(fetched) -> list:
    """Convert the library's objects into plain dicts so they are safe to keep
    in the cache and independent of the library's internal classes."""
    return [
        {"text": s.text, "start": float(s.start), "duration": float(s.duration)}
        for s in fetched.snippets
    ]


def raise_for_transcript_error(exc: Exception) -> None:
    """Translate library exceptions into clear HTTP errors."""
    if isinstance(exc, TranscriptsDisabled):
        raise HTTPException(
            status_code=404,
            detail="This video has captions turned off, so there is no transcript to fetch.",
        )
    if isinstance(exc, NoTranscriptFound):
        raise HTTPException(
            status_code=404,
            detail="No transcript found for this video in the requested language.",
        )
    if isinstance(exc, VideoUnavailable):
        raise HTTPException(
            status_code=404,
            detail="That video is unavailable (private, deleted, or region blocked).",
        )
    if isinstance(exc, (RequestBlocked, IpBlocked)):
        hint = (
            "YouTube is blocking this server's IP address. "
            + (
                "The proxy is configured but was still blocked -- check that the "
                "proxy account has bandwidth left."
                if PROXY_CONFIG
                else "No proxy is configured. Set WEBSHARE_PROXY_USERNAME and "
                     "WEBSHARE_PROXY_PASSWORD in the environment (see README)."
            )
        )
        raise HTTPException(status_code=503, detail=hint)
    raise HTTPException(status_code=502, detail=f"Failed to fetch transcript: {exc}")


def fetch_snippets(video_id: str, lang: str | None) -> list:
    """Fetch (or serve from cache) the transcript snippets for a video."""
    cache_key = (video_id, lang or "auto")
    cached = transcript_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        if lang:
            fetched = api.fetch(video_id, languages=[lang])
        else:
            try:
                # Fast path: one request, English if it exists.
                fetched = api.fetch(video_id)
            except NoTranscriptFound:
                # No English track -- ask what does exist and take the first.
                transcript_list = api.list(video_id)
                available = list(transcript_list)
                if not available:
                    raise
                fetched = available[0].fetch()
    except Exception as exc:  # noqa: BLE001 -- re-raised as HTTPException below
        raise_for_transcript_error(exc)

    snippets = as_plain_snippets(fetched)
    transcript_cache.set(cache_key, snippets)
    return snippets


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    """Quick way to confirm the deploy is alive and see how it is configured."""
    return {
        "status": "ok",
        "proxy_mode": PROXY_MODE,
        "rate_limit": {
            "requests": RATE_LIMIT_REQUESTS,
            "window_seconds": RATE_LIMIT_WINDOW_SECONDS,
        },
        "bulk": {"max_urls": BULK_MAX_URLS, "concurrency": BULK_CONCURRENCY},
        "transcript_cache": transcript_cache.stats(),
        "language_cache": language_cache.stats(),
    }


@app.get("/api/transcript", dependencies=[Depends(enforce_rate_limit)])
def get_transcript(
    url: str = Query(..., description="YouTube URL or video ID"),
    fmt: str = Query("txt", pattern="^(txt|srt)$"),
    lang: str | None = Query(None, description="Preferred language code, e.g. 'en'"),
):
    video_id = extract_video_id(url)
    snippets = fetch_snippets(video_id, lang)

    content = to_srt(snippets) if fmt == "srt" else to_plain_text(snippets)
    media_type = "text/plain" if fmt == "txt" else "application/x-subrip"
    filename = f"{video_id}.{fmt}"

    return StreamingResponse(
        iter([content]),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/languages", dependencies=[Depends(enforce_rate_limit)])
def get_languages(url: str = Query(...)):
    """List which transcript languages are available for a video."""
    video_id = extract_video_id(url)

    cached = language_cache.get(video_id)
    if cached is not None:
        return {"video_id": video_id, "available": cached, "cached": True}

    try:
        transcript_list = api.list(video_id)
        langs = [
            {
                "language": t.language,
                "language_code": t.language_code,
                "is_generated": t.is_generated,
            }
            for t in transcript_list
        ]
    except Exception as exc:  # noqa: BLE001
        raise_for_transcript_error(exc)

    language_cache.set(video_id, langs)
    return {"video_id": video_id, "available": langs, "cached": False}


# ---------------------------------------------------------------------------
# Bulk mode -- many links in one go
# ---------------------------------------------------------------------------

SPLIT_PATTERN = re.compile(r"[\s,;]+")


class BulkRequest(BaseModel):
    urls: list[str] = []
    fmt: str = "txt"
    lang: str | None = None
    output: str = "zip"  # "zip" = one file per video, "combined" = single file


def parse_url_list(raw: list[str]) -> list[str]:
    """Accept anything reasonable: a proper list, or one blob of text with the
    links separated by newlines, commas, spaces. Duplicates are dropped so the
    same video is never fetched twice in one job."""
    items: list[str] = []
    for chunk in raw or []:
        items.extend(part for part in SPLIT_PATTERN.split(chunk or "") if part)

    seen = set()
    unique = []
    for item in items:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def fetch_one_for_bulk(index: int, raw_url: str, fmt: str, lang: str | None) -> dict:
    """Never raises -- a failure for one video must not sink the whole job."""
    result = {"index": index, "input": raw_url, "video_id": None, "ok": False,
              "content": "", "error": ""}
    try:
        video_id = extract_video_id(raw_url)
        result["video_id"] = video_id
        snippets = fetch_snippets(video_id, lang)
        result["content"] = to_srt(snippets) if fmt == "srt" else to_plain_text(snippets)
        result["ok"] = True
    except HTTPException as exc:
        result["error"] = str(exc.detail)
    except Exception as exc:  # noqa: BLE001 -- last-resort guard
        result["error"] = f"Unexpected error: {exc}"
    return result


def build_report(results: list[dict]) -> str:
    ok = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
    lines = [
        "TRANSCRIPT PULLER - BULK REPORT",
        "=" * 60,
        f"Requested: {len(results)}",
        f"Succeeded: {len(ok)}",
        f"Failed:    {len(failed)}",
        "",
    ]
    if failed:
        lines.append("FAILURES")
        lines.append("-" * 60)
        for r in failed:
            lines.append(f"[{r['index']:02d}] {r['input']}")
            lines.append(f"     {r['error']}")
        lines.append("")
    if ok:
        lines.append("SUCCEEDED")
        lines.append("-" * 60)
        for r in ok:
            lines.append(f"[{r['index']:02d}] {r['video_id']}  ({len(r['content'])} chars)")
    return "\n".join(lines) + "\n"


@app.post("/api/bulk")
def bulk_transcripts(payload: BulkRequest, request: Request):
    """Fetch many transcripts at once.

    Returns a ZIP (one file per video plus a report), or a single combined
    text file. Videos that fail are listed in the report rather than failing
    the whole request -- one dead link in twenty shouldn't cost you the other
    nineteen."""
    if payload.fmt not in ("txt", "srt"):
        raise HTTPException(status_code=422, detail="fmt must be 'txt' or 'srt'.")
    if payload.output not in ("zip", "combined"):
        raise HTTPException(status_code=422, detail="output must be 'zip' or 'combined'.")

    urls = parse_url_list(payload.urls)
    if not urls:
        raise HTTPException(status_code=400, detail="No YouTube links found in the input.")
    if len(urls) > BULK_MAX_URLS:
        raise HTTPException(
            status_code=413,
            detail=f"Too many links ({len(urls)}). The limit is {BULK_MAX_URLS} per request.",
        )

    # One slot per video, not per HTTP request.
    charge_rate_limit(request, cost=len(urls))

    workers = min(BULK_CONCURRENCY, len(urls))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(
            pool.map(
                lambda pair: fetch_one_for_bulk(pair[0], pair[1], payload.fmt, payload.lang),
                enumerate(urls, start=1),
            )
        )

    succeeded = sum(1 for r in results if r["ok"])
    report = build_report(results)
    headers = {
        "X-Bulk-Total": str(len(results)),
        "X-Bulk-Succeeded": str(succeeded),
        "X-Bulk-Failed": str(len(results) - succeeded),
    }

    if payload.output == "combined":
        parts = [report, ""]
        for r in results:
            if not r["ok"]:
                continue
            parts.append("=" * 60)
            parts.append(f"[{r['index']:02d}] {r['video_id']}  https://youtu.be/{r['video_id']}")
            parts.append("=" * 60)
            parts.append(r["content"])
            parts.append("")
        headers["Content-Disposition"] = 'attachment; filename="transcripts.txt"'
        return Response("\n".join(parts), media_type="text/plain", headers=headers)

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for r in results:
            if r["ok"]:
                archive.writestr(f"{r['index']:02d}_{r['video_id']}.{payload.fmt}", r["content"])
        archive.writestr("_report.txt", report)

    headers["Content-Disposition"] = 'attachment; filename="transcripts.zip"'
    return Response(buffer.getvalue(), media_type="application/zip", headers=headers)


# The static frontend is mounted last so it does not shadow the /api routes.
app.mount("/", StaticFiles(directory="static", html=True), name="static")
