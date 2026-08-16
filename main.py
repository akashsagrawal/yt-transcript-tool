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
import uuid
import zipfile
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO, StringIO

import requests

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

# Channel mode. Listing a channel's videos uses the official YouTube Data API
# (free, 10,000 units/day; listing 1,000 videos costs about 21 units).
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "").strip()
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# A whole-channel run is a background job, because 1,000 videos cannot finish
# inside one HTTP request. Only one job runs at a time -- the bottleneck is the
# proxy, so a second parallel job would just slow the first one down.
JOB_MAX_VIDEOS = _env_int("JOB_MAX_VIDEOS", 2000)
JOB_RETENTION_SECONDS = _env_int("JOB_RETENTION_SECONDS", 6 * 60 * 60)
JOB_MAX_KEPT = _env_int("JOB_MAX_KEPT", 10)

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
        "channel_mode": {
            "youtube_api_key_set": bool(YOUTUBE_API_KEY),
            "max_videos_per_job": JOB_MAX_VIDEOS,
        },
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
              "content": "", "error": "", "title": ""}
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
            label = r.get("title") or r["input"]
            lines.append(f"[{r['index']:03d}] {label}")
            lines.append(f"      {r['error']}")
        lines.append("")
    if ok:
        lines.append("SUCCEEDED")
        lines.append("-" * 60)
        for r in ok:
            label = f"  {r['title']}" if r.get("title") else ""
            lines.append(f"[{r['index']:03d}] {r['video_id']}{label}  ({len(r['content'])} chars)")
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

    return package_results(results, payload.fmt, payload.output)


# ---------------------------------------------------------------------------
# Channel listing (YouTube Data API v3)
#
# This is the ONE place the official API is used, and only to answer "which
# videos exist on this channel". Transcripts still come from the caption
# endpoint via the proxy -- the Data API cannot give you caption text without
# OAuth as the channel owner.
# ---------------------------------------------------------------------------

CHANNEL_ID_RE = re.compile(r"(?:channel/)?(UC[0-9A-Za-z_-]{22})")
HANDLE_RE = re.compile(r"@([A-Za-z0-9._-]+)")
LEGACY_USER_RE = re.compile(r"/user/([A-Za-z0-9._-]+)")
PLAYLIST_RE = re.compile(r"[?&]list=([0-9A-Za-z_-]+)")


def require_api_key() -> str:
    if not YOUTUBE_API_KEY:
        raise HTTPException(
            status_code=503,
            detail=(
                "Channel mode needs a YouTube Data API key. Set the "
                "YOUTUBE_API_KEY environment variable (see README)."
            ),
        )
    return YOUTUBE_API_KEY


def yt_api(path: str, **params) -> dict:
    """One call to the YouTube Data API, with readable errors."""
    params["key"] = require_api_key()
    try:
        response = requests.get(f"{YOUTUBE_API_BASE}/{path}", params=params, timeout=20)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Could not reach the YouTube API: {exc}")

    if response.status_code == 403:
        raise HTTPException(
            status_code=403,
            detail=(
                "YouTube API rejected the request. Usually this means the daily "
                "quota is used up, or the API key is restricted / the YouTube "
                "Data API v3 is not enabled for it."
            ),
        )
    if response.status_code == 400:
        raise HTTPException(status_code=400, detail="YouTube API rejected the request (bad key or parameter).")
    if not response.ok:
        raise HTTPException(status_code=502, detail=f"YouTube API error {response.status_code}.")
    return response.json()


def resolve_channel(raw: str) -> dict:
    """Turn a channel URL / @handle / UC id into the info we need.

    Returns {channel_id, title, uploads_playlist, video_count}."""
    raw = (raw or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="Give a channel URL or @handle.")

    # A plain playlist link is also accepted -- treat it as the source list.
    playlist_match = PLAYLIST_RE.search(raw)
    if playlist_match and "list=" in raw:
        playlist_id = playlist_match.group(1)
        return {
            "channel_id": None,
            "title": f"Playlist {playlist_id}",
            "uploads_playlist": playlist_id,
            "video_count": None,
        }

    params: dict
    if CHANNEL_ID_RE.search(raw):
        params = {"id": CHANNEL_ID_RE.search(raw).group(1)}
    elif HANDLE_RE.search(raw):
        params = {"forHandle": HANDLE_RE.search(raw).group(1)}
    elif LEGACY_USER_RE.search(raw):
        params = {"forUsername": LEGACY_USER_RE.search(raw).group(1)}
    else:
        # Bare word -- assume it is a handle without the @.
        params = {"forHandle": raw.rstrip("/").split("/")[-1]}

    data = yt_api("channels", part="snippet,contentDetails,statistics", **params)
    items = data.get("items") or []
    if not items:
        raise HTTPException(
            status_code=404,
            detail="No channel found for that link. Try the full URL, or the @handle.",
        )

    channel = items[0]
    uploads = (
        channel.get("contentDetails", {})
        .get("relatedPlaylists", {})
        .get("uploads")
    )
    if not uploads:
        raise HTTPException(status_code=502, detail="Channel has no uploads playlist.")

    count = channel.get("statistics", {}).get("videoCount")
    return {
        "channel_id": channel.get("id"),
        "title": channel.get("snippet", {}).get("title", "Unknown channel"),
        "uploads_playlist": uploads,
        "video_count": int(count) if count is not None else None,
    }


def list_playlist_videos(playlist_id: str, cap: int) -> list[dict]:
    """Page through a playlist. 50 videos per API call, 1 quota unit each."""
    videos: list[dict] = []
    page_token = None
    while len(videos) < cap:
        params = {"part": "contentDetails,snippet", "playlistId": playlist_id, "maxResults": 50}
        if page_token:
            params["pageToken"] = page_token
        data = yt_api("playlistItems", **params)

        for item in data.get("items", []):
            video_id = item.get("contentDetails", {}).get("videoId")
            if not video_id:
                continue
            videos.append({
                "video_id": video_id,
                "title": item.get("snippet", {}).get("title", ""),
                "published_at": item.get("contentDetails", {}).get("videoPublishedAt", ""),
            })
            if len(videos) >= cap:
                break

        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return videos


@app.get("/api/channel")
def preview_channel(
    url: str = Query(..., description="Channel URL, @handle, or playlist URL"),
    limit: int = Query(0, ge=0, description="0 = list everything (up to the cap)"),
):
    """Look up a channel and list its videos WITHOUT fetching any transcripts.
    Cheap and instant -- use it to see how many videos you are about to pull."""
    info = resolve_channel(url)
    cap = min(limit or JOB_MAX_VIDEOS, JOB_MAX_VIDEOS)
    videos = list_playlist_videos(info["uploads_playlist"], cap)
    return {
        "channel": info["title"],
        "channel_id": info["channel_id"],
        "reported_video_count": info["video_count"],
        "listed": len(videos),
        "videos": videos,
    }


# ---------------------------------------------------------------------------
# Background jobs
#
# In-memory only: a redeploy or a Render free-tier sleep loses job state. That
# is a deliberate trade -- adding a database for something you run occasionally
# is not worth the operational weight. Download your results when a job ends.
# ---------------------------------------------------------------------------

class Job:
    def __init__(self, job_id: str, label: str, videos: list[dict], fmt: str, output: str,
                 lang: str | None):
        self.id = job_id
        self.label = label
        self.videos = videos
        self.fmt = fmt
        self.output = output
        self.lang = lang
        self.status = "running"          # running | done | cancelled | error
        self.created_at = time.time()
        self.finished_at: float | None = None
        self.total = len(videos)
        self.completed = 0
        self.succeeded = 0
        self.failed = 0
        self.results: list[dict] = []
        self.error = ""
        self.cancel_requested = False
        self.lock = threading.Lock()

    def snapshot(self) -> dict:
        with self.lock:
            elapsed = (self.finished_at or time.time()) - self.created_at
            rate = self.completed / elapsed if elapsed > 0 and self.completed else 0
            remaining = self.total - self.completed
            return {
                "id": self.id,
                "label": self.label,
                "status": self.status,
                "total": self.total,
                "completed": self.completed,
                "succeeded": self.succeeded,
                "failed": self.failed,
                "percent": round(100 * self.completed / self.total, 1) if self.total else 0,
                "elapsed_seconds": round(elapsed),
                "eta_seconds": round(remaining / rate) if rate else None,
                "characters_fetched": sum(len(r["content"]) for r in self.results if r["ok"]),
                "error": self.error,
                "download_ready": self.status in ("done", "cancelled") and self.succeeded > 0,
            }


jobs: "OrderedDict[str, Job]" = OrderedDict()
jobs_lock = threading.Lock()


def prune_jobs() -> None:
    now = time.time()
    with jobs_lock:
        stale = [
            job_id for job_id, job in jobs.items()
            if job.status != "running" and now - (job.finished_at or job.created_at) > JOB_RETENTION_SECONDS
        ]
        for job_id in stale:
            del jobs[job_id]
        while len(jobs) > JOB_MAX_KEPT:
            for job_id, job in list(jobs.items()):
                if job.status != "running":
                    del jobs[job_id]
                    break
            else:
                break


def running_job() -> Job | None:
    with jobs_lock:
        for job in jobs.values():
            if job.status == "running":
                return job
    return None


def run_job(job: Job) -> None:
    """Worker body. Runs in its own thread."""
    def handle(pair):
        index, video = pair
        if job.cancel_requested:
            return None
        result = fetch_one_for_bulk(index, video["video_id"], job.fmt, job.lang)
        result["title"] = video.get("title", "")
        with job.lock:
            job.completed += 1
            if result["ok"]:
                job.succeeded += 1
            else:
                job.failed += 1
            job.results.append(result)
        return result

    try:
        workers = min(BULK_CONCURRENCY, max(1, job.total))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(handle, enumerate(job.videos, start=1)))
        with job.lock:
            job.status = "cancelled" if job.cancel_requested else "done"
    except Exception as exc:  # noqa: BLE001
        with job.lock:
            job.status = "error"
            job.error = str(exc)
    finally:
        with job.lock:
            job.finished_at = time.time()
            job.results.sort(key=lambda r: r["index"])


class JobRequest(BaseModel):
    channel: str | None = None
    urls: list[str] = []
    fmt: str = "txt"
    output: str = "zip"
    lang: str | None = None
    limit: int = 0   # 0 = no limit (up to JOB_MAX_VIDEOS)
    skip: int = 0    # how many of the newest videos to skip -- lets you resume


@app.post("/api/jobs")
def create_job(payload: JobRequest):
    if payload.fmt not in ("txt", "srt"):
        raise HTTPException(status_code=422, detail="fmt must be 'txt' or 'srt'.")
    if payload.output not in ("zip", "combined"):
        raise HTTPException(status_code=422, detail="output must be 'zip' or 'combined'.")

    existing = running_job()
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"A job is already running ({existing.completed}/{existing.total}). "
                   "Wait for it or cancel it first.",
        )

    if payload.channel:
        info = resolve_channel(payload.channel)
        wanted = payload.skip + (payload.limit or JOB_MAX_VIDEOS)
        listed = list_playlist_videos(info["uploads_playlist"], min(wanted, JOB_MAX_VIDEOS))
        videos = listed[payload.skip:]
        if payload.limit:
            videos = videos[: payload.limit]
        label = info["title"]
    else:
        ids = parse_url_list(payload.urls)
        videos = [{"video_id": extract_video_id(u), "title": ""} for u in ids]
        label = f"{len(videos)} pasted links"

    if not videos:
        raise HTTPException(status_code=400, detail="Nothing to fetch.")
    if len(videos) > JOB_MAX_VIDEOS:
        raise HTTPException(status_code=413, detail=f"Too many videos (cap is {JOB_MAX_VIDEOS}).")

    prune_jobs()
    job = Job(uuid.uuid4().hex[:12], label, videos, payload.fmt, payload.output, payload.lang)
    with jobs_lock:
        jobs[job.id] = job
    threading.Thread(target=run_job, args=(job,), daemon=True).start()
    return job.snapshot()


@app.get("/api/jobs")
def list_jobs():
    with jobs_lock:
        return {"jobs": [job.snapshot() for job in reversed(jobs.values())]}


def get_job_or_404(job_id: str) -> Job:
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="No such job (it may have expired).")
    return job


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    return get_job_or_404(job_id).snapshot()


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    job = get_job_or_404(job_id)
    job.cancel_requested = True
    return {"cancelling": True, "id": job.id}


@app.get("/api/jobs/{job_id}/download")
def download_job(job_id: str):
    job = get_job_or_404(job_id)
    if job.status == "running":
        raise HTTPException(status_code=409, detail="Job is still running.")
    if not job.results:
        raise HTTPException(status_code=404, detail="Nothing was fetched.")
    return package_results(job.results, job.fmt, job.output, stem=safe_stem(job.label))


SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


def safe_stem(label: str) -> str:
    stem = SAFE_CHARS.sub("_", (label or "transcripts")).strip("_")
    return (stem or "transcripts")[:48]


def package_results(results: list[dict], fmt: str, output: str, stem: str = "transcripts"):
    """Shared by bulk mode and jobs: turn results into a zip or one text file."""
    succeeded = sum(1 for r in results if r["ok"])
    report = build_report(results)
    headers = {
        "X-Bulk-Total": str(len(results)),
        "X-Bulk-Succeeded": str(succeeded),
        "X-Bulk-Failed": str(len(results) - succeeded),
    }

    if output == "combined":
        parts = [report, ""]
        for r in results:
            if not r["ok"]:
                continue
            parts.append("=" * 60)
            title = f"  {r['title']}" if r.get("title") else ""
            parts.append(f"[{r['index']:02d}] {r['video_id']}{title}")
            parts.append(f"https://youtu.be/{r['video_id']}")
            parts.append("=" * 60)
            parts.append(r["content"])
            parts.append("")
        headers["Content-Disposition"] = f'attachment; filename="{stem}.txt"'
        return Response("\n".join(parts), media_type="text/plain", headers=headers)

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for r in results:
            if r["ok"]:
                archive.writestr(f"{r['index']:03d}_{r['video_id']}.{fmt}", r["content"])
        archive.writestr("_report.txt", report)
    headers["Content-Disposition"] = f'attachment; filename="{stem}.zip"'
    return Response(buffer.getvalue(), media_type="application/zip", headers=headers)


# The static frontend is mounted last so it does not shadow the /api routes.
app.mount("/", StaticFiles(directory="static", html=True), name="static")
