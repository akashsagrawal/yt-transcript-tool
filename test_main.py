"""
Tests for the transcript tool.

These deliberately do NOT hit YouTube. Network-dependent tests are slow,
flaky, and (once the proxy is on) cost money. Instead the YouTube call is
replaced with a fake, so what gets tested is our own logic: URL parsing,
SRT formatting, caching, rate limiting, and error translation.

Run with:   pytest -q
"""

import threading

import pytest
from fastapi.testclient import TestClient

import main
from main import extract_video_id, format_timestamp, to_srt, to_plain_text


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw",
    [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://www.youtube.com/embed/dQw4w9WgXcQ",
        "https://www.youtube.com/shorts/dQw4w9WgXcQ",
        "https://www.youtube.com/live/dQw4w9WgXcQ",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42s",
        "dQw4w9WgXcQ",
        "   dQw4w9WgXcQ   ",
    ],
)
def test_extract_video_id_accepts_common_formats(raw):
    assert extract_video_id(raw) == "dQw4w9WgXcQ"


@pytest.mark.parametrize("raw", ["", "not a url", "https://example.com/", "abc"])
def test_extract_video_id_rejects_junk(raw):
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as err:
        extract_video_id(raw)
    assert err.value.status_code == 400


def test_format_timestamp():
    assert format_timestamp(0) == "00:00:00,000"
    assert format_timestamp(61.5) == "00:01:01,500"
    assert format_timestamp(3661.25) == "01:01:01,250"


SNIPPETS = [
    {"text": "hello there", "start": 0.0, "duration": 2.5},
    {"text": "general kenobi", "start": 2.5, "duration": 3.0},
]


def test_to_plain_text():
    assert to_plain_text(SNIPPETS) == "hello there\ngeneral kenobi"


def test_to_srt_numbering_and_timing():
    srt = to_srt(SNIPPETS)
    assert srt.startswith("1\n00:00:00,000 --> 00:00:02,500\nhello there\n")
    assert "2\n00:00:02,500 --> 00:00:05,500\ngeneral kenobi\n" in srt


# ---------------------------------------------------------------------------
# Fakes so we never touch the network
# ---------------------------------------------------------------------------

class FakeSnippet:
    def __init__(self, text, start, duration):
        self.text = text
        self.start = start
        self.duration = duration


class FakeFetched:
    def __init__(self):
        self.snippets = [FakeSnippet(s["text"], s["start"], s["duration"]) for s in SNIPPETS]


class FakeApi:
    """Stands in for YouTubeTranscriptApi and counts how often it is called,
    which is how we prove the cache is doing its job."""

    def __init__(self):
        self.fetch_calls = 0

    def fetch(self, video_id, languages=("en",), preserve_formatting=False):
        self.fetch_calls += 1
        return FakeFetched()


@pytest.fixture
def client(monkeypatch):
    fake = FakeApi()
    monkeypatch.setattr(main, "api", fake)
    # Start every test from a clean cache and rate-limit state.
    main.transcript_cache = main.TTLCache(3600, 100)
    main.language_cache = main.TTLCache(3600, 100)
    main.rate_limiter = main.RateLimiter(main.RATE_LIMIT_REQUESTS, main.RATE_LIMIT_WINDOW_SECONDS)
    main.jobs.clear()
    test_client = TestClient(main.app)
    test_client.fake = fake
    return test_client


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

def test_health(client):
    res = client.get("/api/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_transcript_txt(client):
    res = client.get("/api/transcript", params={"url": "dQw4w9WgXcQ"})
    assert res.status_code == 200
    assert res.text == "hello there\ngeneral kenobi"
    assert "attachment" in res.headers["content-disposition"]


def test_transcript_srt(client):
    res = client.get("/api/transcript", params={"url": "dQw4w9WgXcQ", "fmt": "srt"})
    assert res.status_code == 200
    assert res.text.startswith("1\n00:00:00,000 --> 00:00:02,500\n")


def test_bad_format_rejected(client):
    res = client.get("/api/transcript", params={"url": "dQw4w9WgXcQ", "fmt": "docx"})
    assert res.status_code == 422


def test_second_request_is_served_from_cache(client):
    client.get("/api/transcript", params={"url": "dQw4w9WgXcQ"})
    client.get("/api/transcript", params={"url": "dQw4w9WgXcQ"})
    client.get("/api/transcript", params={"url": "dQw4w9WgXcQ", "fmt": "srt"})
    # Three requests, one actual YouTube call.
    assert client.fake.fetch_calls == 1


def test_rate_limit_kicks_in(client):
    main.rate_limiter = main.RateLimiter(max_requests=3, window_seconds=60)
    for _ in range(3):
        assert client.get("/api/transcript", params={"url": "dQw4w9WgXcQ"}).status_code == 200
    blocked = client.get("/api/transcript", params={"url": "dQw4w9WgXcQ"})
    assert blocked.status_code == 429
    assert "Retry-After" in blocked.headers


def test_blocked_ip_returns_503_with_guidance(client, monkeypatch):
    from youtube_transcript_api import RequestBlocked

    def blow_up(*args, **kwargs):
        raise RequestBlocked("dQw4w9WgXcQ")

    monkeypatch.setattr(main.api, "fetch", blow_up)
    res = client.get("/api/transcript", params={"url": "dQw4w9WgXcQ"})
    assert res.status_code == 503
    assert "blocking" in res.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Bulk mode
# ---------------------------------------------------------------------------

import io
import zipfile

THREE_IDS = ["dQw4w9WgXcQ", "aBcDeFgHiJk", "12345678901"]


def test_bulk_zip_contains_one_file_per_video_plus_report(client):
    res = client.post("/api/bulk", json={"urls": THREE_IDS})
    assert res.status_code == 200
    assert res.headers["content-type"] == "application/zip"
    assert res.headers["X-Bulk-Succeeded"] == "3"
    assert res.headers["X-Bulk-Failed"] == "0"

    archive = zipfile.ZipFile(io.BytesIO(res.content))
    names = archive.namelist()
    assert "_report.txt" in names
    assert len([n for n in names if n.endswith(".txt") and n != "_report.txt"]) == 3
    assert archive.read("001_dQw4w9WgXcQ.txt").decode() == "hello there\ngeneral kenobi"


def test_bulk_combined_returns_one_text_file(client):
    res = client.post("/api/bulk", json={"urls": THREE_IDS, "output": "combined"})
    assert res.status_code == 200
    assert "transcripts.txt" in res.headers["content-disposition"]
    assert res.text.count("general kenobi") == 3


def test_bulk_accepts_one_newline_separated_blob(client):
    res = client.post("/api/bulk", json={"urls": ["\n".join(THREE_IDS)]})
    assert res.status_code == 200
    assert res.headers["X-Bulk-Total"] == "3"


def test_bulk_dedupes_repeated_links(client):
    res = client.post("/api/bulk", json={"urls": THREE_IDS + THREE_IDS})
    assert res.status_code == 200
    assert res.headers["X-Bulk-Total"] == "3"
    assert client.fake.fetch_calls == 3


def test_bulk_survives_one_bad_link(client):
    res = client.post("/api/bulk", json={"urls": ["dQw4w9WgXcQ", "not-a-link"]})
    assert res.status_code == 200
    assert res.headers["X-Bulk-Succeeded"] == "1"
    assert res.headers["X-Bulk-Failed"] == "1"

    report = zipfile.ZipFile(io.BytesIO(res.content)).read("_report.txt").decode()
    assert "not-a-link" in report
    assert "FAILURES" in report


def test_bulk_rejects_empty_and_oversized(client):
    assert client.post("/api/bulk", json={"urls": []}).status_code == 400
    too_many = [f"{i:011d}" for i in range(main.BULK_MAX_URLS + 1)]
    assert client.post("/api/bulk", json={"urls": too_many}).status_code == 413


def test_bulk_charges_rate_limit_per_video(client):
    main.rate_limiter = main.RateLimiter(max_requests=5, window_seconds=60)
    assert client.post("/api/bulk", json={"urls": THREE_IDS}).status_code == 200
    # 3 spent, 2 left -- a second 3-video job must not fit.
    blocked = client.post("/api/bulk", json={"urls": THREE_IDS})
    assert blocked.status_code == 429


def test_bulk_rejects_bad_format(client):
    res = client.post("/api/bulk", json={"urls": THREE_IDS, "fmt": "docx"})
    assert res.status_code == 422


def test_captions_disabled_returns_404(client, monkeypatch):
    from youtube_transcript_api import TranscriptsDisabled

    def blow_up(*args, **kwargs):
        raise TranscriptsDisabled("dQw4w9WgXcQ")

    monkeypatch.setattr(main.api, "fetch", blow_up)
    res = client.get("/api/transcript", params={"url": "dQw4w9WgXcQ"})
    assert res.status_code == 404
    assert "captions" in res.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Channel mode + background jobs
#
# The YouTube Data API is faked too. These tests prove our plumbing: URL/handle
# parsing, pagination, job lifecycle, cancellation, and packaging.
# ---------------------------------------------------------------------------

import time


def fake_yt_api_factory(video_count=7):
    """Return a stand-in for main.yt_api that pages 50 videos at a time."""
    calls = []

    def fake_yt_api(path, **params):
        calls.append((path, params))
        if path == "channels":
            return {"items": [{
                "id": "UC" + "x" * 22,
                "snippet": {"title": "NK Stock Talk"},
                "contentDetails": {"relatedPlaylists": {"uploads": "UU" + "x" * 22}},
                "statistics": {"videoCount": str(video_count)},
            }]}
        if path == "playlistItems":
            start = int(params.get("pageToken") or 0)
            page = []
            for i in range(start, min(start + 50, video_count)):
                page.append({
                    "contentDetails": {"videoId": f"vid{i:08d}", "videoPublishedAt": "2026-01-01T00:00:00Z"},
                    "snippet": {"title": f"Episode {i}"},
                })
            out = {"items": page}
            if start + 50 < video_count:
                out["nextPageToken"] = str(start + 50)
            return out
        raise AssertionError(f"unexpected API path {path}")

    fake_yt_api.calls = calls
    return fake_yt_api


@pytest.fixture
def channel_client(client, monkeypatch):
    monkeypatch.setattr(main, "YOUTUBE_API_KEY", "fake-key")
    monkeypatch.setattr(main, "yt_api", fake_yt_api_factory())
    return client


def wait_for_job(client, job_id, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = client.get(f"/api/jobs/{job_id}").json()
        if body["status"] != "running":
            return body
        time.sleep(0.05)
    raise AssertionError("job did not finish in time")


def test_channel_mode_requires_api_key(client, monkeypatch):
    monkeypatch.setattr(main, "YOUTUBE_API_KEY", "")
    res = client.get("/api/channel", params={"url": "@nkstocktalk"})
    assert res.status_code == 503
    assert "YOUTUBE_API_KEY" in res.json()["detail"]


def test_channel_preview_lists_videos_without_fetching(channel_client):
    res = channel_client.get("/api/channel", params={"url": "https://youtube.com/@nkstocktalk"})
    assert res.status_code == 200
    body = res.json()
    assert body["channel"] == "NK Stock Talk"
    assert body["listed"] == 7
    assert body["videos"][0]["title"] == "Episode 0"
    # Preview must not cost any transcript fetches.
    assert channel_client.fake.fetch_calls == 0


def test_channel_pagination_beyond_one_page(client, monkeypatch):
    monkeypatch.setattr(main, "YOUTUBE_API_KEY", "fake-key")
    monkeypatch.setattr(main, "yt_api", fake_yt_api_factory(video_count=120))
    res = client.get("/api/channel", params={"url": "@big"})
    assert res.json()["listed"] == 120


@pytest.mark.parametrize(
    "raw,expected_param",
    [
        ("https://www.youtube.com/channel/UC" + "a" * 22, "id"),
        ("https://www.youtube.com/@somehandle", "forHandle"),
        ("https://www.youtube.com/user/oldstyle", "forUsername"),
    ],
)
def test_resolve_channel_picks_the_right_lookup(raw, expected_param, monkeypatch):
    monkeypatch.setattr(main, "YOUTUBE_API_KEY", "fake-key")
    fake = fake_yt_api_factory()
    monkeypatch.setattr(main, "yt_api", fake)
    main.resolve_channel(raw)
    assert expected_param in fake.calls[0][1]


def test_playlist_url_is_used_directly(monkeypatch):
    monkeypatch.setattr(main, "YOUTUBE_API_KEY", "fake-key")
    info = main.resolve_channel("https://www.youtube.com/playlist?list=PLabc123")
    assert info["uploads_playlist"] == "PLabc123"


def test_job_runs_to_completion_and_downloads(channel_client):
    started = channel_client.post("/api/jobs", json={"channel": "@nkstocktalk"}).json()
    assert started["status"] == "running"
    assert started["total"] == 7

    done = wait_for_job(channel_client, started["id"])
    assert done["status"] == "done"
    assert done["succeeded"] == 7
    assert done["percent"] == 100.0
    assert done["download_ready"] is True

    dl = channel_client.get(f"/api/jobs/{started['id']}/download")
    assert dl.status_code == 200
    archive = zipfile.ZipFile(io.BytesIO(dl.content))
    assert len([n for n in archive.namelist() if n != "_report.txt"]) == 7
    assert "NK_Stock_Talk" in dl.headers["content-disposition"]


def test_job_limit_and_skip_let_you_resume(channel_client):
    started = channel_client.post(
        "/api/jobs", json={"channel": "@nkstocktalk", "skip": 2, "limit": 3}
    ).json()
    assert started["total"] == 3
    done = wait_for_job(channel_client, started["id"])
    assert done["succeeded"] == 3


def test_only_one_job_at_a_time(channel_client, monkeypatch):
    # Freeze the first job mid-flight so the second one collides with it.
    gate = threading.Event()
    original = main.fetch_one_for_bulk
    monkeypatch.setattr(
        main, "fetch_one_for_bulk",
        lambda *a, **k: (gate.wait(5), original(*a, **k))[1],
    )
    first = channel_client.post("/api/jobs", json={"channel": "@nkstocktalk"}).json()
    second = channel_client.post("/api/jobs", json={"channel": "@nkstocktalk"})
    assert second.status_code == 409
    gate.set()
    wait_for_job(channel_client, first["id"])


def test_job_can_be_cancelled(channel_client, monkeypatch):
    gate = threading.Event()
    original = main.fetch_one_for_bulk
    monkeypatch.setattr(
        main, "fetch_one_for_bulk",
        lambda *a, **k: (gate.wait(5), original(*a, **k))[1],
    )
    started = channel_client.post("/api/jobs", json={"channel": "@nkstocktalk"}).json()
    assert channel_client.post(f"/api/jobs/{started['id']}/cancel").json()["cancelling"] is True
    gate.set()
    done = wait_for_job(channel_client, started["id"])
    assert done["status"] == "cancelled"


def test_download_while_running_is_rejected(channel_client, monkeypatch):
    gate = threading.Event()
    original = main.fetch_one_for_bulk
    monkeypatch.setattr(
        main, "fetch_one_for_bulk",
        lambda *a, **k: (gate.wait(5), original(*a, **k))[1],
    )
    started = channel_client.post("/api/jobs", json={"channel": "@nkstocktalk"}).json()
    assert channel_client.get(f"/api/jobs/{started['id']}/download").status_code == 409
    gate.set()
    wait_for_job(channel_client, started["id"])


def test_unknown_job_is_404(client):
    assert client.get("/api/jobs/doesnotexist").status_code == 404


def test_job_combined_output_includes_titles(channel_client):
    started = channel_client.post(
        "/api/jobs", json={"channel": "@nkstocktalk", "output": "combined", "limit": 2}
    ).json()
    wait_for_job(channel_client, started["id"])
    text = channel_client.get(f"/api/jobs/{started['id']}/download").text
    assert "Episode 0" in text
    assert "general kenobi" in text
