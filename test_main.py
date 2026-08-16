"""
Tests for the transcript tool.

These deliberately do NOT hit YouTube. Network-dependent tests are slow,
flaky, and (once the proxy is on) cost money. Instead the YouTube call is
replaced with a fake, so what gets tested is our own logic: URL parsing,
SRT formatting, caching, rate limiting, and error translation.

Run with:   pytest -q
"""

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


def test_captions_disabled_returns_404(client, monkeypatch):
    from youtube_transcript_api import TranscriptsDisabled

    def blow_up(*args, **kwargs):
        raise TranscriptsDisabled("dQw4w9WgXcQ")

    monkeypatch.setattr(main.api, "fetch", blow_up)
    res = client.get("/api/transcript", params={"url": "dQw4w9WgXcQ"})
    assert res.status_code == 404
    assert "captions" in res.json()["detail"].lower()
