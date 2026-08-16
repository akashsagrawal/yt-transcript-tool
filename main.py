import re
from io import StringIO

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

app = FastAPI(title="YT Transcript Tool")

# Allow the frontend (any origin) to call this API. Tighten this to your
# actual domain once you know it, e.g. allow_origins=["https://yoursite.com"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

api = YouTubeTranscriptApi()


def extract_video_id(url_or_id: str) -> str:
    """Pull an 11-char YouTube video ID out of any common URL format,
    or pass through a bare ID if that's what was given."""
    url_or_id = url_or_id.strip()

    patterns = [
        r"(?:v=|/videos/|embed/|youtu\.be/|/v/|/shorts/)([0-9A-Za-z_-]{11})",
        r"^([0-9A-Za-z_-]{11})$",  # bare video id
    ]
    for pattern in patterns:
        match = re.search(pattern, url_or_id)
        if match:
            return match.group(1)

    raise HTTPException(status_code=400, detail="Could not find a YouTube video ID in that input.")


def format_timestamp(seconds: float) -> str:
    ms = int((seconds - int(seconds)) * 1000)
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02}:{m:02}:{s:02},{ms:03}"


def to_srt(snippets) -> str:
    buf = StringIO()
    for i, snip in enumerate(snippets, start=1):
        start = format_timestamp(snip.start)
        end = format_timestamp(snip.start + snip.duration)
        buf.write(f"{i}\n{start} --> {end}\n{snip.text}\n\n")
    return buf.getvalue()


def to_plain_text(snippets) -> str:
    return "\n".join(snip.text for snip in snippets)


@app.get("/api/transcript")
def get_transcript(
    url: str = Query(..., description="YouTube URL or video ID"),
    fmt: str = Query("txt", pattern="^(txt|srt)$"),
    lang: str | None = Query(None, description="Preferred language code, e.g. 'en'"),
):
    video_id = extract_video_id(url)

    try:
        if lang:
            transcript_list = api.list(video_id)
            transcript = transcript_list.find_transcript([lang])
            fetched = transcript.fetch()
        else:
            fetched = api.fetch(video_id)
    except TranscriptsDisabled:
        raise HTTPException(status_code=404, detail="Transcripts are disabled for this video.")
    except NoTranscriptFound:
        raise HTTPException(status_code=404, detail="No transcript found for this video/language.")
    except VideoUnavailable:
        raise HTTPException(status_code=404, detail="Video unavailable.")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch transcript: {e}")

    snippets = fetched.snippets
    content = to_srt(snippets) if fmt == "srt" else to_plain_text(snippets)
    media_type = "text/plain" if fmt == "txt" else "application/x-subrip"
    filename = f"{video_id}.{fmt}"

    return StreamingResponse(
        iter([content]),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/languages")
def get_languages(url: str = Query(...)):
    """List what transcript languages are available for a video."""
    video_id = extract_video_id(url)
    try:
        transcript_list = api.list(video_id)
    except TranscriptsDisabled:
        raise HTTPException(status_code=404, detail="Transcripts are disabled for this video.")
    except VideoUnavailable:
        raise HTTPException(status_code=404, detail="Video unavailable.")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to list transcripts: {e}")

    langs = [
        {
            "language": t.language,
            "language_code": t.language_code,
            "is_generated": t.is_generated,
        }
        for t in transcript_list
    ]
    return {"video_id": video_id, "available": langs}


# Serve the simple frontend
app.mount("/", StaticFiles(directory="static", html=True), name="static")
