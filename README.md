# Transcript Puller

Paste a YouTube link, get the transcript as plain text or SRT. No API key,
no per-request quota (that only applies to YouTube's official Data API,
which this doesn't use) — the limits you'll actually hit are your own
server's capacity and, occasionally, YouTube rate-limiting your server's IP
if you send a huge burst of requests in a short window.

## How it works

- `GET /api/transcript?url=<youtube url or id>&fmt=txt|srt` fetches the
  caption track YouTube already has for that video (auto-generated or
  uploaded) and streams it back as a downloadable file.
- `GET /api/languages?url=<...>` lists which caption languages exist for a
  video, if you want to add a language picker later.
- If a video has no captions at all, there's nothing to pull — this tool
  can't transcribe audio itself (that would need Whisper or a similar
  speech-to-text model bolted on separately).

## Run it locally

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

Then open http://127.0.0.1:8000

## Deploy it (recommended: Render, free tier available)

1. Push this folder to a GitHub repo.
2. Go to https://render.com → New → Web Service → connect the repo.
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Deploy. Render gives you a public `https://yourapp.onrender.com` link —
   that's the link you share.

Railway (https://railway.app) works the same way and also reads the
included `Procfile` automatically if you deploy there instead.

A plain cloud VM (DigitalOcean, AWS Lightsail, etc.) works too if you want
more control — just install Python, clone the repo, `pip install -r
requirements.txt`, and run uvicorn behind nginx or as a systemd service.

## A couple of things worth knowing before you launch this publicly

- **It scrapes YouTube's public caption endpoint rather than going through
  their official Data API.** This is what every popular "YouTube transcript"
  tool does, but it sits in a gray area of YouTube's Terms of Service, and
  YouTube can rate-limit or block the IP making requests if traffic gets
  heavy. If that happens, transcripts will start failing until the block
  lifts or you rotate infrastructure.
- **You're not hosting or storing YouTube's video/audio** — just relaying
  text captions that were already public on the video page — but you're
  still responsible for how your tool gets used once it's live.
- If you later want to add "upload a video, get a transcript" for videos
  without captions, that needs a real transcription model (e.g. Whisper)
  running server-side, which is a heavier lift in compute and hosting cost
  — happy to help with that when you're ready.
