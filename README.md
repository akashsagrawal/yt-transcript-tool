# Transcript Puller

Paste a YouTube link, get the transcript as plain text or SRT. No YouTube API
key needed — this reads the caption track that's already public on the video
page.

## How it works

- `GET /api/transcript?url=<youtube url or id>&fmt=txt|srt&lang=<code>` fetches
  the caption track YouTube already has for that video (auto-generated or
  uploaded) and streams it back as a downloadable file.
- `GET /api/languages?url=<...>` lists which caption languages exist for a
  video, if you want to add a language picker later.
- `POST /api/bulk` takes many links at once — see below.
- `GET /api/health` reports whether the proxy is configured, what the rate
  limit is, and how the cache is doing. Useful for checking a deploy.
- If a video has no captions at all, there's nothing to pull — this tool can't
  transcribe audio itself (that would need Whisper or a similar speech-to-text
  model bolted on separately).

**How to tell in advance whether a video will work:** open it on YouTube, click
the `...` (more) button under the video, and look for "Show transcript". If
that option is there, this tool can pull it. If it isn't, the video has no
captions and nothing will work — that's a YouTube limitation, not a bug here.

## The proxy: why it's required in production

YouTube blocks requests coming from datacenter IP addresses — Render, AWS,
Google Cloud, and every other cloud host. It's how they stop large-scale
scraping. So the app works fine on your laptop (residential IP) and then
returns "blocked" errors once deployed.

The fix, which is what every production transcript tool does, is to route
requests through a residential proxy. The library has built-in support for
Webshare, so setup is two environment variables and no code changes.

**This is the only part of the project that costs money.** GitHub, Render, and
the app itself stay at $0. A Webshare residential plan runs roughly $1–3/month
at light personal use.

### Setting it up

1. Sign up at https://www.webshare.io/ and buy the plan listed in the sidebar
   as **Rotating Residential**.

   > **Buy the right one.** Webshare sells three similar-sounding products:
   > *Proxy Server*, *Static Residential*, and *Rotating Residential*. Only
   > **Rotating Residential** works properly here — the library's docs say
   > outright not to buy the other two. Static Residential gives you a small
   > set of fixed IPs, so once YouTube blocks one, every request through it
   > keeps failing. Rotating Residential cycles through a large pool and the
   > library automatically retries on a fresh IP when it hits a block.

2. Go to **https://dashboard.webshare.io/proxy/settings** and copy your
   **Proxy Username** and **Proxy Password**. These are *not* your Webshare
   account login — they're separate credentials on that page.
3. In Render: your service → **Environment** → add two variables:

   | Key | Value |
   | --- | --- |
   | `WEBSHARE_PROXY_USERNAME` | your proxy username |
   | `WEBSHARE_PROXY_PASSWORD` | your proxy password |

4. Save. Render redeploys automatically. Then open
   `https://yourapp.onrender.com/api/health` and confirm it says
   `"proxy_mode": "webshare"`.

Using a different provider? Set `HTTP_PROXY_URL` and `HTTPS_PROXY_URL` to full
URLs like `http://user:pass@host:port` instead, and the app uses those.

With no proxy variables set, the app still runs and works fine locally — it
just talks to YouTube directly and will get blocked intermittently once it's on
a cloud host.

## Bulk mode

The **Bulk** tab takes a list of links — one per line — and returns either a
`.zip` with one file per video, or a single combined `.txt`. You can also drag
a `.txt` file of links straight onto the box.

```
POST /api/bulk
{"urls": ["https://youtu.be/xxx", "yyy"], "fmt": "txt", "output": "zip"}
```

Behaviour worth knowing:

- **One bad link doesn't sink the job.** Failures are collected and listed in
  `_report.txt` inside the zip (or at the top of the combined file); everything
  that worked still comes back.
- **Duplicates are dropped** before fetching, so the same video is never pulled
  twice in one run.
- **It costs rate-limit slots per video, not per request** — a 10-link job
  spends 10 of your 20/minute. Otherwise bulk mode would be a hole straight
  through the rate limit.
- **Cached videos are free.** Anything pulled in the last 24 hours comes from
  memory and never touches the proxy.

| Variable | Default | What it does |
| --- | --- | --- |
| `BULK_MAX_URLS` | `25` | Max links accepted in one request |
| `BULK_CONCURRENCY` | `4` | How many are fetched in parallel |

Don't raise `BULK_CONCURRENCY` much. Twenty simultaneous requests through the
proxy is a good way to get flagged, and the gain is small since the slow part
is YouTube, not you.

## Channel mode — a whole channel in one go

The **Channel** tab takes a channel URL or `@handle`, lists every video on it,
and pulls the transcripts as a background job with a live progress bar.

Two steps on purpose:

1. **Check** — lists the videos. Free, instant, downloads nothing. You see the
   count before committing to anything.
2. **Start job** — actually fetches. Runs in the background, so closing the tab
   or reloading the page doesn't kill it; reopening reattaches to the progress.

### The API key

Listing a channel's videos uses the official **YouTube Data API v3**. Getting a
key is free:

1. https://console.cloud.google.com/ → create a project
2. **APIs & Services → Library** → search "YouTube Data API v3" → **Enable**
3. **APIs & Services → Credentials → Create credentials → API key** → copy it
4. Render → **Environment** → add `YOUTUBE_API_KEY` = your key → Save

Quota is 10,000 units/day. Listing 1,000 videos costs about 21 units, so this
is not a limit you will notice.

Without the key the rest of the tool still works — only the Channel tab returns
a 503 telling you it's missing. Check `/api/health` for
`"youtube_api_key_set": true`.

Note the key only lists *which videos exist*. Caption text still comes through
the proxy — the Data API can't hand out transcripts unless you're the channel
owner authenticating with OAuth.

### Going in batches

**Skip newest** and **How many** exist because of proxy bandwidth. Each
transcript costs roughly 200–500 KB of proxy traffic, so a 1,000-video channel
can eat most of a 1 GB plan. Run `skip 0 / how many 200`, then `skip 200 / how
many 200`, and so on. Anything already pulled in the last 24 hours comes from
cache and costs nothing.

### Job endpoints

```
GET  /api/channel?url=@handle      list videos, fetch nothing
POST /api/jobs                     {channel, fmt, output, skip, limit}
GET  /api/jobs/{id}                progress, ETA, counts
POST /api/jobs/{id}/cancel         stop early, keep what's done
GET  /api/jobs/{id}/download       zip or combined file
```

One job runs at a time — the proxy is the bottleneck, so a second parallel job
would only slow the first one down. Jobs live in memory and are dropped after 6
hours or a redeploy, so download your results when a job finishes.

| Variable | Default | What it does |
| --- | --- | --- |
| `YOUTUBE_API_KEY` | *(unset)* | Enables channel mode |
| `JOB_MAX_VIDEOS` | `2000` | Hard cap on videos per job |
| `JOB_RETENTION_SECONDS` | `21600` | How long finished jobs stay downloadable |

### Videos with no captions

Some videos have captions switched off by the uploader. There is no transcript
to fetch and no tool can produce one without running speech-to-text on the
audio. Those videos are skipped, counted as failures, and listed with the
reason in `_report.txt`. A run of 1,000 videos will normally have some of
these — that's expected, not a bug.

## Caching and rate limiting

Two things were added so a public link can't quietly cost you money:

- **Cache** — transcripts are held in memory for 24 hours, keyed by video and
  language. Ask for the same video twice and the second request never touches
  YouTube (or the proxy). It's an in-memory cache, so it empties on every
  redeploy and isn't shared between instances — fine for one small service,
  and the swap to Redis later touches only the `TTLCache` class.
- **Rate limit** — 20 requests per minute per IP by default. A single visitor
  or bot hammering the URL gets `429 Too Many Requests` instead of burning
  proxy bandwidth. The static page itself is never rate limited.

Both are tunable via environment variables (all optional):

| Variable | Default | What it does |
| --- | --- | --- |
| `CACHE_TTL_SECONDS` | `86400` | How long a cached transcript stays fresh |
| `CACHE_MAX_ENTRIES` | `500` | Cache size cap; oldest-unused evicted first |
| `RATE_LIMIT_REQUESTS` | `20` | Requests allowed per window per IP (`0` disables) |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | Length of the window |
| `ALLOWED_ORIGINS` | `*` | Comma-separated CORS origins; tighten to your domain |

## Run it locally

```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload
```

Then open http://127.0.0.1:8000

## Run the tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q
```

The tests never hit YouTube — the network call is replaced with a fake, so they
run in under a second, cost nothing, and don't break when a video gets deleted.
What they check is our own logic: URL parsing, SRT timing, that the cache
actually prevents a second fetch, that the rate limiter trips, and that each
YouTube failure maps to a sensible HTTP status.

Run these before pushing. If they pass, the deploy will at least boot.

## Deploy it (Render, free tier)

1. Push this folder to a GitHub repo.
2. https://render.com → New → Web Service → connect the repo.
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Add the proxy environment variables from the section above.
6. Deploy. Render gives you a public `https://yourapp.onrender.com` link.

Note that the free tier sleeps after inactivity, so the first request after a
quiet spell takes ~30 seconds to wake up. That's normal, not a failure.

Railway (https://railway.app) works the same way and reads the included
`Procfile` automatically. A plain VM (DigitalOcean, Lightsail) works too —
install Python, clone, `pip install -r requirements.txt`, run uvicorn behind
nginx or as a systemd service.

Keep uvicorn at a **single worker** (the default). The cache and rate limiter
live in process memory, so multiple workers would each keep their own copy and
both would be less effective.

## Things worth knowing before launching this publicly

- **It reads YouTube's public caption endpoint rather than the official Data
  API.** This is what every popular transcript tool does, but it sits in a gray
  area of YouTube's Terms of Service. The proxy makes it reliable; it doesn't
  make it officially sanctioned.
- **You're not hosting or storing YouTube's video or audio** — just relaying
  caption text that was already public on the video page — but you're still
  responsible for how the tool gets used once it's live.
- If you later want "upload a video, get a transcript" for videos *without*
  captions, that needs a real transcription model (e.g. Whisper) running
  server-side — a heavier lift in compute and hosting cost.

## Project layout

```
main.py              the whole API: config, cache, rate limiter, routes
test_main.py         tests (no network)
requirements.txt     what Render installs
requirements-dev.txt test-only extras; Render ignores this
Procfile             start command for Railway / Heroku-style hosts
static/index.html    the frontend page
```
