# Sketch-to-Ring MVP

Draw or upload a ring sketch and add a description. The app generates up to 6 approved photorealistic variations. You pick one, run **one** refinement round, and download the result.
The spec is in [Sketch-to-Ring-Architecture.md](Sketch-to-Ring-Architecture.md).

## Run (Windows / PowerShell)
```powershell
copy .env.example .env            # defaults = MOCK providers, no keys needed
docker compose up -d              # mongo, redis, rabbitmq, seaweedfs (S3)

cd server; npm install; npm start           # http://127.0.0.1:3001
cd worker; python -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt
.\.venv\Scripts\python worker.py            # can run a 2nd copy for parallel consumption
cd web; npm install; npm run dev            # http://localhost:5173

node scripts/smoke.mjs            # end-to-end smoke test (run against the mocks)
```
- RabbitMQ UI: http://127.0.0.1:15672 (guest/guest). All ports are bound to 127.0.0.1 only.
- SeaweedFS Filer (local dev only, **no auth**): http://127.0.0.1:8888/buckets/sketch2ring/ shows a file by its key, e.g. `results/<taskId>.png`. Remove the 8888 port mapping in `docker-compose.yml` for anything beyond local dev.
- `/health` shows the service state, which provider and guardrail are active (MOCK or real), and the neurons reserved today.

### Live providers (explicit, small runs only)
Set the following in `.env`, then restart the server and the worker:
- `PROVIDER=cloudflare`, `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN` (the account must be on the **Free** plan)
- `GUARDRAIL=gemini`, `GEMINI_API_KEY` (free tier)

Without keys, or with the defaults, both stay MOCK, and the UI and `/health` say so.

Verified live on 2026-10-20 with `flux-2-klein-4b` and `gemini-3.1-flash-lite`:
- FLUX generation from a sketch (multipart `prompt`, `input_image_0` ≤512², `width`, `height`, about 13 s per image). Cloudflare returns JPEG, and the worker stores it as PNG.
- FLUX edit from the selected candidate (refine).
- Gemini input allow, input reject for an unrelated request, and output approval.
- A full round of 6 variations → select → refine → download.
- Gemini sometimes returns 503 "high demand". After 3 failed output checks, the image is not approved and is hidden.

## Flow and the two queues
```
React ──POST /api/jobs──▶ Node (session, idempotency, Redis rate limit, input validation, sharp normalize)
                          │ stores image in SeaweedFS (private), job in Mongo → 202 {jobId}
                          ├─ input-check loop: atomic claim+lease → Gemini (input policy) → prompt template → tasks in job (outbox)
                          ├─ outbox publisher ──▶ [generation.tasks] ──▶ Python worker (prefetch=1, manual ack)
                          │                                              loads ref from S3 → FLUX (Cloudflare) → saves result to S3
                          │                       [generation.results] ◀── publish (confirm) → then ACK task
                          ├─ results consumer: dedupe by taskId, persist, ACK immediately
                          └─ output-check loop: atomic per-task claim+lease → Gemini (output policy) → APPROVED/REJECTED
React polls GET /api/jobs/:id every 3s (pauses when the tab is hidden) → select → POST /refine (1 task, selected image) → download
```
- **generation.tasks**: Node is the producer and the Python worker is the consumer.
  - There is **one message per user request**, with `imageCount` (6 for a design, 1 for a refine) and `candidateIds`.
  - Messages carry only IDs, the prompt and storage keys, never image bytes or API keys.
- **generation.results**: the worker is the producer and Node is the consumer.
  - The worker makes one FLUX call per image (the API returns one image per call) and stores each image as `results/{candidateId}.png`.
  - It then publishes **one result message** with a `results` list, waits for the confirm, and only then ACKs the task.
  - Node updates each candidate idempotently (`bulkWrite`), ACKs, and runs the output guardrail **per image**.
  - Python never writes to Mongo.
- **Retry**:
  - On a safe-to-retry error, the whole task goes to the retry queue.
  - On the next attempt, stored images are skipped by the checkpoint, so nothing is regenerated.
  - UNKNOWN/FAILED outcomes are carried in the message as `settled`, so they are never re-sent to the provider.
- Technical queues:
  - `generation.tasks.retry`: per-message TTL, dead-lettered back into tasks.
  - `generation.tasks.dlq`: receives a task after its 3rd attempt.

## Debug view (local dev only)
Click **Debug** next to a job's status (shown only by `npm run dev`; the server needs `DEBUG_UI=1` and `NODE_ENV` not `production`). It shows a timeline per jobId/taskId, taken from `job_events`:
- **Node events** are recorded when each step happens (`server/src/debug.ts` `record`).
- **Worker events** travel inside the result message and are recorded by Node, because the worker never writes to Mongo.
- Messages are not read from the queues for this view.
- Each step shows its file/function, a "FROM CODE" explanation and the "MEASURED" runtime data.
- Secrets, cookies and base64 are redacted.
- Rejected images are never displayed.
- Jobs created before tracing existed show "not recorded".

## Protections in the MVP
- Dev session via a signed httpOnly cookie, created by the server. Every job and image read checks ownership.
- PNG/JPEG only, max 5MB, decoded-pixel cap, normalized to ≤1024 on a white background. Description ≤1000 characters.
- Order of checks on create:
  1. `Idempotency-Key`: a repeat returns the same job.
  2. Rate limit: 3 per 10 min and 10 per 24h. Over the limit returns 429 + Retry-After. Redis down returns 503.
  3. Input validation.
  4. One active job per session (409).
  5. Backpressure: 30 or more pending tasks returns 503.
  6. Daily neuron budget.
- Polling is limited to 30/min. It falls back to an in-memory limiter when Redis is down.
- Refine:
  - It uses the **selected image**, not the original sketch.
  - The input check is reused by `sha256(image)` + policy version.
  - An atomic lock on the parent (`refineJobId`) allows only one refine per design.
  - The refine job is upserted with a pre-chosen id, and a recovery sweep re-creates it after a crash.
- Worker retries (max 3 attempts):
  - Retried: only failures where the request certainly did not execute (connect failure, explicit 429 rate limit, 500/503 with an error body).
  - Read timeout, a connection dropped after the request was sent, or 502/504 → `UNKNOWN`, no retry.
  - Quota exhausted or content blocked → `FAILED`, no retry.
- A checkpoint of `results/{taskId}.png` means a result is republished, never regenerated.
- Rejected images are never listed or downloadable.

## Free-tier guard
- Before creating a job with `PROVIDER=cloudflare`, Node reserves estimated neurons in Redis, up to `DAILY_NEURON_BUDGET`. The free allocation is 10,000 per day.
- The estimate uses the dimensions actually sent: `tiles(ref ≤512²)×5.37 + tiles(out 1024²)×26.05` ≈ 110 neurons per image, so about 660 per 6-variation design. It is an estimate, not billing data.
- Mock runs reserve nothing.
