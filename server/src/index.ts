import express, { Request, Response, NextFunction } from "express";
import cookieParser from "cookie-parser";
import multer from "multer";
import crypto from "node:crypto";
import { config, estimateNeurons } from "./config.js";
import { connectDb, jobs, sessions, Job, TERMINAL_STAGE } from "./db.js";
import { ensureBucket, getObject, putObject } from "./storage.js";
import { normalizeUpload, BadImage, sha256 } from "./image.js";
import { connectRabbit, rabbitReady } from "./rabbit.js";
import { redis, redisReady, consume, createShort, createDaily, pollLimiter, reserveNeurons, releaseNeurons, neuronsUsedToday } from "./redis.js";
import { onResult, startLoops, storeReference } from "./loops.js";
import { guardrailIsMock } from "./guardrails.js";
import { record, mask, initDebug, eventsFor, debugEnabled } from "./debug.js";
import { refKey } from "./loops.js";

const FI = "server/src/index.ts";

const providerIsMock = () => config.provider !== "cloudflare";
const app = express();
app.use(cookieParser(config.cookieSecret));
app.use(express.json({ limit: "10kb" }));

// ---------- dev session: server-generated id in a signed httpOnly cookie ----------
declare module "express-serve-static-core" {
  interface Request { sid: string }
}
app.use("/api", async (req, res, next) => {
  try {
    let sid = req.signedCookies?.sid as string | undefined;
    if (!sid || !(await sessions.findOne({ _id: sid }))) {
      sid = crypto.randomUUID();
      await sessions.insertOne({ _id: sid, createdAt: new Date() });
      res.cookie("sid", sid, { httpOnly: true, signed: true, sameSite: "lax", maxAge: 30 * 86400_000 });
    }
    req.sid = sid;
    next();
  } catch (e) {
    next(e);
  }
});

type AsyncHandler = (req: Request, res: Response, next: NextFunction) => Promise<unknown>;
const ah = (fn: AsyncHandler) => (req: Request, res: Response, next: NextFunction) => { fn(req, res, next).catch(next); };

const deny = (res: Response, status: number, message: string, retryAfter?: number) => {
  if (retryAfter) res.setHeader("Retry-After", String(retryAfter));
  return res.status(status).json({ error: message });
};

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 5 * 1024 * 1024, files: 1, fields: 5 } });

async function pendingTaskCount() {
  const r = await jobs
    .aggregate([
      { $match: { stage: { $in: ["QUEUED", "GENERATING", "VALIDATING_OUTPUT"] } } },
      { $unwind: "$tasks" },
      { $match: { "tasks.status": { $in: ["PENDING_PUBLISH", "PUBLISHED"] } } },
      { $count: "n" },
    ])
    .toArray();
  return r[0]?.n ?? 0;
}

async function admissionChecks(sid: string): Promise<{ status: number; msg: string } | null> {
  const active = await jobs.findOne({ sessionId: sid, stage: { $nin: TERMINAL_STAGE } }, { projection: { _id: 1 } });
  if (active) return { status: 409, msg: `You already have an active job (${active._id}). Wait for it to finish.` };
  if ((await pendingTaskCount()) >= config.maxPendingTasks)
    return { status: 503, msg: "System is busy (generation queue is full). Try again in a few minutes." };
  return null;
}

/** Neurons actually reserved only when the real (billable-quota) provider is used. */
async function reserveFor(refW: number, refH: number, tasks: number) {
  if (providerIsMock()) return { ok: true, amount: 0 };
  const amount = tasks * estimateNeurons(refW, refH, config.genWidth, config.genHeight);
  return { ok: await reserveNeurons(amount), amount };
}

// ---------- POST /api/jobs ----------
app.post("/api/jobs", upload.single("image"), ah(async (req, res) => {
  const sid = req.sid;
  const idemKey = (req.header("Idempotency-Key") || "").slice(0, 100) || undefined;

  // 1. Idempotency first: a repeated request returns the same job, before any other gate.
  if (idemKey) {
    const existing = await jobs.findOne({ sessionId: sid, idempotencyKey: idemKey }, { projection: { _id: 1 } });
    if (existing) {
      record({ jobId: existing._id, step: "http.create.duplicate", file: FI, fn: "POST /api/jobs", data: { idempotencyKey: idemKey, result: "same Idempotency-Key → existing jobId returned (202), no other gate evaluated" } });
      return res.status(202).json({ jobId: existing._id, duplicate: true });
    }
  }
  // 2. Rate limit (Redis down -> 503)
  const limits: Record<string, unknown> = {};
  for (const [name, l] of [["3 per 10 min", createShort], ["10 per 24 h", createDaily]] as const) {
    const r = await consume(l, sid);
    if (!r.ok) return deny(res, r.status, r.message, r.retryAfter);
    limits[name] = { key: `${l.keyPrefix}:${mask(sid)}`, remainingAfter: r.remaining, windowResetsInSec: r.resetInSec };
  }
  // 3. Input validation
  const description = String(req.body?.description ?? "").trim();
  if (!req.file) return deny(res, 400, "Please draw or upload a sketch (PNG/JPEG).");
  if (description.length > 1000) return deny(res, 400, "Description is limited to 1000 characters.");
  if (!["image/png", "image/jpeg"].includes(req.file.mimetype)) return deny(res, 400, "Only PNG or JPEG allowed.");
  let img;
  try {
    img = await normalizeUpload(req.file.buffer);
  } catch (e) {
    if (e instanceof BadImage) return deny(res, 400, e.message);
    throw e;
  }
  // 4. Active job / backpressure
  const gate = await admissionChecks(sid);
  if (gate) return deny(res, gate.status, gate.msg);

  // 5. Store private images, reserve quota, create job
  const jobId = crypto.randomUUID();
  const inputKey = `inputs/${jobId}.png`;
  await putObject(inputKey, img.png);
  const ref = await storeReference(jobId, img.png);
  const q = await reserveFor(ref.width, ref.height, config.variations);
  if (!q.ok) return deny(res, 503, "Daily free generation quota reached. Try again tomorrow.");

  const job: Job = {
    _id: jobId,
    sessionId: sid,
    kind: "GENERATE",
    idempotencyKey: idemKey,
    stage: "PENDING_INPUT_CHECK",
    inputKey,
    inputHash: img.hash,
    inputWidth: ref.width,
    inputHeight: ref.height,
    description,
    mock: { provider: providerIsMock(), guardrail: guardrailIsMock() },
    refineJobId: null,
    tasks: [],
    createdAt: new Date(),
    updatedAt: new Date(),
  };
  (job as any).neuronsReserved = q.amount;
  try {
    await jobs.insertOne(job);
  } catch (e: any) {
    if (e?.code === 11000 && idemKey) {
      await releaseNeurons(q.amount);
      const existing = await jobs.findOne({ sessionId: sid, idempotencyKey: idemKey });
      return res.status(202).json({ jobId: existing?._id, duplicate: true });
    }
    throw e;
  }
  console.log(`[job ${jobId}] created (session ${sid.slice(0, 8)})`);
  record({
    jobId, step: "http.create", file: FI, fn: "POST /api/jobs",
    data: {
      idempotencyKey: idemKey ?? "none", rateLimit: { store: "Redis (rate-limiter-flexible, RateLimiterRedis)", ...limits },
      upload: { mimetype: req.file.mimetype, bytes: req.file.size, note: "original upload is not stored; only the normalized copy" },
      normalized: { key: inputKey, sha256: img.hash, width: img.width, height: img.height, steps: "decode check → rotate → fit inside 1024 → white background → PNG" },
      fluxReference: { key: refKey({ _id: jobId }), width: ref.width, height: ref.height, note: "downscaled ≤512 copy that the worker sends to FLUX" },
      storage: `SeaweedFS S3 bucket "${config.s3.bucket}" (private, served only through Node)`,
      neuronsReserved: q.amount, description,
      response: { status: 202, body: { jobId } },
    },
    mongo: { collection: "jobs", op: "insertOne", filter: { _id: jobId }, fields: { kind: "GENERATE", stage: "PENDING_INPUT_CHECK", inputKey, inputHash: img.hash, description, idempotencyKey: idemKey, tasks: [], neuronsReserved: q.amount } },
  });
  res.status(202).json({ jobId });
}));

// ---------- GET /api/jobs/:id (polling) ----------
const pollGuard = async (req: Request, res: Response, next: NextFunction) => {
  const r = await consume(pollLimiter, req.sid);
  if (!r.ok && r.status === 429) {
    if (req.params.id) record({ jobId: req.params.id, step: "poll.limited", file: FI, fn: "pollGuard", data: { limit: "30 per minute", retryAfter: r.retryAfter, response: 429 } });
    return deny(res, 429, "Too many requests", r.retryAfter);
  }
  res.locals.pollRemaining = r.ok ? r.remaining : "unknown (Redis down → in-memory insurance limiter)";
  next(); // insurance limiter keeps reads working when Redis is down
};

const STAGE_MSG: Record<string, string> = {
  PENDING_INPUT_CHECK: "Checking your request...",
  CHECKING_INPUT: "Checking your request...",
  QUEUED: "Queued for generation...",
  GENERATING: "Generating ring images...",
  VALIDATING_OUTPUT: "Checking generated images...",
  COMPLETED: "Done.",
  FAILED: "Failed.",
  REJECTED: "Your request was rejected by the content check.",
};

app.get("/api/jobs/:id", pollGuard, ah(async (req, res) => {
  const job = await jobs.findOne({ _id: req.params.id, sessionId: req.sid });
  if (!job) return deny(res, 404, "Job not found");
  const count = (s: string) => job.tasks.filter((t) => t.status === s).length;
  const approved = job.tasks.filter((t) => t.status === "APPROVED");
  let message = STAGE_MSG[job.stage];
  if (job.stage === "COMPLETED" && approved.length < job.tasks.length)
    message = `Done. ${approved.length} of ${job.tasks.length} images passed the checks.`;
  // errorCode lets the UI tell "content rejected" apart from "check unavailable" (technical, not a rejection).
  const errorCode = (job as any).errorCode as string | undefined;
  if (errorCode === "CONTENT_CHECK_UNAVAILABLE") message = "Content check unavailable — not rejected.";
  if (errorCode === "CONTENT_CHECK_MISCONFIGURED") message = "Content check is not working (server configuration).";
  const body = {
    jobId: job._id,
    kind: job.kind,
    stage: job.stage,
    terminal: TERMINAL_STAGE.includes(job.stage),
    message,
    error: job.error,
    errorCode,
    mock: job.mock,
    parentJobId: job.parentJobId,
    refineJobId: job.refineJobId ?? null,
    counts: { total: job.tasks.length, approved: approved.length, rejected: count("REJECTED"), failed: count("FAILED") + count("UNKNOWN") },
    candidates: approved.map((t) => ({ candidateId: t.taskId, url: `/api/jobs/${job._id}/candidates/${t.taskId}/image` })),
  };
  // Record only polls whose response differs from the previous one (not every 3s tick).
  const snap = JSON.stringify([body.stage, body.counts, body.refineJobId]);
  if (lastPoll.get(job._id) !== snap) {
    lastPoll.set(job._id, snap);
    record({
      jobId: job._id, step: "poll.update", file: FI, fn: "GET /api/jobs/:id",
      data: { pollLimitRemainingThisMinute: res.locals.pollRemaining, response: { ...body, candidates: body.candidates.map((c) => c.candidateId) } },
    });
  }
  res.json(body);
}));
const lastPoll = new Map<string, string>();

// ---------- latest job of this session (restore after refresh) ----------
app.get("/api/session/latest", pollGuard, ah(async (req, res) => {
  const job = await jobs.find({ sessionId: req.sid }).sort({ createdAt: -1 }).limit(1).next();
  res.json({ jobId: job?._id ?? null });
}));

// ---------- image download (owner + approved only) ----------
// Not counted against the polling limit: a 6-image gallery would otherwise hit 429 and show broken images.
app.get("/api/jobs/:id/candidates/:cid/image", ah(async (req, res) => {
  const job = await jobs.findOne({ _id: req.params.id, sessionId: req.sid });
  const t = job?.tasks.find((t) => t.taskId === req.params.cid && t.status === "APPROVED");
  if (!job || !t?.resultKey) return deny(res, 404, "Image not found");
  const buf = await getObject(t.resultKey);
  res.setHeader("Content-Type", "image/png");
  res.setHeader("Cache-Control", "private, no-store");
  if (req.query.download) res.setHeader("Content-Disposition", `attachment; filename="ring-${t.taskId.slice(0, 8)}.png"`);
  res.send(buf);
}));

// ---------- refine (one per design; lock + crash-safe creation) ----------
async function ensureRefineJob(parent: Job): Promise<void> {
  const req = (parent as any).refineRequest as { candidateId: string } | undefined;
  if (!parent.refineJobId || !req) return;
  if (await jobs.findOne({ _id: parent.refineJobId }, { projection: { _id: 1 } })) return;
  const cand = parent.tasks.find((t) => t.taskId === req.candidateId)!;
  const png = await getObject(cand.resultKey!);
  const ref = await storeReference(parent.refineJobId, png);
  const refine: Job = {
    _id: parent.refineJobId,
    sessionId: parent.sessionId,
    kind: "REFINE",
    parentJobId: parent._id,
    parentCandidateId: req.candidateId,
    stage: "PENDING_INPUT_CHECK",
    inputKey: cand.resultKey!, // the SELECTED image, not the original sketch
    inputHash: sha256(png),
    inputWidth: ref.width,
    inputHeight: ref.height,
    description: parent.description,
    design: parent.design,
    mock: { provider: providerIsMock(), guardrail: guardrailIsMock() },
    tasks: [],
    createdAt: new Date(),
    updatedAt: new Date(),
  };
  // Upsert with a pre-chosen id: re-running after a crash cannot create a second refine.
  const up = await jobs.updateOne({ _id: refine._id }, { $setOnInsert: refine }, { upsert: true });
  console.log(`[job ${refine._id}] refine created from ${parent._id}/${req.candidateId}`);
  if (up.upsertedCount) {
    const data = {
      parentJobId: parent._id, selectedCandidateId: req.candidateId, selectedImageKey: cand.resultKey,
      selectedImageSha256: refine.inputHash, fluxReference: { key: refKey(refine), width: ref.width, height: ref.height },
      savedDesign: parent.design, description: parent.description,
      note: "refine input is the SELECTED approved image, not the original sketch",
    };
    record({
      jobId: refine._id, step: "refine.create", file: FI, fn: "ensureRefineJob", data,
      mongo: { collection: "jobs", op: "updateOne upsert ($setOnInsert, pre-chosen _id)", filter: { _id: refine._id }, fields: { kind: "REFINE", parentJobId: parent._id, parentCandidateId: req.candidateId, stage: "PENDING_INPUT_CHECK", inputKey: cand.resultKey, design: parent.design } },
    });
    record({ jobId: parent._id, taskId: req.candidateId, step: "refine.link", file: FI, fn: "ensureRefineJob", data: { refineJobId: refine._id, selectedCandidateId: req.candidateId } });
  }
}

async function recoverRefines() {
  const orphans = await jobs.find({ refineJobId: { $type: "string" }, kind: "GENERATE" }).sort({ updatedAt: -1 }).limit(20).toArray();
  for (const p of orphans) await ensureRefineJob(p).catch((e) => console.error("[refine-recovery]", e.message));
  return false;
}

app.post("/api/jobs/:id/refine", ah(async (req, res) => {
  const candidateId = String(req.body?.candidateId ?? "");
  const load = () => jobs.findOne({ _id: req.params.id, sessionId: req.sid });
  // If a refine already exists: same candidate => return it (double click / retry), else 409.
  const existingRefine = async (p: Job | null) => {
    if (!p?.refineJobId) return false;
    if ((p as any).refineRequest?.candidateId !== candidateId) {
      deny(res, 409, "This design was already refined once.");
      return true;
    }
    await ensureRefineJob(p);
    res.status(202).json({ jobId: p.refineJobId, duplicate: true });
    return true;
  };
  const parent = await load();
  if (!parent) return deny(res, 404, "Job not found");
  if (parent.kind !== "GENERATE") return deny(res, 400, "Only one refinement round is allowed.");
  if (await existingRefine(parent)) return;
  if (!parent.tasks.some((t) => t.taskId === candidateId && t.status === "APPROVED"))
    return deny(res, 400, "Select an approved image.");
  const gate = await admissionChecks(req.sid);
  if (gate) {
    if (await existingRefine(await load())) return; // raced with a concurrent identical click
    return deny(res, gate.status, gate.msg);
  }
  const q = await reserveFor(config.refMax, config.refMax, 1);
  if (!q.ok) return deny(res, 503, "Daily free generation quota reached. Try again tomorrow.");

  const refineJobId = crypto.randomUUID();
  // Atomic lock: only one refine per design, and the request is persisted with the lock.
  const locked = await jobs.findOneAndUpdate(
    { _id: parent._id, refineJobId: null },
    { $set: { refineJobId, refineRequest: { candidateId }, updatedAt: new Date() } as any },
    { returnDocument: "after" }
  );
  if (!locked) {
    await releaseNeurons(q.amount);
    if (await existingRefine(await load())) return;
    return deny(res, 409, "This design was already refined once.");
  }
  record({
    jobId: parent._id, taskId: candidateId, step: "refine.lock", file: FI, fn: "POST /api/jobs/:id/refine",
    data: { refineJobId, neuronsReserved: q.amount, rule: "atomic: succeeds only while refineJobId is null → one refine per design" },
    mongo: { collection: "jobs", op: "findOneAndUpdate (atomic lock)", filter: { _id: parent._id, refineJobId: null }, fields: { refineJobId, refineRequest: { candidateId } } },
  });
  await ensureRefineJob(locked);
  res.status(202).json({ jobId: refineJobId });
}));

// ---------- dev-only debug view (owner only; DEBUG_UI=1 and NODE_ENV!=production) ----------
const debugOnly = (req: Request, res: Response, next: NextFunction) =>
  debugEnabled() ? next() : deny(res, 404, "Not found");

app.get("/api/debug/jobs", debugOnly, ah(async (req, res) => {
  const list = await jobs.find({ sessionId: req.sid }, { projection: { _id: 1, kind: 1, stage: 1, createdAt: 1, parentJobId: 1 } })
    .sort({ createdAt: -1 }).limit(15).toArray();
  res.json(list);
}));

app.get("/api/debug/jobs/:id", debugOnly, ah(async (req, res) => {
  const job = await jobs.findOne({ _id: req.params.id, sessionId: req.sid });
  if (!job) return deny(res, 404, "Job not found");
  const events = await eventsFor(job._id);
  const { sessionId, ...rest } = job as any;
  const doc = { ...rest, sessionId: mask(sessionId), tasks: job.tasks.map(({ ...t }: any) => ({ ...t, claim: t.claim ? "<claim token>" : undefined })) };
  const ids = [job._id, job.parentJobId, job.refineJobId].filter(Boolean);
  res.json({
    job: doc,
    events,
    tracingNote: events.length
      ? "Events were recorded at runtime as each step happened."
      : "No runtime events: this job was processed before debug tracing existed (not recorded).",
    compass: {
      connection: config.mongoUrl.replace(/\/\/[^@]*@/, "//"),
      database: "sketch2ring",
      queries: [
        { collection: "jobs", filter: `{ "_id": "${job._id}" }` },
        { collection: "job_events", filter: `{ "jobId": "${job._id}" }`, sort: `{ "ts": 1 }` },
        ...(job.parentJobId ? [{ collection: "jobs", filter: `{ "_id": "${job.parentJobId}" }`, note: "parent design" }] : []),
        ...(job.refineJobId ? [{ collection: "jobs", filter: `{ "_id": "${job.refineJobId}" }`, note: "refine job" }] : []),
        { collection: "guard_checks", filter: `{ "sourceJobId": { "$in": ${JSON.stringify(ids)} } }`, note: "cached guardrail verdicts (sourceJobId recorded only for new entries)" },
      ],
    },
    rabbitUi: "http://127.0.0.1:15672 (guest/guest) → Queues: generation.tasks, generation.results, generation.tasks.retry, generation.tasks.dlq",
  });
}));

// Serves only images that belong to this job and that may be shown: the sketch, the FLUX reference,
// and APPROVED outputs. Rejected/failed outputs are never served, also not in debug.
app.get("/api/debug/jobs/:id/object", debugOnly, ah(async (req, res) => {
  const job = await jobs.findOne({ _id: req.params.id, sessionId: req.sid });
  const key = String(req.query.key ?? "");
  if (!job) return deny(res, 404, "Not found");
  const allowed = new Set([job.inputKey, refKey(job), ...job.tasks.filter((t) => t.status === "APPROVED").map((t) => t.resultKey!)]);
  if (!allowed.has(key)) return deny(res, 404, "Not available (not part of this job, or not an approved image)");
  res.setHeader("Content-Type", "image/png");
  res.setHeader("Cache-Control", "private, no-store");
  res.send(await getObject(key));
}));

// ---------- health ----------
app.get("/health", async (_req, res) => {
  const mongo = await jobs.estimatedDocumentCount().then(() => true).catch(() => false);
  res.json({
    ok: mongo && redisReady() && rabbitReady(),
    mongo,
    redis: redisReady(),
    rabbit: rabbitReady(),
    provider: providerIsMock() ? "FAKE (mock)" : config.cfModel,
    guardrail: guardrailIsMock() ? "MOCK" : config.geminiModel,
    neuronsReservedToday: await neuronsUsedToday(),
    dailyNeuronBudget: config.dailyNeuronBudget,
  });
});

app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
  if (err?.code === "LIMIT_FILE_SIZE") return deny(res, 413, "Image is larger than 5MB.");
  if (err instanceof multer.MulterError) return deny(res, 400, err.message);
  console.error("[http]", err);
  deny(res, 500, "Internal error");
});

async function main() {
  const client = await connectDb();
  await initDebug(client.db());
  await ensureBucket();
  await connectRabbit(onResult);
  startLoops(recoverRefines);
  app.listen(config.port, "127.0.0.1", () =>
    console.log(`[server] http://127.0.0.1:${config.port}  provider=${providerIsMock() ? "FAKE" : "cloudflare"} guardrail=${guardrailIsMock() ? "MOCK" : "gemini"}`)
  );
  void redis;
}
main().catch((e) => {
  console.error("[server] fatal", e);
  process.exit(1);
});
