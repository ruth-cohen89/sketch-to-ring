import crypto from "node:crypto";
import sharp from "sharp";
import type { Channel, ConsumeMessage } from "amqplib";
import { config, QUEUES } from "./config.js";
import { jobs, guardChecks, Job, Task, TERMINAL_STAGE, TERMINAL_TASK } from "./db.js";
import { checkInput, checkOutput, GuardError } from "./guardrails.js";
import { buildGeneratePrompt, buildRefinePrompt, genOptions, modelId, GENERATE_TEMPLATE, REFINE_TEMPLATE } from "./prompt.js";
import { getObject, putObject } from "./storage.js";
import { publishTask, rabbitReady } from "./rabbit.js";
import { sha256 } from "./image.js";
import { releaseNeurons } from "./redis.js";
import { record } from "./debug.js";

const LEASE_MS = 90_000;
const now = () => new Date();
const lease = () => new Date(Date.now() + LEASE_MS);
const log = (jobId: string, msg: string, taskId?: string) =>
  console.log(`[job ${jobId}${taskId ? ` task ${taskId}` : ""}] ${msg}`);
const F = "server/src/loops.ts";
// Backoff after a technical guardrail failure (timeout / 503): an immediate retry lands in the same
// provider load spike. Attempt 1 fails → wait 15 s, attempt 2 fails → wait 45 s, attempt 3 fails → not approved.
const GUARD_BACKOFF_MS = [15_000, 45_000];
const notBefore = (attempt: number) => new Date(Date.now() + GUARD_BACKOFF_MS[Math.min(attempt, GUARD_BACKOFF_MS.length) - 1]);
const due = () => ({ $or: [{ notBefore: { $exists: false } }, { notBefore: { $lte: now() } }] });

// ---------------- 1. Input guardrail (atomic claim + expiring lease) ----------------
async function inputCheckOnce() {
  const leaseUntil = lease();
  const job = await jobs.findOneAndUpdate(
    {
      $or: [
        { stage: "PENDING_INPUT_CHECK", ...due() },
        { stage: "CHECKING_INPUT", leaseUntil: { $lt: now() } },
      ],
    },
    { $set: { stage: "CHECKING_INPUT", leaseUntil, updatedAt: now() }, $inc: { inputCheckAttempts: 1 } as any },
    { returnDocument: "after" }
  );
  if (!job) return false;
  const attempt = (job as any).inputCheckAttempts ?? 1;
  record({
    jobId: job._id, step: "input.claim", file: F, fn: "inputCheckOnce",
    data: { attempt, leaseUntil },
    mongo: { collection: "jobs", op: "findOneAndUpdate (atomic claim)", filter: { _id: job._id, stage: "PENDING_INPUT_CHECK | CHECKING_INPUT with expired lease" }, fields: { stage: "CHECKING_INPUT", leaseUntil, $inc: { inputCheckAttempts: 1 } } },
  });
  try {
    const cacheId =
      job.kind === "REFINE"
        ? `image:${job.inputHash}:${config.policyVersion}`
        : `input:${sha256(Buffer.from(job.inputHash + "\n" + job.description))}:${config.policyVersion}`;
    let cached = await guardChecks.findOne({ _id: cacheId });
    let verdict: string, reason: string | undefined, design: Record<string, string | undefined>;
    if (cached) {
      ({ verdict, reason } = cached);
      design = job.kind === "REFINE" ? job.design ?? {} : cached.design ?? {};
      log(job._id, `input check reused from cache (${cacheId.split(":")[0]})`);
      record({
        jobId: job._id, step: "guard.input", file: F, fn: "inputCheckOnce",
        data: {
          realCall: false, cached: true, cacheId, policyVersion: config.policyVersion,
          cachedRecord: { verdict: cached.verdict, reason: cached.reason, design: cached.design, createdAt: cached.createdAt, sourceJobId: (cached as any).sourceJobId ?? "not recorded", sourceTaskId: (cached as any).sourceTaskId },
          noteOnCache: "No request was sent to Gemini. The original request/response is in the debug view of sourceJobId (if it was recorded).",
          imageKey: job.inputKey, description: job.description, decision: verdict === "allow" ? "allow → build prompt" : "reject",
        },
      });
    } else {
      const png = await getObject(job.inputKey);
      const r = await checkInput(job.description, png, (request) =>
        record({
          jobId: job._id, step: "guard.input.request", file: "server/src/guardrails.ts", fn: "checkInput → callGemini",
          data: { attempt, cacheId, cacheResult: "miss", imageKey: job.inputKey, imageSha256: job.inputHash, request },
        })
      );
      ({ verdict, reason, design } = r);
      const extracted = r.design;
      if (job.kind === "REFINE") design = job.design ?? {};
      await guardChecks.updateOne(
        { _id: cacheId },
        { $setOnInsert: { verdict, reason, design, createdAt: now(), sourceJobId: job._id } as any },
        { upsert: true }
      );
      record({
        jobId: job._id, step: "guard.input", file: F, fn: "inputCheckOnce → guardrails.ts checkInput",
        data: {
          realCall: !r.trace.mock, cached: false, cacheId, policyVersion: config.policyVersion, imageKey: job.inputKey,
          trace: r.trace, parsed: { verdict, reason, extracted },
          serverValidation: "verdict must be allow|reject|uncertain; only 'allow' passes (uncertain = not approved)",
          decision: verdict === "allow" ? "allow → build prompt" : `${verdict} → REJECTED`,
        },
        mongo: { collection: "guard_checks", op: "updateOne upsert ($setOnInsert)", filter: { _id: cacheId }, fields: { verdict, reason, design, sourceJobId: job._id } },
      });
    }
    if (verdict !== "allow") {
      const set = { stage: "REJECTED", errorCode: "CONTENT_REJECTED", inputCheck: { verdict, reason, cached: !!cached },
                    error: verdict === "uncertain" ? "The content check could not confirm this is an allowed ring design, so it was not approved." : "Your request was rejected by the content check." };
      await jobs.updateOne({ _id: job._id, stage: "CHECKING_INPUT" }, { $set: { ...set, updatedAt: now() } as any });
      const released = (job as any).neuronsReserved ?? 0;
      await releaseNeurons(released); // nothing will be generated
      log(job._id, `input REJECTED (${verdict}): ${reason}`);
      record({
        jobId: job._id, step: "job.rejected", file: F, fn: "inputCheckOnce",
        data: { verdict, reason, neuronsReleased: released },
        mongo: { collection: "jobs", op: "updateOne", filter: { _id: job._id, stage: "CHECKING_INPUT" }, fields: set },
      });
      return true;
    }
    const isRefine = job.kind === "REFINE";
    const rendered = isRefine ? buildRefinePrompt(design) : buildGeneratePrompt(design, job.description);
    const prompt = rendered.prompt;
    const n = isRefine ? 1 : config.variations;
    const tasks: Task[] = Array.from({ length: n }, () => ({
      taskId: crypto.randomUUID(),
      status: "PENDING_PUBLISH",
      attempts: 0,
      updatedAt: now(),
    }));
    const templateVersion = isRefine ? `${config.templateVersion}-refine` : config.templateVersion;
    // Outbox: ONE generation task (generationTaskId, imageCount = n) + its n per-image candidates,
    // stored atomically in the job document.
    const generationTaskId = crypto.randomUUID();
    const set = { stage: "QUEUED", design, prompt, templateVersion, modelId: modelId(), options: genOptions(), generationTaskId, tasks, inputCheck: { verdict, reason, cached: !!cached } };
    await jobs.updateOne({ _id: job._id, stage: "CHECKING_INPUT" }, { $set: { ...set, updatedAt: now() } as any });
    log(job._id, `input allowed, 1 generation task (imageCount=${n}) queued in outbox`);
    record({
      jobId: job._id, step: "prompt.build", file: "server/src/prompt.ts", fn: isRefine ? "buildRefinePrompt" : "buildGeneratePrompt",
      data: {
        templateVersion, template: isRefine ? REFINE_TEMPLATE : GENERATE_TEMPLATE,
        designFields: design, filledPlaceholders: rendered.used, omittedPlaceholders: rendered.omitted,
        defaults: "none — a template line whose field is unknown is dropped",
        finalPrompt: prompt, modelId: modelId(), options: genOptions(), variations: n,
      },
    });
    record({
      jobId: job._id, step: "outbox.write", file: F, fn: "inputCheckOnce",
      data: { generationTaskId, imageCount: n, candidateIds: tasks.map((t) => t.taskId), note: "One RabbitMQ message per job. tasks[] holds one candidate per image; PENDING_PUBLISH = the generation task is not yet published" },
      mongo: { collection: "jobs", op: "updateOne (single atomic write: tasks + prompt + stage)", filter: { _id: job._id, stage: "CHECKING_INPUT" }, fields: { ...set, tasks: tasks.map((t) => ({ taskId: t.taskId, status: t.status })) } },
    });
  } catch (e) {
    // Technical failure: never approval. Config errors (key/permission/bad request) will not fix themselves → stop now.
    const kind = e instanceof GuardError ? e.kind : "unavailable";
    const fail = attempt >= 3 || kind === "config";
    if (fail) await releaseNeurons((job as any).neuronsReserved ?? 0);
    const set = !fail
      ? { stage: "PENDING_INPUT_CHECK", notBefore: notBefore(attempt) }
      : kind === "config"
        ? { stage: "FAILED", errorCode: "CONTENT_CHECK_MISCONFIGURED", error: "The content check is misconfigured on the server, so the request was not checked or approved. Nothing was generated." }
        : { stage: "FAILED", errorCode: "CONTENT_CHECK_UNAVAILABLE", error: "The content check service is temporarily unavailable, so your request could not be checked. It was not rejected — please try again in a few minutes." };
    await jobs.updateOne({ _id: job._id, stage: "CHECKING_INPUT" }, { $set: { ...set, updatedAt: now() } as any });
    log(job._id, `input check error (attempt ${attempt}): ${(e as Error).message}`);
    record({
      jobId: job._id, step: "guard.input.error", file: F, fn: "inputCheckOnce",
      data: { attempt, errorKind: kind, error: (e as Error).message, trace: e instanceof GuardError ? e.trace : undefined,
              decision: fail ? `${kind === "config" ? "configuration error" : "3 attempts"} → FAILED (not approved)` : `released for retry, not before ${(set as any).notBefore?.toISOString()}` },
      mongo: { collection: "jobs", op: "updateOne", filter: { _id: job._id, stage: "CHECKING_INPUT" }, fields: set },
    });
  }
  return true;
}

// ---------------- 2. Outbox publisher ----------------
async function publishOnce() {
  if (!rabbitReady()) return false;
  const list = await jobs.find({ stage: { $in: ["QUEUED", "GENERATING"] }, "tasks.status": "PENDING_PUBLISH" }).limit(10).toArray();
  for (const job of list) {
    // One message for the whole job: the worker produces imageCount images for these candidateIds.
    const candidateIds = job.tasks.filter((t) => t.status === "PENDING_PUBLISH").map((t) => t.taskId);
    const genTaskId = job.generationTaskId ?? candidateIds[0];
    const payload = {
      taskId: genTaskId,
      jobId: job._id,
      kind: job.kind,
      attempt: 1,
      imageCount: candidateIds.length,
      candidateIds,
      prompt: job.prompt,
      inputKey: refKey(job),
      modelId: job.modelId,
      width: job.options?.width,
      height: job.options?.height,
    };
    const publishedAt = now();
    const ok = await publishTask(payload, genTaskId);
    const confirmedAt = now();
    record({
      jobId: job._id, taskId: genTaskId, step: "rabbit.publish", file: "server/src/rabbit.ts", fn: "publishTask (called by loops.ts publishOnce)",
      data: {
        queue: QUEUES.tasks, publisher: "Node (outbox publisher)", consumer: "Python worker",
        message: payload, properties: { persistent: true, mandatory: true, messageId: genTaskId },
        publishedAt, brokerConfirmAt: confirmedAt, confirmMs: confirmedAt.getTime() - publishedAt.getTime(),
        routed: ok,
      },
    });
    if (!ok) {
      log(job._id, "publish unroutable - will retry", genTaskId);
      continue;
    }
    await jobs.updateOne(
      { _id: job._id },
      { $set: { "tasks.$[t].status": "PUBLISHED", "tasks.$[t].publishedAt": now(), "tasks.$[t].updatedAt": now(), stage: "GENERATING", updatedAt: now() } },
      { arrayFilters: [{ "t.taskId": { $in: candidateIds }, "t.status": "PENDING_PUBLISH" }] }
    );
    log(job._id, `published generation task (imageCount=${candidateIds.length}, confirmed)`, genTaskId);
    record({
      jobId: job._id, taskId: genTaskId, step: "outbox.mark", file: F, fn: "publishOnce",
      data: { note: "all candidates of the generation task marked only after broker confirm", candidateIds },
      mongo: { collection: "jobs", op: "updateOne (arrayFilters)", filter: { _id: job._id, "tasks.taskId": { $in: candidateIds }, "tasks.status": "PENDING_PUBLISH" }, fields: { "tasks.$[].status": "PUBLISHED", stage: "GENERATING" } },
    });
  }
  return list.length > 0;
}

export const refKey = (job: Pick<Job, "_id">) => `inputs/${job._id}-ref.png`;

/** Stores the downscaled reference image actually sent to FLUX; returns its dimensions. */
export async function storeReference(jobId: string, png: Buffer) {
  const { data, info } = await sharp(png)
    .resize(config.refMax, config.refMax, { fit: "inside", withoutEnlargement: true })
    .png()
    .toBuffer({ resolveWithObject: true });
  await putObject(refKey({ _id: jobId }), data);
  return { width: info.width, height: info.height };
}

// ---------------- 3. Results consumer (ack right after persisting) ----------------
export async function onResult(msg: ConsumeMessage, ch: Channel) {
  const receivedAt = now();
  type ImageResult = { candidateId: string; status: string; resultKey?: string; error?: string };
  let r: { taskId: string; jobId: string; attempt?: number; imageCount?: number; results?: ImageResult[]; trace?: any[];
           status?: string; resultKey?: string; error?: string };
  try {
    r = JSON.parse(msg.content.toString());
  } catch {
    console.error("[results] bad message dropped");
    ch.ack(msg);
    return;
  }
  // Worker-side trace travels inside the result message (the worker never writes to Mongo).
  for (const ev of r.trace ?? []) {
    record({ jobId: r.jobId, taskId: ev.candidateId ?? r.taskId, service: "worker", ts: ev.ts ? new Date(ev.ts) : undefined, step: ev.step, file: "worker/" + (ev.file ?? "worker.py"), fn: ev.fn ?? "", data: ev.data ?? {} });
  }
  const { trace: _trace, ...message } = r;
  // One result message per generation task, with one entry per image (legacy single-image shape tolerated).
  const items: ImageResult[] = r.results ?? [{ candidateId: r.taskId, status: r.status ?? "FAILED", resultKey: r.resultKey, error: r.error }];
  try {
    // Dedupe per image: only move a candidate forward from PENDING_PUBLISH/PUBLISHED; late/duplicate results are ignored.
    const ops = items.map((it) => {
      const status = it.status === "GENERATED" ? "GENERATED" : it.status === "UNKNOWN" ? "UNKNOWN" : "FAILED";
      return {
        updateOne: {
          filter: { _id: r.jobId, stage: { $nin: TERMINAL_STAGE } },
          update: {
            $set: {
              "tasks.$[t].status": status, "tasks.$[t].resultKey": it.resultKey, "tasks.$[t].error": it.error?.slice(0, 300),
              "tasks.$[t].attempts": r.attempt ?? 1, "tasks.$[t].updatedAt": now(), stage: "VALIDATING_OUTPUT", updatedAt: now(),
            },
          } as any,
          arrayFilters: [{ "t.taskId": it.candidateId, "t.status": { $in: ["PENDING_PUBLISH", "PUBLISHED"] } }],
        },
      };
    });
    const res = ops.length ? await jobs.bulkWrite(ops as any, { ordered: true }) : null;
    const counts: Record<string, number> = {};
    items.forEach((it) => (counts[it.status] = (counts[it.status] ?? 0) + 1));
    log(r.jobId, `result for ${items.length} image(s) ${JSON.stringify(counts)} (applied ${res?.modifiedCount ?? 0})`, r.taskId);
    ch.ack(msg);
    record({
      jobId: r.jobId, taskId: r.taskId, step: "rabbit.consume.result", file: F, fn: "onResult",
      data: {
        queue: QUEUES.results, publisher: "Python worker", consumer: "Node (results consumer)",
        message, receivedAt, ackAt: now(), redelivered: msg.fields.redelivered, counts,
        dedupe: `${res?.modifiedCount ?? 0} of ${items.length} image results applied; the rest were duplicate/late (candidate already past PUBLISHED)`,
      },
      mongo: { collection: "jobs", op: `bulkWrite: ${items.length} × updateOne (arrayFilters per candidate, only from PENDING_PUBLISH|PUBLISHED)`, filter: { _id: r.jobId }, fields: Object.fromEntries(items.map((it) => [it.candidateId, { status: it.status, resultKey: it.resultKey, error: it.error }])) },
    });
    await finalizeIfDone(r.jobId);
  } catch (e) {
    console.error("[results] persist failed, requeue:", (e as Error).message);
    record({ jobId: r.jobId, taskId: r.taskId, step: "rabbit.consume.result.error", file: F, fn: "onResult", data: { error: (e as Error).message, action: "nack + requeue" } });
    ch.nack(msg, false, true);
  }
}

// ---------------- 4. Output guardrail (atomic claim + expiring lease per task) ----------------
async function outputCheckOnce() {
  const token = crypto.randomUUID();
  const leaseUntil = lease();
  const job = await jobs.findOneAndUpdate(
    {
      stage: { $nin: TERMINAL_STAGE },
      tasks: {
        $elemMatch: {
          $or: [
            { status: "GENERATED", $or: [{ notBefore: { $exists: false } }, { notBefore: { $lte: now() } }] },
            { status: "CHECKING", leaseUntil: { $lt: now() } },
          ],
        },
      },
    },
    { $set: { "tasks.$.status": "CHECKING", "tasks.$.leaseUntil": leaseUntil, "tasks.$.claim": token } as any },
    { returnDocument: "after" }
  );
  if (!job) return false;
  const task = job.tasks.find((t: any) => t.claim === token)!;
  const attempt = ((task as any).checkAttempts ?? 0) + 1;
  record({
    jobId: job._id, taskId: task.taskId, step: "output.claim", file: F, fn: "outputCheckOnce",
    data: { attempt, leaseUntil },
    mongo: { collection: "jobs", op: "findOneAndUpdate (atomic per-task claim)", filter: { _id: job._id, "tasks.status": "GENERATED | CHECKING with expired lease" }, fields: { "tasks.$.status": "CHECKING", "tasks.$.leaseUntil": leaseUntil, "tasks.$.claim": "<token>" } },
  });
  const done = async (set: Record<string, unknown>) =>
    jobs.updateOne(
      { _id: job._id },
      { $set: Object.fromEntries(Object.entries(set).map(([k, v]) => [`tasks.$[t].${k}`, v])) },
      { arrayFilters: [{ "t.taskId": task.taskId, "t.claim": token, "t.status": "CHECKING" }] }
    );
  try {
    const png = await getObject(task.resultKey!);
    const r = await checkOutput(job.description, png, (request) =>
      record({
        jobId: job._id, taskId: task.taskId, step: "guard.output.request", file: "server/src/guardrails.ts", fn: "checkOutput → callGemini",
        data: { attempt, imageKey: task.resultKey, imageSha256: sha256(png), cache: "output checks are never cached", request },
      })
    );
    const approved = r.verdict === "allow";
    const set = { status: approved ? "APPROVED" : "REJECTED", checkReason: r.reason };
    await done({ ...set, updatedAt: now() });
    const cacheId = `image:${sha256(png)}:${config.policyVersion}`;
    if (approved) {
      // Remember approval so a refine of this exact image can reuse the input check.
      await guardChecks.updateOne(
        { _id: cacheId },
        { $setOnInsert: { verdict: "allow", reason: "approved output image", createdAt: now(), sourceJobId: job._id, sourceTaskId: task.taskId } as any },
        { upsert: true }
      );
    }
    log(job._id, `output ${approved ? "APPROVED" : "REJECTED"}: ${r.reason}`, task.taskId);
    record({
      jobId: job._id, taskId: task.taskId, step: "guard.output", file: F, fn: "outputCheckOnce → guardrails.ts checkOutput",
      data: {
        realCall: !r.trace.mock, attempt, imageKey: task.resultKey, trace: r.trace, parsed: { verdict: r.verdict, reason: r.reason },
        decision: approved ? "APPROVED → visible to the user" : `${r.verdict} → REJECTED (never shown or downloadable)`,
        approvalCached: approved ? cacheId : undefined,
      },
      mongo: { collection: "jobs", op: "updateOne (only if this claim still holds)", filter: { _id: job._id, "tasks.taskId": task.taskId, "tasks.claim": "<token>" }, fields: { "tasks.$.status": set.status, "tasks.$.checkReason": set.checkReason } },
    });
  } catch (e) {
    const kind = e instanceof GuardError ? e.kind : "unavailable";
    const set = attempt >= 3 || kind === "config"
      ? { status: "REJECTED", checkReason: `output check unavailable (${kind}) - not approved` }
      : { status: "GENERATED", checkAttempts: attempt, notBefore: notBefore(attempt) };
    await done({ ...set, updatedAt: now() });
    log(job._id, `output check error (${attempt}): ${(e as Error).message}`, task.taskId);
    record({
      jobId: job._id, taskId: task.taskId, step: "guard.output.error", file: F, fn: "outputCheckOnce",
      data: { attempt, error: (e as Error).message, trace: e instanceof GuardError ? e.trace : undefined, decision: attempt >= 3 ? "technical failure x3 → not approved (hidden)" : "released for another check" },
      mongo: { collection: "jobs", op: "updateOne", filter: { _id: job._id, "tasks.taskId": task.taskId }, fields: Object.fromEntries(Object.entries(set).map(([k, v]) => [`tasks.$.${k}`, v])) },
    });
  }
  await finalizeIfDone(job._id);
  return true;
}

async function finalizeIfDone(jobId: string) {
  const job = await jobs.findOne({ _id: jobId });
  if (!job || TERMINAL_STAGE.includes(job.stage) || !job.tasks.length) return;
  if (!job.tasks.every((t) => TERMINAL_TASK.includes(t.status))) return;
  const approved = job.tasks.filter((t) => t.status === "APPROVED").length;
  const set = approved
    ? { stage: "COMPLETED" }
    : { stage: "FAILED", error: "No approved image was produced. Please submit a new design." };
  const res = await jobs.updateOne({ _id: jobId, stage: { $nin: TERMINAL_STAGE } }, { $set: { ...set, updatedAt: now() } as any });
  if (!res.modifiedCount) return;
  log(jobId, `finished: ${approved ? "COMPLETED" : "FAILED"} (${approved}/${job.tasks.length} approved)`);
  const counts: Record<string, number> = {};
  job.tasks.forEach((t) => (counts[t.status] = (counts[t.status] ?? 0) + 1));
  record({
    jobId, step: "job.finalize", file: F, fn: "finalizeIfDone",
    data: { counts, rule: "all tasks terminal → COMPLETED if ≥1 APPROVED, else FAILED" },
    mongo: { collection: "jobs", op: "updateOne", filter: { _id: jobId, stage: "not terminal" }, fields: set },
  });
}

// ---------------- runner ----------------
function loop(name: string, fn: () => Promise<boolean>, idleMs: number) {
  const tick = async () => {
    let busy = false;
    try {
      busy = await fn();
    } catch (e) {
      console.error(`[${name}]`, (e as Error).message);
    }
    setTimeout(tick, busy ? 50 : idleMs);
  };
  tick();
}

export function startLoops(recoverRefines: () => Promise<boolean>) {
  loop("input-check", inputCheckOnce, 1000);
  loop("outbox", publishOnce, 1000);
  loop("output-check", outputCheckOnce, 1000);
  loop("refine-recovery", recoverRefines, 10_000);
}
