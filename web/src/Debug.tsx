import { useEffect, useState } from "react";

// Dev-only "behind the scenes" view. Runtime data comes from server-recorded events (job_events);
// the "From code" text below is a static explanation of what the code does.
type Ev = {
  jobId: string; taskId?: string; ts: string; step: string; service: "node" | "worker"; file: string; fn: string;
  data: Record<string, any>; mongo?: { collection: string; op: string; filter: unknown; fields: unknown };
};
type DebugResp = {
  job: Record<string, any>; events: Ev[]; tracingNote: string;
  compass: { connection: string; database: string; queries: { collection: string; filter: string; sort?: string; note?: string }[] };
  rabbitUi: string;
};

const SECTIONS: Record<number, string> = {
  1: "User input", 2: "Input guardrail (Gemini)", 3: "Prompt template", 4: "MongoDB / outbox", 5: "RabbitMQ",
  6: "Python worker + FLUX + SeaweedFS", 7: "Output guardrail (Gemini)", 8: "Refine round", 9: "Redis rate limit / polling",
};

// From code: what each recorded step is, and where it runs.
const STEP: Record<string, { sec: number; title: string; code: string }> = {
  "http.create": { sec: 1, title: "Request accepted (202)", code: "Order: session → Idempotency-Key → Redis rate limit (3/10min, 10/24h) → validate & normalize image with sharp (image.ts normalizeUpload) → active-job + backpressure → store sketch and ≤512 FLUX reference in SeaweedFS (storage.ts putObject, loops.ts storeReference) → reserve neurons (redis.ts reserveNeurons) → insert job." },
  "http.create.duplicate": { sec: 1, title: "Duplicate request (same Idempotency-Key)", code: "Checked before every other gate; returns the existing jobId without creating anything." },
  "input.claim": { sec: 4, title: "Input check claimed (atomic + lease)", code: "loops.ts inputCheckOnce: findOneAndUpdate moves PENDING_INPUT_CHECK → CHECKING_INPUT with a 90 s lease; an expired lease is re-claimed after a restart." },
  "guard.input": { sec: 2, title: "Input guardrail", code: "Cache key = sha256(image)+description+policy version (generate) or sha256(selected image)+policy (refine). On a miss, guardrails.ts checkInput sends system instruction + description text + image to Gemini with a JSON responseSchema. Only verdict 'allow' passes; uncertain, timeout or invalid JSON is not approval." },
  "guard.input.request": { sec: 2, title: "Input guardrail request (recorded before sending)", code: "Recorded immediately before generateContent is called, so it is visible even if the call hangs or times out (60 s client timeout)." },
  "guard.output.request": { sec: 7, title: "Output guardrail request (recorded before sending)", code: "Recorded immediately before generateContent is called. Output checks are never cached." },
  "guard.input.error": { sec: 2, title: "Input guardrail technical failure", code: "Released for retry; after 3 attempts the job FAILS (never treated as approved)." },
  "job.rejected": { sec: 2, title: "Job rejected by input check", code: "Job → REJECTED; reserved neurons released; nothing is queued." },
  "prompt.build": { sec: 3, title: "Prompt rendered from template", code: "prompt.ts: template lines with {field} placeholders; a line whose field is unknown is dropped (no invented defaults). Only fields verified for FLUX.2 are used later: prompt, input_image_0, width, height." },
  "outbox.write": { sec: 4, title: "Outbox written (1 generation task + per-image candidates)", code: "One atomic update of the job document: prompt, generationTaskId, tasks[] = one candidate per image (PENDING_PUBLISH) and stage QUEUED — this is the outbox." },
  "rabbit.publish": { sec: 5, title: "Generation task published → generation.tasks (1 message, imageCount)", code: "loops.ts publishOnce → rabbit.ts publishTask: ONE persistent + mandatory message per request with imageCount and candidateIds, on a ConfirmChannel; waits for the broker confirm before marking." },
  "outbox.mark": { sec: 4, title: "Candidates marked PUBLISHED", code: "All candidates of the task are marked only after the confirm; a crash before this re-publishes (the worker checkpoint prevents re-generation of stored images)." },
  "rabbit.consume.task": { sec: 5, title: "Worker consumed generation task", code: "worker.py handle: prefetch=1, manual ACK. The worker generates imageCount images (one FLUX call each — the API returns one image per call), then publishes ONE result message, waits for its confirm, and only then ACKs the task." },
  "image.settled": { sec: 6, title: "Image outcome carried from an earlier attempt", code: "UNKNOWN/FAILED outcomes travel in the retried message ('settled') so they are never sent to the provider again." },
  "storage.checkpoint": { sec: 6, title: "Checkpoint hit (image already stored)", code: "results/{candidateId}.png already exists → used as is, no new FLUX call (protects retries and re-deliveries)." },
  "storage.get": { sec: 6, title: "Reference image loaded from SeaweedFS", code: "The ≤512 PNG reference is read once per task from shared storage by key (no image bytes in the queue)." },
  "flux.call": { sec: 6, title: "FLUX call", code: "providers.py CloudflareFluxProvider.generate: REST multipart to Workers AI. Retries only when the request certainly did not execute; read timeout / dropped connection → UNKNOWN (no blind retry)." },
  "flux.error": { sec: 6, title: "Provider error", code: "Classified as RETRYABLE / UNKNOWN / QUOTA / FATAL in providers.py." },
  "worker.error": { sec: 6, title: "Worker error", code: "Storage/unexpected error; retried via retry queue up to 3 attempts." },
  "storage.put": { sec: 6, title: "Result stored in SeaweedFS", code: "Provider JPEG decoded and stored as PNG (worker.py to_png) under results/{taskId}.png, private and not yet approved." },
  "rabbit.retry": { sec: 5, title: "Retry scheduled (whole task)", code: "Only when an image failed in a way that certainly did not execute. The task is re-published to generation.tasks.retry (per-message TTL → back to generation.tasks); on the next attempt stored images are skipped by the checkpoint and UNKNOWN/FAILED outcomes are carried, so nothing stored is regenerated." },
  "rabbit.dlq": { sec: 5, title: "Copy sent to DLQ", code: "Images that still failed after the last attempt are reported FAILED in the result; a copy of the task goes to generation.tasks.dlq." },
  "rabbit.publish.result": { sec: 5, title: "One result published → generation.results (results list)", code: "worker.py publish with confirm_delivery: one message with an entry per image (GENERATED / FAILED / UNKNOWN); then ACK of the task." },
  "rabbit.consume.result": { sec: 5, title: "Node consumed result (all images)", code: "loops.ts onResult: bulkWrite with one idempotent update per candidate (only from PUBLISHED), stage → VALIDATING_OUTPUT, ACK immediately (not held during the per-image output checks)." },
  "rabbit.consume.result.error": { sec: 5, title: "Result persist failed", code: "nack + requeue." },
  "output.claim": { sec: 7, title: "Output check claimed (atomic + lease)", code: "loops.ts outputCheckOnce: per-task GENERATED → CHECKING with lease + claim token." },
  "guard.output": { sec: 7, title: "Output guardrail", code: "guardrails.ts checkOutput: separate system instruction; checks it is a ring, no forbidden content, no clear mismatch. Only APPROVED images are ever shown or downloadable." },
  "guard.output.error": { sec: 7, title: "Output guardrail technical failure", code: "Retried; after 3 failures the image is not approved (hidden)." },
  "job.finalize": { sec: 4, title: "Job finalized", code: "All tasks terminal → COMPLETED if ≥1 approved, else FAILED." },
  "refine.lock": { sec: 8, title: "Refine lock taken", code: "POST /api/jobs/:id/refine: atomic findOneAndUpdate only while refineJobId is null → exactly one refine per design." },
  "refine.link": { sec: 8, title: "Refine job linked", code: "The parent records which candidate was selected." },
  "refine.create": { sec: 8, title: "Refine job created", code: "index.ts ensureRefineJob: upsert with a pre-chosen id (crash-safe; recovery sweep re-runs it). Input = the selected approved image; then the same two queues and both guardrails run again with 1 task." },
  "poll.update": { sec: 9, title: "Polling response changed", code: "GET /api/jobs/:id every 3 s (paused when tab hidden). Redis limit 30/min per session. Recorded only when the response changes." },
  "poll.limited": { sec: 9, title: "Polling rate-limited (429)", code: "Over 30 requests/min → 429 + Retry-After." },
};

const IMAGE_PATHS = ["normalized.key", "fluxReference.key", "imageKey", "key", "selectedImageKey"];
const get = (o: any, p: string) => p.split(".").reduce((a, k) => (a == null ? a : a[k]), o);
const time = (s: string) => new Date(s).toLocaleTimeString([], { hour12: false }) + "." + String(new Date(s).getMilliseconds()).padStart(3, "0");
const pre: React.CSSProperties = { background: "#f6f8fa", padding: 8, overflow: "auto", maxHeight: 320, fontSize: 12, whiteSpace: "pre-wrap", wordBreak: "break-word" };

function Img({ jobId, k }: { jobId: string; k: string }) {
  const [err, setErr] = useState(false);
  if (err) return <div style={{ fontSize: 12, color: "#666" }}>{k}: not displayed (not an approved image of this job)</div>;
  return (
    <figure style={{ margin: 4, display: "inline-block" }}>
      <img src={`/api/debug/jobs/${jobId}/object?key=${encodeURIComponent(k)}`} onError={() => setErr(true)} style={{ width: 160, border: "1px solid #ccc" }} />
      <figcaption style={{ fontSize: 11 }}>{k}</figcaption>
    </figure>
  );
}

const box: React.CSSProperties = { ...pre, maxHeight: 400, background: "#fffdf3", border: "1px solid #eee" };
const Field = ({ label, children }: { label: string; children: React.ReactNode }) => (
  <div style={{ margin: "6px 0" }}>
    <div style={{ fontSize: 12, fontWeight: 600 }}>{label}</div>
    {children}
  </div>
);

/** Readable view of a Gemini guardrail request / response / cache hit. */
function GuardView({ e, jobId }: { e: Ev; jobId: string }) {
  const d = e.data;
  const isRequest = e.step.endsWith(".request");
  const req = d.request ?? {};
  const t = d.trace ?? {};
  const parts: any[] = req.contents?.[0]?.parts ?? [];
  const sentText = parts.find((p) => p.text)?.text ?? t.userText;
  const system = req.config?.systemInstruction ?? t.systemInstruction;
  if (d.cached) {
    return (
      <div>
        <Field label="Source"><b style={{ color: "#a60" }}>CACHE HIT — no request sent to Gemini</b> · cache id <code>{d.cacheId}</code></Field>
        <Field label="Stored result"><pre style={box}>{JSON.stringify(d.cachedRecord, null, 2)}</pre></Field>
        <Field label="Image this check refers to"><Img jobId={jobId} k={d.imageKey} /></Field>
        <Field label="Decision"><b>{d.decision}</b></Field>
      </div>
    );
  }
  if (req.mock || t.mock) {
    return <Field label="Source"><b style={{ color: "#a60" }}>MOCK guardrail — nothing sent to Gemini</b> <pre style={box}>{JSON.stringify(d, null, 2)}</pre></Field>;
  }
  return (
    <div>
      <Field label="Model">{req.model ?? t.model} {t.latencyMs != null && <span>· latency {t.latencyMs} ms</span>} {d.attempt && <span>· attempt {d.attempt}</span>}</Field>
      <Field label="System instruction (exact text sent)"><pre style={box}>{system ?? "not recorded"}</pre></Field>
      <Field label="Text part sent (exact)"><pre style={box}>{sentText ?? "not recorded"}</pre></Field>
      <Field label="Image part sent">{d.imageKey ? <Img jobId={jobId} k={d.imageKey} /> : "not recorded"} {t.imageBytes && <span style={{ fontSize: 12 }}>({t.imageBytes} bytes PNG, sent inline as base64 — omitted here)</span>}</Field>
      {isRequest ? (
        <>
          <Field label="Generation config"><pre style={box}>{JSON.stringify({ ...req.config, systemInstruction: "(shown above)" }, null, 2)}</pre></Field>
          <Field label="Status"><i>Request recorded before sending. The response appears as the next event of this check; if none follows, the call is still pending or timed out.</i></Field>
        </>
      ) : (
        <>
          <Field label="Raw response from Gemini (exact text)"><pre style={box}>{t.rawResponse ?? "no response (see error)"}</pre></Field>
          {d.parsed && <Field label="Parsed by the server"><pre style={box}>{JSON.stringify(d.parsed, null, 2)}</pre></Field>}
          {d.serverValidation && <Field label="Server validation rule">{d.serverValidation}</Field>}
          {d.error && <Field label="Error"><b style={{ color: "#b00020" }}>{d.error}</b></Field>}
          <Field label="Decision"><b>{d.decision}</b></Field>
        </>
      )}
    </div>
  );
}

export default function Debug({ jobId, onOpen }: { jobId: string; onOpen: (id: string) => void }) {
  const [d, setD] = useState<DebugResp | null>(null);
  const [err, setErr] = useState("");
  const [task, setTask] = useState("");
  const [list, setList] = useState<{ _id: string; kind: string; stage: string }[]>([]);

  const load = () => {
    fetch(`/api/debug/jobs/${jobId}`).then(async (r) => (r.ok ? setD(await r.json()) : setErr((await r.json()).error))).catch((e) => setErr(String(e)));
    fetch(`/api/debug/jobs`).then((r) => r.json()).then(setList).catch(() => {});
  };
  useEffect(load, [jobId]);
  if (err) return <div style={{ color: "#b00020" }}>Debug: {err}</div>;
  if (!d) return <div>Loading debug…</div>;

  const job = d.job;
  const tasks: string[] = (job.tasks ?? []).map((t: any) => t.taskId);
  // Job-level and generation-task events (one RabbitMQ task per job) are shown under every image filter.
  const evs = d.events.filter((e) => !task || !e.taskId || e.taskId === task || !tasks.includes(e.taskId));

  return (
    <div style={{ border: "2px dashed #888", padding: 12, marginTop: 16, fontSize: 14 }}>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
        <b>Debug (local dev only)</b>
        <select value={jobId} onChange={(e) => onOpen(e.target.value)}>
          {list.map((j) => <option key={j._id} value={j._id}>{j._id.slice(0, 8)} {j.kind} {j.stage}</option>)}
        </select>
        <button onClick={load}>Reload</button>
        {job.parentJobId && <button onClick={() => onOpen(job.parentJobId)}>Open parent design {job.parentJobId.slice(0, 8)}</button>}
        {job.refineJobId && <button onClick={() => onOpen(job.refineJobId)}>Open refine job {job.refineJobId.slice(0, 8)}</button>}
      </div>
      <p>
        jobId <code>{job._id}</code> · {job.kind} · <b>{job.stage}</b> · mock provider: {String(job.mock?.provider)} · mock guardrail: {String(job.mock?.guardrail)}
        <br /><i>{d.tracingNote}</i>
      </p>
      <p style={{ fontSize: 12 }}>
        <span style={{ background: "#e6f4ea", padding: "1px 4px" }}>MEASURED</span> = recorded at runtime when the step happened ·{" "}
        <span style={{ background: "#eef", padding: "1px 4px" }}>FROM CODE</span> = explanation of what the code does
      </p>

      <details>
        <summary>MongoDB Compass / RabbitMQ UI</summary>
        <div style={{ fontSize: 13 }}>
          Connection: <code>{d.compass.connection}</code> · DB <code>{d.compass.database}</code>
          <ul>{d.compass.queries.map((q, i) => <li key={i}><code>{q.collection}</code> filter <code>{q.filter}</code>{q.sort && <> sort <code>{q.sort}</code></>} {q.note && `— ${q.note}`}</li>)}</ul>
          RabbitMQ: {d.rabbitUi} (messages are not read from the queues for this view)
        </div>
      </details>
      <details>
        <summary>Current job document (MongoDB, live)</summary>
        <pre style={pre}>{JSON.stringify(job, null, 2)}</pre>
      </details>

      <div style={{ margin: "8px 0" }}>
        Filter by task:{" "}
        <select value={task} onChange={(e) => setTask(e.target.value)}>
          <option value="">all (job + every task)</option>
          {tasks.map((t, i) => {
            const st = job.tasks[i].status;
            return <option key={t} value={t}>#{i + 1} {t.slice(0, 8)} {st}</option>;
          })}
        </select>
      </div>

      <h4 style={{ margin: "8px 0" }}>Timeline ({evs.length} events)</h4>
      {evs.length === 0 && <div>Not recorded for this job.</div>}
      {evs.map((e, i) => {
        const info = STEP[e.step] ?? { sec: 0, title: e.step, code: "" };
        const images = [...new Set(IMAGE_PATHS.map((p) => get(e.data, p)).filter((v) => typeof v === "string" && /\.png$/.test(v)))] as string[];
        const tIdx = e.taskId ? tasks.indexOf(e.taskId) : -1;
        return (
          <details key={i} style={{ borderBottom: "1px solid #eee", padding: "4px 0" }}>
            <summary>
              <code>{time(e.ts)}</code> <span style={{ background: "#ddd", padding: "0 4px" }}>{info.sec}. {SECTIONS[info.sec] ?? "other"}</span>{" "}
              <b>{info.title}</b> <span style={{ color: "#666" }}>[{e.service}]</span>
              {e.taskId && (tIdx >= 0
                ? <span style={{ color: "#06c" }}> image #{tIdx + 1} {e.taskId.slice(0, 8)}</span>
                : <span style={{ color: "#850" }}> generation task {e.taskId.slice(0, 8)}</span>)}
              {e.data?.realCall === false && <span style={{ color: "#a60" }}> (cached / no real call)</span>}
              {e.data?.realCall === true && <span style={{ color: "#070" }}> (real call)</span>}
            </summary>
            <div style={{ paddingLeft: 12 }}>
              <div style={{ fontSize: 12 }}><code>{e.file}</code> → <code>{e.fn}</code></div>
              {info.code && <div style={{ background: "#eef", padding: 6, fontSize: 13, margin: "4px 0" }}><b>FROM CODE:</b> {info.code}</div>}
              <div style={{ background: "#e6f4ea", padding: "2px 6px", fontSize: 12, marginTop: 4 }}><b>MEASURED</b> at {e.ts}</div>
              {e.step.startsWith("guard.") ? (
                <>
                  <GuardView e={e} jobId={job._id} />
                  <details><summary style={{ fontSize: 12 }}>Full recorded event (JSON)</summary><pre style={pre}>{JSON.stringify(e.data, null, 2)}</pre></details>
                </>
              ) : (
                <>
                  {images.length > 0 && <div>{images.map((k) => <Img key={k} jobId={job._id} k={k} />)}</div>}
                  <pre style={pre}>{JSON.stringify(e.data, null, 2)}</pre>
                </>
              )}
              {e.mongo && (
                <>
                  <div style={{ fontSize: 12 }}><b>MongoDB change</b> ({e.mongo.collection} · {e.mongo.op})</div>
                  <pre style={pre}>{JSON.stringify({ filter: e.mongo.filter, set: e.mongo.fields }, null, 2)}</pre>
                </>
              )}
            </div>
          </details>
        );
      })}
    </div>
  );
}
