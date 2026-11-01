// End-to-end smoke test (Node 22, no deps). Usage: node scripts/smoke.mjs [baseUrl]
// Runs against whatever providers the server/worker are configured with (default: MOCKS).
import zlib from "node:zlib";
import crypto from "node:crypto";

const BASE = process.argv[2] || "http://127.0.0.1:3001";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let failures = 0;
const check = (cond, label) => {
  console.log(`${cond ? "PASS" : "FAIL"}  ${label}`);
  if (!cond) failures++;
};

// ---- tiny PNG encoder: white canvas with a black ring sketch ----
function sketchPng(size = 256) {
  const raw = Buffer.alloc((size * 3 + 1) * size, 255);
  for (let y = 0; y < size; y++) {
    raw[y * (size * 3 + 1)] = 0;
    for (let x = 0; x < size; x++) {
      const d = Math.hypot(x - size / 2, (y - size / 2) * 1.6);
      if (Math.abs(d - size / 3) < 3 || Math.hypot(x - size / 2, y - size / 5) < 14) {
        const o = y * (size * 3 + 1) + 1 + x * 3;
        raw[o] = raw[o + 1] = raw[o + 2] = 0;
      }
    }
  }
  const crcTable = Array.from({ length: 256 }, (_, n) => {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    return c >>> 0;
  });
  const crc = (b) => { let c = 0xffffffff; for (const x of b) c = crcTable[(c ^ x) & 255] ^ (c >>> 8); return (c ^ 0xffffffff) >>> 0; };
  const chunk = (type, data) => {
    const len = Buffer.alloc(4); len.writeUInt32BE(data.length);
    const td = Buffer.concat([Buffer.from(type), data]);
    const c = Buffer.alloc(4); c.writeUInt32BE(crc(td));
    return Buffer.concat([len, td, c]);
  };
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(size, 0); ihdr.writeUInt32BE(size, 4); ihdr[8] = 8; ihdr[9] = 2;
  return Buffer.concat([
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk("IHDR", ihdr), chunk("IDAT", zlib.deflateSync(raw)), chunk("IEND", Buffer.alloc(0)),
  ]);
}

// ---- client with its own cookie jar (= its own session) ----
function client() {
  let cookie = "";
  return async (path, init = {}) => {
    const r = await fetch(BASE + path, { ...init, headers: { ...(init.headers || {}), cookie } });
    const sc = r.headers.getSetCookie?.() ?? [];
    if (sc.length) cookie = sc.map((c) => c.split(";")[0]).join("; ");
    return r;
  };
}

const PNG = sketchPng();
const createJob = (c, description, key = crypto.randomUUID(), img = PNG, type = "image/png") => {
  const fd = new FormData();
  fd.append("image", new Blob([img], { type }), "sketch.png");
  fd.append("description", description);
  return c("/api/jobs", { method: "POST", body: fd, headers: { "Idempotency-Key": key } });
};

async function poll(c, jobId, maxMs = 240_000) {
  const t0 = Date.now();
  let last = "";
  while (Date.now() - t0 < maxMs) {
    const r = await c(`/api/jobs/${jobId}`);
    const j = await r.json();
    if (!r.ok) throw new Error(`poll ${jobId}: HTTP ${r.status} ${j.error}`);
    if (j.stage !== last) { console.log(`      ${jobId.slice(0, 8)} → ${j.stage} ${j.message ?? ""}`); last = j.stage; }
    if (j.terminal) return j;
    await sleep(2500);
  }
  throw new Error("poll timeout");
}

const health = await (await fetch(BASE + "/health")).json();
console.log("health:", JSON.stringify(health));
check(health.ok, "all services up (mongo, redis, rabbit)");

// 1) Full flow + double click
const A = client();
await A("/api/session/latest"); // establish session cookie first (as the web app does on load)
const key = crypto.randomUUID();
const [r1, r2] = await Promise.all([
  createJob(A, "rose gold solitaire ring with an oval diamond", key),
  createJob(A, "rose gold solitaire ring with an oval diamond", key),
]);
const j1 = await r1.json(), j2 = await r2.json();
check(r1.status === 202 && r2.status === 202 && j1.jobId === j2.jobId, "double click (same Idempotency-Key) → same jobId");
const r3 = await createJob(A, "rose gold solitaire ring with an oval diamond", key);
check((await r3.json()).jobId === j1.jobId, "repeat after creation returns same jobId (before active-job gate)");

const gen = await poll(A, j1.jobId);
check(gen.stage === "COMPLETED" && gen.candidates.length >= 1 && gen.candidates.length <= 6,
  `generation round COMPLETED via both queues (${gen.candidates.length} approved candidates)`);

const cand = gen.candidates[0].candidateId;
const refBody = (cid) => ({ method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ candidateId: cid }) });
const [f1, f2] = await Promise.all([A(`/api/jobs/${j1.jobId}/refine`, refBody(cand)), A(`/api/jobs/${j1.jobId}/refine`, refBody(cand))]);
const fj1 = await f1.json(), fj2 = await f2.json();
check(fj1.jobId && fj1.jobId === fj2.jobId, "double refine click → single refine job");
if (gen.candidates[1]) {
  const f3 = await A(`/api/jobs/${j1.jobId}/refine`, refBody(gen.candidates[1].candidateId));
  check(f3.status === 409, "second refine with another candidate → 409");
}
const ref = await poll(A, fj1.jobId);
check(ref.stage === "COMPLETED" && ref.candidates.length === 1, "refine round COMPLETED (1 approved image)");
const f4 = await A(`/api/jobs/${fj1.jobId}/refine`, refBody(ref.candidates[0].candidateId));
check(f4.status === 400, "refining a refinement is blocked");
const dl = await A(`${ref.candidates[0].url}?download=1`);
const dlBuf = Buffer.from(await dl.arrayBuffer());
check(dl.status === 200 && dl.headers.get("content-type") === "image/png" && dlBuf.subarray(1, 4).toString() === "PNG",
  `download final PNG (${dlBuf.length} bytes)`);
const orig = await A(`${gen.candidates[0].url}?download=1`);
check(orig.status === 200, "original approved image still downloadable after refine");

// 2) Forbidden input + ownership isolation
const B = client();
const bad = await (await createJob(B, "a ring shaped like a gun with blood on it")).json();
const badJob = await poll(B, bad.jobId);
check(badJob.stage === "REJECTED" && badJob.candidates.length === 0, "forbidden input REJECTED, no images exposed");
check((await B(`/api/jobs/${j1.jobId}`)).status === 404, "other session cannot read job (ownership)");
check((await B(`${gen.candidates[0].url}`)).status === 404, "other session cannot fetch image (ownership)");
const notImg = await createJob(B, "ring", crypto.randomUUID(), Buffer.from("<svg></svg>"), "image/svg+xml");
check(notImg.status === 400, "non PNG/JPEG upload → 400");

// 3) Rate limit: 3 new designs / 10 min per session
const C = client();
const statuses = [];
let retryAfter = null;
for (let i = 0; i < 4; i++) {
  const r = await createJob(C, "simple silver band ring");
  statuses.push(r.status);
  if (r.status === 429) retryAfter = r.headers.get("retry-after");
}
console.log("      create statuses:", statuses.join(","));
check(statuses[3] === 429 && !!retryAfter, `4th create in 10 min → 429 with Retry-After=${retryAfter}`);

console.log(failures ? `\n${failures} check(s) FAILED` : "\nALL CHECKS PASSED");
process.exit(failures ? 1 : 0);
