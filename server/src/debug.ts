import { Collection, Db } from "mongodb";

/** Dev-only runtime trace of a job. Events are written at the moment each step happens
 *  (never reconstructed afterwards) into the `job_events` collection. */
export interface JobEvent {
  jobId: string;
  taskId?: string;
  ts: Date;
  step: string; // e.g. "guard.input", "rabbit.publish"
  service: "node" | "worker";
  file: string;
  fn: string;
  data: Record<string, unknown>;
  mongo?: { collection: string; op: string; filter: Record<string, unknown>; fields: Record<string, unknown> };
}

export const debugEnabled = () => (process.env.DEBUG_UI ?? "1") === "1" && process.env.NODE_ENV !== "production";

let events: Collection<JobEvent> | null = null;
export async function initDebug(db: Db) {
  events = db.collection<JobEvent>("job_events");
  await events.createIndex({ jobId: 1, ts: 1 });
}

const B64ISH = /^[A-Za-z0-9+/=_-]{300,}$/;
/** Strip secrets/large blobs: long base64 → placeholder, long strings truncated. */
export function sanitize(v: unknown, depth = 0): unknown {
  if (depth > 8) return "[…]";
  if (typeof v === "string") {
    if (B64ISH.test(v)) return `[base64 omitted, ${v.length} chars]`;
    return v.length > 4000 ? v.slice(0, 4000) + `… [truncated, ${v.length} chars]` : v;
  }
  if (Buffer.isBuffer(v)) return `[binary ${v.length} bytes]`;
  if (Array.isArray(v)) return v.slice(0, 50).map((x) => sanitize(x, depth + 1));
  if (v && typeof v === "object" && !(v instanceof Date)) {
    const out: Record<string, unknown> = {};
    for (const [k, x] of Object.entries(v)) {
      if (/authorization|api[_-]?key|token|cookie|secret|password/i.test(k)) out[k] = "[redacted]";
      else out[k] = sanitize(x, depth + 1);
    }
    return out;
  }
  return v;
}

/** Fire-and-forget: tracing must never change or block processing. */
export function record(e: Omit<JobEvent, "ts" | "service"> & { service?: JobEvent["service"]; ts?: Date }) {
  if (!events || !debugEnabled()) return;
  const doc = { ...e, service: e.service ?? "node", ts: e.ts ?? new Date(), data: sanitize(e.data) as Record<string, unknown> } as JobEvent;
  if (doc.mongo) doc.mongo = sanitize(doc.mongo) as JobEvent["mongo"];
  events.insertOne(doc).catch((err) => console.error("[debug] record failed:", err.message));
}

export async function eventsFor(jobId: string) {
  return events ? events.find({ jobId }, { projection: { _id: 0 } }).sort({ ts: 1 }).toArray() : [];
}

/** Rough mask for identifiers derived from the session cookie. */
export const mask = (s?: string) => (s ? `${s.slice(0, 8)}…` : s);
