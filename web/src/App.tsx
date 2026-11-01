import { useEffect, useRef, useState } from "react";
import Debug from "./Debug";

type Candidate = { candidateId: string; url: string };
type JobView = {
  jobId: string;
  kind: "GENERATE" | "REFINE";
  stage: string;
  terminal: boolean;
  message: string;
  error?: string;
  errorCode?: "CONTENT_REJECTED" | "CONTENT_CHECK_UNAVAILABLE" | "CONTENT_CHECK_MISCONFIGURED";
  mock: { provider: boolean; guardrail: boolean };
  parentJobId?: string;
  refineJobId: string | null;
  counts: { total: number; approved: number; rejected: number; failed: number };
  candidates: Candidate[];
};

const LS_KEY = "s2r.jobId";
const lsGet = () => { try { return localStorage.getItem(LS_KEY); } catch { return null; } };
const lsSet = (v: string) => { try { localStorage.setItem(LS_KEY, v); } catch { /* ignore */ } };

async function api<T>(url: string, init?: RequestInit): Promise<T> {
  const r = await fetch(url, { credentials: "same-origin", ...init });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) {
    const ra = r.headers.get("Retry-After");
    throw new Error((body.error || `HTTP ${r.status}`) + (ra ? ` (retry in ${ra}s)` : ""));
  }
  return body as T;
}

export default function App() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const drawing = useRef(false);
  const idemKey = useRef(crypto.randomUUID());
  const [file, setFile] = useState<File | null>(null);
  const [desc, setDesc] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [jobId, setJobId] = useState<string | null>(lsGet());
  const [job, setJob] = useState<JobView | null>(null);
  const [parent, setParent] = useState<JobView | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [debugId, setDebugId] = useState<string | null>(null);

  // ---------- canvas ----------
  const clearCanvas = () => {
    const c = canvasRef.current!;
    const ctx = c.getContext("2d")!;
    ctx.fillStyle = "#fff";
    ctx.fillRect(0, 0, c.width, c.height);
  };
  useEffect(clearCanvas, []);
  // Establish the server session on load; restore the latest job if the browser lost it.
  useEffect(() => {
    api<{ jobId: string | null }>("/api/session/latest")
      .then((r) => { if (!lsGet() && r.jobId) { lsSet(r.jobId); setJobId(r.jobId); } })
      .catch(() => {});
  }, []);
  const pos = (e: React.PointerEvent) => {
    const r = canvasRef.current!.getBoundingClientRect();
    return [((e.clientX - r.left) * 512) / r.width, ((e.clientY - r.top) * 512) / r.height];
  };
  const down = (e: React.PointerEvent) => {
    drawing.current = true;
    const ctx = canvasRef.current!.getContext("2d")!;
    const [x, y] = pos(e);
    ctx.beginPath();
    ctx.moveTo(x, y);
    newDraft();
  };
  const move = (e: React.PointerEvent) => {
    if (!drawing.current) return;
    const ctx = canvasRef.current!.getContext("2d")!;
    ctx.lineWidth = 4;
    ctx.lineCap = "round";
    ctx.strokeStyle = "#111";
    const [x, y] = pos(e);
    ctx.lineTo(x, y);
    ctx.stroke();
  };
  // Any edit makes a new draft => new Idempotency-Key. Double clicks on one draft share the key.
  const newDraft = () => { idemKey.current = crypto.randomUUID(); };

  // ---------- polling (3s, paused when tab hidden, stops at terminal state) ----------
  useEffect(() => {
    if (!jobId) return;
    let timer: number | undefined;
    let stopped = false;
    const tick = async () => {
      if (stopped) return;
      if (document.hidden) return; // resumed by visibilitychange
      try {
        const j = await api<JobView>(`/api/jobs/${jobId}`);
        setJob(j);
        if (j.parentJobId && (!parent || parent.jobId !== j.parentJobId)) {
          setParent(await api<JobView>(`/api/jobs/${j.parentJobId}`));
        }
        if (j.terminal) return;
      } catch (e) {
        setError((e as Error).message);
      }
      timer = window.setTimeout(tick, 3000);
    };
    const onVis = () => { if (!document.hidden) { clearTimeout(timer); tick(); } };
    document.addEventListener("visibilitychange", onVis);
    tick();
    return () => { stopped = true; clearTimeout(timer); document.removeEventListener("visibilitychange", onVis); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId]);

  const track = (id: string) => { lsSet(id); setJobId(id); setJob(null); setSelected(null); };

  // ---------- actions ----------
  const generate = async () => {
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      const blob: Blob =
        file ?? (await new Promise<Blob>((res) => canvasRef.current!.toBlob((b) => res(b!), "image/png")));
      const fd = new FormData();
      fd.append("image", blob, file?.name ?? "sketch.png");
      fd.append("description", desc);
      const r = await api<{ jobId: string }>("/api/jobs", {
        method: "POST",
        body: fd,
        headers: { "Idempotency-Key": idemKey.current },
      });
      setParent(null);
      track(r.jobId);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const refine = async () => {
    if (!job || !selected || busy) return;
    setBusy(true);
    setError("");
    try {
      const r = await api<{ jobId: string }>(`/api/jobs/${job.jobId}/refine`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ candidateId: selected }),
      });
      setParent(job);
      track(r.jobId);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const canRefine = job?.kind === "GENERATE" && job.stage === "COMPLETED" && !job.refineJobId;
  const active = job && !job.terminal;

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", maxWidth: 1100, margin: "0 auto", padding: 16 }}>
      <h1 style={{ margin: "8px 0" }}>Sketch → Ring</h1>
      {job && (job.mock.provider || job.mock.guardrail) && (
        <div style={{ background: "#fff3cd", padding: 8, borderRadius: 6, marginBottom: 8 }}>
          MOCK mode: {job.mock.provider && "image generation is FAKE (no FLUX call). "}
          {job.mock.guardrail && "content checks are a keyword MOCK (no Gemini call)."}
        </div>
      )}
      <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
        <div>
          <canvas
            ref={canvasRef}
            width={512}
            height={512}
            style={{ width: 360, height: 360, border: "1px solid #999", touchAction: "none", cursor: "crosshair", opacity: file ? 0.3 : 1 }}
            onPointerDown={down}
            onPointerMove={move}
            onPointerUp={() => (drawing.current = false)}
            onPointerLeave={() => (drawing.current = false)}
          />
          <div style={{ display: "flex", gap: 8, marginTop: 6, alignItems: "center" }}>
            <button onClick={() => { clearCanvas(); setFile(null); newDraft(); }}>Clear</button>
            <input
              type="file"
              accept="image/png,image/jpeg"
              onChange={(e) => { setFile(e.target.files?.[0] ?? null); newDraft(); }}
            />
          </div>
          <textarea
            value={desc}
            maxLength={1000}
            onChange={(e) => { setDesc(e.target.value); newDraft(); }}
            placeholder="Describe the ring, e.g. rose gold solitaire with an oval diamond"
            rows={3}
            style={{ width: 360, marginTop: 6, display: "block" }}
          />
          <button onClick={generate} disabled={busy || !!active} style={{ marginTop: 6, padding: "8px 16px" }}>
            {busy ? "Sending…" : "Generate"}
          </button>
        </div>

        <div style={{ flex: 1, minWidth: 300 }}>
          {error && <div style={{ color: "#b00020", marginBottom: 8 }}>{error}</div>}
          {job && (
            <div style={{ marginBottom: 8 }}>
              <b>{job.kind === "REFINE" ? "Refinement" : "Design"}:</b> {job.stage} — {job.message}{" "}
              {import.meta.env.DEV && (
                <button onClick={() => setDebugId(debugId ? null : job.jobId)} style={{ fontSize: 12 }}>
                  {debugId ? "Hide debug" : "Debug"}
                </button>
              )}
              {job.counts.total > 0 && (
                <div>
                  {job.counts.approved} of {job.counts.total} approved
                  {!job.terminal && `, ${job.counts.total - job.counts.approved - job.counts.rejected - job.counts.failed} still in progress (images appear one by one)`}
                </div>
              )}
              {job.error && (job.errorCode === "CONTENT_CHECK_UNAVAILABLE" || job.errorCode === "CONTENT_CHECK_MISCONFIGURED" ? (
                // Technical problem, not a rejection of the user's content.
                <div style={{ background: "#fff3cd", border: "1px solid #e0b400", padding: 8, borderRadius: 6, marginTop: 4 }}>
                  <b>Content check unavailable</b> — your design was <b>not</b> rejected. {job.error}
                </div>
              ) : (
                <div style={{ color: "#b00020" }}>{job.errorCode === "CONTENT_REJECTED" && <b>Rejected: </b>}{job.error}</div>
              ))}
            </div>
          )}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 8 }}>
            {job?.candidates.map((c) => (
              <div key={c.candidateId}>
                <img
                  src={c.url}
                  onClick={() => setSelected(c.candidateId)}
                  style={{ width: "100%", cursor: "pointer", border: selected === c.candidateId ? "3px solid #0a6" : "3px solid transparent" }}
                />
                <a href={`${c.url}?download=1`}>Download</a>
              </div>
            ))}
          </div>
          {canRefine && (
            <button onClick={refine} disabled={!selected || busy} style={{ marginTop: 8, padding: "8px 16px" }}>
              Refine selected (one round)
            </button>
          )}
          {job?.kind === "REFINE" && parent && (
            <div style={{ marginTop: 16 }}>
              <div>Original approved images remain available:</div>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                {parent.candidates.map((c) => (
                  <a key={c.candidateId} href={`${c.url}?download=1`}>
                    <img src={c.url} style={{ width: 90 }} />
                  </a>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
      {import.meta.env.DEV && debugId && <Debug jobId={debugId} onOpen={setDebugId} />}
    </div>
  );
}
