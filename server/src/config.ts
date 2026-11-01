import dotenv from "dotenv";
import path from "node:path";

dotenv.config({ path: path.resolve(process.cwd(), "../.env") });
dotenv.config();

const env = (k: string, d = "") => process.env[k] ?? d;

export const config = {
  port: Number(env("PORT", "3001")),
  cookieSecret: env("COOKIE_SECRET", "dev-secret"),
  mongoUrl: env("MONGO_URL", "mongodb://127.0.0.1:27018/sketch2ring"),
  redisUrl: env("REDIS_URL", "redis://127.0.0.1:6380"),
  amqpUrl: env("AMQP_URL", "amqp://guest:guest@127.0.0.1:5672"),
  s3: {
    endpoint: env("S3_ENDPOINT", "http://127.0.0.1:8333"),
    accessKey: env("S3_ACCESS_KEY"),
    secretKey: env("S3_SECRET_KEY"),
    bucket: env("S3_BUCKET", "sketch2ring"),
  },
  provider: env("PROVIDER", "fake"),
  guardrail: env("GUARDRAIL", "mock"),
  geminiKey: env("GEMINI_API_KEY"),
  geminiModel: env("GEMINI_MODEL", "gemini-3.1-flash-lite"),
  // Used only when the primary model fails transiently (503/429/5xx/timeout/malformed answer). Free tier as well.
  // ("gemini-2.5-flash-lite" is no longer available to new users: 404.)
  geminiFallbackModel: env("GEMINI_FALLBACK_MODEL", "gemini-3.5-flash-lite"),
  cfModel: env("CF_MODEL", "@cf/black-forest-labs/flux-2-klein-4b"),
  dailyNeuronBudget: Number(env("DAILY_NEURON_BUDGET", "5000")),
  genWidth: Number(env("GEN_WIDTH", "1024")),
  genHeight: Number(env("GEN_HEIGHT", "1024")),
  // Input image sent to FLUX as reference is downscaled to fit this box.
  refMax: 512,
  variations: Math.min(6, Math.max(1, Number(env("VARIATIONS", "6")))),
  maxPendingTasks: 30,
  policyVersion: "policy-v1",
  templateVersion: "ring-v1",
};

export const QUEUES = {
  tasks: "generation.tasks",
  results: "generation.results",
  retry: "generation.tasks.retry",
  dlq: "generation.tasks.dlq",
};

// Cloudflare pricing (checked 2026-10-20): 512x512 tile in = 5.37, out = 26.05 neurons.
export function estimateNeurons(inW: number, inH: number, outW: number, outH: number) {
  const tiles = (w: number, h: number) => Math.ceil(w / 512) * Math.ceil(h / 512);
  return tiles(inW, inH) * 5.37 + tiles(outW, outH) * 26.05;
}
