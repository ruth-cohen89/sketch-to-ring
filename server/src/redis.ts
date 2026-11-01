import { Redis } from "ioredis";
import { RateLimiterRedis, RateLimiterMemory, RateLimiterRes } from "rate-limiter-flexible";
import { config } from "./config.js";

export const redis = new Redis(config.redisUrl, {
  enableOfflineQueue: false, // fail fast when Redis is down
  maxRetriesPerRequest: 1,
});
redis.on("error", (e) => console.error("[redis]", e.message));

export const redisReady = () => redis.status === "ready";

const mk = (keyPrefix: string, points: number, duration: number, insurance = false) =>
  new RateLimiterRedis({
    storeClient: redis,
    keyPrefix,
    points,
    duration,
    insuranceLimiter: insurance ? new RateLimiterMemory({ keyPrefix, points, duration }) : undefined,
  });

export const createShort = mk("rl:create10m", 3, 600);
export const createDaily = mk("rl:create24h", 10, 86400);
// Polling/read endpoints fall back to an in-process limiter when Redis is down.
export const pollLimiter = mk("rl:poll", 30, 60, true);

export type LimitResult =
  | { ok: true; remaining: number; resetInSec: number }
  | { ok: false; status: 429 | 503; retryAfter?: number; message: string };

export async function consume(limiter: RateLimiterRedis, key: string): Promise<LimitResult> {
  try {
    const r = await limiter.consume(key);
    return { ok: true, remaining: r.remainingPoints, resetInSec: Math.ceil(r.msBeforeNext / 1000) };
  } catch (e) {
    if (e instanceof RateLimiterRes) {
      return { ok: false, status: 429, retryAfter: Math.ceil(e.msBeforeNext / 1000), message: "Rate limit exceeded" };
    }
    return { ok: false, status: 503, message: "Rate limiter unavailable (Redis down) - new designs are blocked" };
  }
}

// ---- Daily neuron budget (estimated, FREE_ONLY guard) ----
const dayKey = () => `neurons:${new Date().toISOString().slice(0, 10)}`;

export async function reserveNeurons(amount: number): Promise<boolean> {
  const key = dayKey();
  const total = Number(await redis.incrbyfloat(key, amount));
  await redis.expire(key, 2 * 86400);
  if (total > config.dailyNeuronBudget) {
    await redis.incrbyfloat(key, -amount);
    return false;
  }
  return true;
}
export async function releaseNeurons(amount: number) {
  await redis.incrbyfloat(dayKey(), -amount).catch(() => {});
}
export async function neuronsUsedToday() {
  return Number((await redis.get(dayKey()).catch(() => null)) ?? 0);
}
