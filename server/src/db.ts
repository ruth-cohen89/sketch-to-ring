import { MongoClient, Db, Collection } from "mongodb";
import { config } from "./config.js";

export type Stage =
  | "PENDING_INPUT_CHECK"
  | "CHECKING_INPUT"
  | "QUEUED"
  | "GENERATING"
  | "VALIDATING_OUTPUT"
  | "COMPLETED"
  | "FAILED"
  | "REJECTED";

export type TaskStatus =
  | "PENDING_PUBLISH"
  | "PUBLISHED"
  | "GENERATED"
  | "CHECKING"
  | "APPROVED"
  | "REJECTED"
  | "FAILED"
  | "UNKNOWN";

export const TERMINAL_TASK: TaskStatus[] = ["APPROVED", "REJECTED", "FAILED", "UNKNOWN"];
export const TERMINAL_STAGE: Stage[] = ["COMPLETED", "FAILED", "REJECTED"];

export interface Task {
  taskId: string;
  status: TaskStatus;
  attempts: number;
  resultKey?: string;
  leaseUntil?: Date;
  checkReason?: string;
  error?: string;
  publishedAt?: Date;
  updatedAt: Date;
}

export interface Job {
  _id: string;
  sessionId: string;
  kind: "GENERATE" | "REFINE";
  parentJobId?: string;
  parentCandidateId?: string;
  idempotencyKey?: string;
  stage: Stage;
  inputKey: string;
  inputHash: string;
  inputWidth: number;
  inputHeight: number;
  description: string;
  design?: Record<string, string | undefined>;
  prompt?: string;
  templateVersion?: string;
  modelId?: string;
  options?: Record<string, unknown>;
  mock: { provider: boolean; guardrail: boolean };
  refineJobId?: string | null;
  /** One RabbitMQ generation task per job; tasks[] below are the per-image candidates it produces. */
  generationTaskId?: string;
  inputCheck?: { verdict: string; reason?: string; cached?: boolean };
  leaseUntil?: Date;
  error?: string;
  tasks: Task[];
  createdAt: Date;
  updatedAt: Date;
}

export interface GuardCheck {
  _id: string; // `${kind}:${hash}:${policyVersion}`
  verdict: string;
  reason?: string;
  design?: Record<string, string | undefined>;
  createdAt: Date;
}

let db: Db;
export let jobs: Collection<Job>;
export let sessions: Collection<{ _id: string; createdAt: Date }>;
export let guardChecks: Collection<GuardCheck>;

export async function connectDb() {
  const client = await MongoClient.connect(config.mongoUrl, { serverSelectionTimeoutMS: 5000 });
  db = client.db();
  jobs = db.collection<Job>("jobs");
  sessions = db.collection("sessions");
  guardChecks = db.collection<GuardCheck>("guard_checks");
  await jobs.createIndex(
    { sessionId: 1, idempotencyKey: 1 },
    { unique: true, partialFilterExpression: { idempotencyKey: { $type: "string" } } }
  );
  await jobs.createIndex({ stage: 1 });
  await jobs.createIndex({ "tasks.taskId": 1 });
  await jobs.createIndex({ "tasks.status": 1 });
  return client;
}
