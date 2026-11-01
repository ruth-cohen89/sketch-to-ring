import amqp, { ConfirmChannel, Channel, ConsumeMessage } from "amqplib";
import { config, QUEUES } from "./config.js";

let conn: Awaited<ReturnType<typeof amqp.connect>> | null = null;
let pubCh: ConfirmChannel | null = null;
let consumeCh: Channel | null = null;
const returned = new Set<string>();

export async function assertTopology(ch: Channel) {
  await ch.assertQueue(QUEUES.tasks, { durable: true });
  await ch.assertQueue(QUEUES.results, { durable: true });
  await ch.assertQueue(QUEUES.dlq, { durable: true });
  // Retry queue: messages wait (per-message expiration) then dead-letter back into generation.tasks.
  await ch.assertQueue(QUEUES.retry, {
    durable: true,
    deadLetterExchange: "",
    deadLetterRoutingKey: QUEUES.tasks,
  });
}

export const rabbitReady = () => !!pubCh;

export async function connectRabbit(onResult: (msg: ConsumeMessage, ch: Channel) => Promise<void>) {
  try {
    conn = await amqp.connect(config.amqpUrl);
    conn.on("close", () => {
      console.error("[rabbit] connection closed, reconnecting in 3s");
      conn = null; pubCh = null; consumeCh = null;
      setTimeout(() => connectRabbit(onResult), 3000);
    });
    conn.on("error", (e) => console.error("[rabbit] error", e.message));
    pubCh = await conn.createConfirmChannel();
    await assertTopology(pubCh);
    pubCh.on("return", (m) => {
      const id = m.properties.messageId;
      if (id) returned.add(id);
      console.error(`[rabbit] message ${id} unroutable`);
    });
    consumeCh = await conn.createChannel();
    await consumeCh.prefetch(10);
    const ch = consumeCh;
    await ch.consume(QUEUES.results, (m) => m && onResult(m, ch), { noAck: false });
    console.log("[rabbit] connected; consuming", QUEUES.results);
  } catch (e) {
    console.error("[rabbit] connect failed:", (e as Error).message, "- retry in 3s");
    pubCh = null;
    setTimeout(() => connectRabbit(onResult), 3000);
  }
}

/** Publish persistent+mandatory and wait for broker confirm. Returns false if unroutable. */
export async function publishTask(payload: object, messageId: string): Promise<boolean> {
  const ch = pubCh;
  if (!ch) throw new Error("rabbit not connected");
  await new Promise<void>((resolve, reject) => {
    ch.publish("", QUEUES.tasks, Buffer.from(JSON.stringify(payload)), {
      persistent: true,
      mandatory: true,
      contentType: "application/json",
      messageId,
    }, (err) => (err ? reject(err) : resolve()));
  });
  // 'return' is emitted before the confirm for unroutable mandatory messages.
  if (returned.delete(messageId)) return false;
  return true;
}
