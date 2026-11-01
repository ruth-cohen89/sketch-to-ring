"""Generation worker: consumes generation.tasks, calls the ImageProvider, stores the image in
shared storage (SeaweedFS S3), publishes to generation.results with publisher confirms, then ACKs.
It never writes to Mongo."""
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pika
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from PIL import Image

from providers import ProviderError, make_provider

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(message)s")
log = logging.getLogger()
logging.getLogger("pika").setLevel(logging.WARNING)

TASKS, RESULTS = "generation.tasks", "generation.results"
RETRY, DLQ = "generation.tasks.retry", "generation.tasks.dlq"
MAX_ATTEMPTS = 3
BUCKET = os.getenv("S3_BUCKET", "sketch2ring")

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT", "http://127.0.0.1:8333"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)
provider = make_provider()


def exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError:
        return False


def to_png(data: bytes) -> bytes:
    out = io.BytesIO()
    Image.open(io.BytesIO(data)).convert("RGB").save(out, "PNG")
    return out.getvalue()


def publish(ch, queue: str, body: dict, expiration_ms: int | None = None):
    # confirm_delivery() is on: basic_publish blocks until broker ACK, raises on nack/unroutable.
    ch.basic_publish(
        exchange="",
        routing_key=queue,
        body=json.dumps(body).encode(),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=body.get("taskId"),
            expiration=str(expiration_ms) if expiration_ms else None,
        ),
        mandatory=True,
    )


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# Structured metrics for the retry policy: one JSON line per provider call / retry / task result,
# appended to a file that survives restarts (stdout logs do not). Summarize with scripts/retry_stats.py.
METRICS_PATH = Path(__file__).resolve().parent / "logs" / "generation-metrics.jsonl"


def metric(event: str, **fields):
    try:
        METRICS_PATH.parent.mkdir(exist_ok=True)
        with METRICS_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": now_iso(), "event": event, "provider": provider.name, **fields}) + "\n")
    except Exception as e:  # metrics must never affect processing
        log.warning(f"metric write failed: {e}")


def handle(ch, method, props, raw: bytes):
    """One task = one user request with imageCount images (candidateIds). FLUX returns one image per call,
    so the worker loops. Each image is stored under results/{candidateId}.png (checkpoint), then ONE result
    message with a results list is published (confirmed) and only then the task is ACKed."""
    received_at = now_iso()
    try:
        task = json.loads(raw)
        task_id, job_id = task["taskId"], task["jobId"]
    except Exception:
        log.error("malformed task -> DLQ")
        ch.basic_publish("", DLQ, raw, pika.BasicProperties(delivery_mode=2))
        ch.basic_ack(method.delivery_tag)
        return
    attempt = int(task.get("attempt", 1))
    candidate_ids = list(task.get("candidateIds") or [task_id])
    n = len(candidate_ids)
    tag = f"job={job_id} task={task_id} attempt={attempt}"
    # Outcomes decided in an earlier attempt that must NOT be re-sent to the provider (UNKNOWN / FAILED).
    settled = dict(task.get("settled") or {})

    # Debug trace: travels in the messages (the worker never writes to Mongo); Node records it.
    trace = list(task.get("trace") or [])
    task_msg = {k: v for k, v in task.items() if k not in ("trace", "settled")}

    def ev(step, fn, data, cid=None):
        e = {"ts": now_iso(), "step": step, "file": "worker.py", "fn": fn, "data": data}
        if cid:
            e["candidateId"] = cid
        trace.append(e)

    ev("rabbit.consume.task", "handle", {
        "queue": method.routing_key, "publisher": "Node (outbox publisher)" if attempt == 1 else "worker (retry queue → dead-letter back)",
        "consumer": f"Python worker pid={os.getpid()}", "receivedAt": received_at, "redelivered": method.redelivered,
        "deliveryTag": method.delivery_tag, "prefetch": 1, "ackMode": "manual", "imageCount": n,
        "settledFromEarlierAttempts": settled, "message": task_msg,
    })

    results: dict[str, dict] = {}
    ref = None
    retry_reason = None  # set when an image failed in a way that is safe to retry (request certainly not executed)
    quota_hit = False
    retryable_exhausted = []

    for i, cid in enumerate(candidate_ids, 1):
        key = f"results/{cid}.png"
        pos = f"{i}/{n}"
        if cid in settled:
            results[cid] = {"candidateId": cid, **settled[cid]}
            ev("image.settled", "handle", {"image": pos, "status": settled[cid]["status"], "note": "decided in an earlier attempt; not sent to the provider again"}, cid)
            continue
        # Checkpoint: image already saved for this candidate -> never regenerate.
        if exists(key):
            results[cid] = {"candidateId": cid, "status": "GENERATED", "resultKey": key}
            ev("storage.checkpoint", "exists", {"image": pos, "key": key, "hit": True, "action": "already stored; no FLUX call"}, cid)
            continue
        if retry_reason:
            continue  # left for the scheduled retry of this task
        if quota_hit:
            results[cid] = {"candidateId": cid, "status": "FAILED", "error": "Daily free quota exhausted (not attempted)"}
            continue
        try:
            if ref is None:
                ref = s3.get_object(Bucket=BUCKET, Key=task["inputKey"])["Body"].read()
                rw, rh = Image.open(io.BytesIO(ref)).size
                ev("storage.get", "handle", {"store": f"SeaweedFS S3 bucket {BUCKET}", "key": task["inputKey"], "bytes": len(ref), "width": rw, "height": rh, "note": "loaded once, reused for all images of this task"})
            log.info(f"{tag} image {pos} generating via {provider.name}")
            t0, started = time.time(), now_iso()
            try:
                img = provider.generate(task["prompt"], ref, int(task.get("width") or 1024), int(task.get("height") or 1024))
            finally:
                call_ms = int((time.time() - t0) * 1000)
                ev("flux.call", f"{type(provider).__name__}.generate (providers.py)", {
                    "image": pos, "provider": provider.name, "model": task.get("modelId"), "startedAt": started,
                    "latencyMs": call_ms, **provider.last_meta,
                }, cid)
            metric("provider_call", jobId=job_id, taskId=task_id, candidateId=cid, attempt=attempt, outcome="ok",
                   httpStatus=provider.last_meta.get("httpStatus"), latencyMs=call_ms)
            fmt, raw_len = Image.open(io.BytesIO(img)).format, len(img)
            img = to_png(img)  # provider returns JPEG; store a verified PNG
            s3.put_object(Bucket=BUCKET, Key=key, Body=img, ContentType="image/png")
            ev("storage.put", "handle", {"image": pos, "store": f"SeaweedFS S3 bucket {BUCKET}", "key": key, "providerFormat": fmt, "providerBytes": raw_len, "storedFormat": "PNG", "storedBytes": len(img), "visibility": "private, not yet approved"}, cid)
            results[cid] = {"candidateId": cid, "status": "GENERATED", "resultKey": key}
            log.info(f"{tag} image {pos} stored {key}")
        except ProviderError as e:
            ev("flux.error", "handle", {"image": pos, "kind": e.kind, "error": str(e)[:300]}, cid)
            log.error(f"{tag} image {pos} {e.kind}: {e}")
            metric("provider_call", jobId=job_id, taskId=task_id, candidateId=cid, attempt=attempt, outcome=e.kind,
                   httpStatus=provider.last_meta.get("httpStatus"), latencyMs=call_ms, error=str(e)[:200])
            if e.kind == "RETRYABLE" and attempt < MAX_ATTEMPTS:
                retry_reason = str(e)[:200]
            elif e.kind == "RETRYABLE":
                retryable_exhausted.append(cid)
                results[cid] = {"candidateId": cid, "status": "FAILED", "error": f"Generation failed after {attempt} attempts: {str(e)[:200]}"}
            elif e.kind == "QUOTA":
                quota_hit = True
                results[cid] = {"candidateId": cid, "status": "FAILED", "error": f"Daily free quota exhausted: {str(e)[:200]}"}
            elif e.kind == "UNKNOWN":
                results[cid] = {"candidateId": cid, "status": "UNKNOWN", "error": f"Provider outcome unknown (not retried): {str(e)[:200]}"}
            else:
                results[cid] = {"candidateId": cid, "status": "FAILED", "error": f"Generation failed: {str(e)[:200]}"}
        except Exception as e:  # storage or unexpected errors: the provider result (if any) was not stored → safe to retry
            ev("worker.error", "handle", {"image": pos, "error": str(e)[:300]}, cid)
            metric("worker_error", jobId=job_id, taskId=task_id, candidateId=cid, attempt=attempt, error=str(e)[:200])
            if attempt < MAX_ATTEMPTS:
                retry_reason = str(e)[:200]
            else:
                retryable_exhausted.append(cid)
                results[cid] = {"candidateId": cid, "status": "FAILED", "error": f"Worker error: {str(e)[:200]}"}

    if retry_reason:
        # Stored images are found again by the checkpoint; UNKNOWN/FAILED decisions are carried so they are not re-sent.
        carry = {cid: {k: v for k, v in r.items() if k != "candidateId"} for cid, r in results.items() if r["status"] != "GENERATED"}
        delay = 5000 * attempt
        stored = sum(1 for r in results.values() if r["status"] == "GENERATED")
        ev("rabbit.retry", "handle", {"queue": RETRY, "delayMs": delay, "nextAttempt": attempt + 1, "reason": retry_reason,
                                      "alreadyStored": stored, "carriedOutcomes": carry,
                                      "mechanism": "whole task re-queued with per-message TTL → dead-lettered back to generation.tasks; stored images are skipped via checkpoint"})
        log.warning(f"{tag} retryable error after {stored}/{n} stored, retry in {delay}ms: {retry_reason}")
        metric("retry_scheduled", jobId=job_id, taskId=task_id, attempt=attempt, nextAttempt=attempt + 1, delayMs=delay,
               storedSoFar=stored, imageCount=n, reason=retry_reason)
        publish(ch, RETRY, {**task, "attempt": attempt + 1, "settled": carry, "trace": trace}, expiration_ms=delay)
        ch.basic_ack(method.delivery_tag)
        return

    if retryable_exhausted:
        ev("rabbit.dlq", "handle", {"queue": DLQ, "reason": "max attempts reached", "candidateIds": retryable_exhausted})
        publish(ch, DLQ, {**task_msg, "failedCandidateIds": retryable_exhausted})

    ordered = [results[cid] for cid in candidate_ids]
    counts = {}
    for r in ordered:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    result = {"taskId": task_id, "jobId": job_id, "attempt": attempt, "imageCount": n, "results": ordered}
    ev("rabbit.publish.result", "publish", {
        "queue": RESULTS, "publisher": "Python worker", "consumer": "Node (results consumer)", "publishStartedAt": now_iso(),
        "message": result, "counts": counts, "properties": {"delivery_mode": 2, "mandatory": True},
        "afterThis": "worker waits for broker confirm, then ACKs the task (not measured here; see worker console log)",
    })
    publish(ch, RESULTS, {**result, "trace": trace})  # confirmed before ACK of the task
    ch.basic_ack(method.delivery_tag)
    log.info(f"{tag} one result message published ({counts}), task acked")
    metric("task_result", jobId=job_id, taskId=task_id, finalAttempt=attempt, imageCount=n, counts=counts,
           dlqCandidates=len(retryable_exhausted))


def main():
    params = pika.URLParameters(os.getenv("AMQP_URL", "amqp://guest:guest@127.0.0.1:5672"))
    params.heartbeat = 600
    params.blocked_connection_timeout = 300
    while True:
        try:
            conn = pika.BlockingConnection(params)
            break
        except pika.exceptions.AMQPConnectionError as e:
            log.warning(f"rabbit not ready ({e!r}), retry in 3s")
            time.sleep(3)
    ch = conn.channel()
    ch.confirm_delivery()
    for q in (TASKS, RESULTS, DLQ):
        ch.queue_declare(q, durable=True)
    ch.queue_declare(RETRY, durable=True, arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": TASKS})
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(TASKS, handle, auto_ack=False)
    log.info(f"consuming {TASKS} (prefetch=1, manual ack) provider={provider.name}")
    ch.start_consuming()


if __name__ == "__main__":
    main()
