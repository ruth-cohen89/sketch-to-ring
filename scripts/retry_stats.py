"""Summarize worker/logs/generation-metrics.jsonl to evaluate the generation retry policy.
Usage: worker\\.venv\\Scripts\\python scripts\\retry_stats.py [--include-mocks]
Mock providers (fake/stub) are excluded by default."""
import json
import sys
from collections import Counter
from pathlib import Path

path = Path(__file__).resolve().parent.parent / "worker" / "logs" / "generation-metrics.jsonl"
include_mocks = "--include-mocks" in sys.argv
if not path.exists():
    sys.exit(f"no metrics yet: {path}")

rows = [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
if not include_mocks:
    rows = [r for r in rows if r.get("provider") not in ("fake", "stub")]

calls = [r for r in rows if r["event"] == "provider_call"]
tasks = [r for r in rows if r["event"] == "task_result"]
retries = [r for r in rows if r["event"] == "retry_scheduled"]
werr = [r for r in rows if r["event"] == "worker_error"]

# Per image: the attempt at which it was generated, and whether it ever hit a retryable error.
ok_at = {r["candidateId"]: r["attempt"] for r in calls if r["outcome"] == "ok"}
had_retryable = {r["candidateId"] for r in calls if r["outcome"] == "RETRYABLE"}
final_fail = Counter()
for t in tasks:
    for status, n in t["counts"].items():
        if status != "GENERATED":
            final_fail[status] += n

print(f"sample: {len(tasks)} tasks, {sum(t['imageCount'] for t in tasks)} images, {len(calls)} provider calls ({path.name})")
print("\n| metric | value |\n|---|---|")
for a in (1, 2, 3):
    print(f"| tasks finished on attempt {a} | {sum(1 for t in tasks if t['finalAttempt'] == a)} |")
for a in (1, 2, 3):
    print(f"| images generated on attempt {a} | {sum(1 for v in ok_at.values() if v == a)} |")
print(f"| images FAILED after 3 attempts (DLQ) | {sum(t.get('dlqCandidates', 0) for t in tasks)} |")
print(f"| images rescued by a retry (RETRYABLE, later ok) | {len(had_retryable & ok_at.keys())} |")
for kind, n in sorted(Counter(r["outcome"] for r in calls).items()):
    print(f"| provider calls → {kind} | {n} |")
print(f"| worker/storage errors | {len(werr)} |")
print(f"| final non-generated images by status | {dict(final_fail) or 0} |")
print(f"| retries scheduled | {len(retries)} |")
print(f"| extra wait from backoff (sum delayMs) | {sum(r['delayMs'] for r in retries) / 1000:.1f} s |")
print(f"| time spent in failed RETRYABLE calls | {sum(r.get('latencyMs') or 0 for r in calls if r['outcome'] == 'RETRYABLE') / 1000:.1f} s |")
