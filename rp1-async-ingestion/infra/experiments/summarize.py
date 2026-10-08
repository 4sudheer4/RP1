#!/usr/bin/env python3
"""Builds a markdown table from every <run>/summary.json + verify.json under an experiment directory."""
import json
import os
import sys


def load(p):
    try:
        with open(p) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def metric(delta, name, **labels):
    total = 0.0
    for k, v in delta.items():
        if not k.startswith(name):
            continue
        if all(f'{lk}="{lv}"' in k for lk, lv in labels.items()):
            total += v
    return total


def parse_prom(text, prefix):
    out = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith(prefix) or "_bucket" in line:
            continue
        name, _, value = line.rpartition(" ")
        try:
            out[name] = float(value)
        except ValueError:
            pass
    return out


def main(root):
    rows = []
    for name in sorted(os.listdir(root)):
        d = os.path.join(root, name)
        if not os.path.isdir(d) or name.endswith(".logs"):
            continue
        s, v = load(os.path.join(d, "summary.json")), load(os.path.join(d, "verify.json"))
        if not s:
            continue
        delta = s.get("serverMetricsDelta", {})
        final = os.path.join(d, "metrics-final.prom")
        if os.path.exists(final):  # scraped after drain; services start fresh per run, so final == delta
            delta = parse_prom(open(final).read(), "rp1_ingest_")
        sync_total = metric(delta, "rp1_ingest_sync_batches_total")
        sync_late = metric(delta, "rp1_ingest_sync_batches_total", late="true")
        batches = s.get("batches", 0) or 1
        rows.append({
            "run": name,
            "rate": s["config"]["rateBatchesPerSec"],
            "first": s.get("batchCaptureFirstAttempt"),
            "eventual": s.get("batchCaptureEventual"),
            "events": v.get("eventCapture"),
            "lost": v.get("lostUnaccounted"),
            "dup": v.get("duplicateRows"),
            "mismatch": v.get("statusMismatches"),
            "p50": s.get("firstAttemptLatencyMs", {}).get("p50"),
            "p99": s.get("firstAttemptLatencyMs", {}).get("p99"),
            "attempts": s.get("clientAttemptsPerBatch"),
            "susp": s.get("batchesDroppedBySuspension"),
            "amp": round(sync_total / batches, 2) if sync_total else None,
            "late": round(sync_late / sync_total, 3) if sync_total else None,
        })
    print("| run | rate b/s | batch capture 1st try | batch capture eventual | event capture | lost | dup rows | "
          "status mismatches | 1st-try ack p50 ms | p99 ms | client attempts/batch | server batches/batch (sync) | "
          "share done after client timeout (sync) | batches dropped by ENS suspension |")
    print("|" + "---|" * 14)
    for r in rows:
        print("| {run} | {rate:g} | {first} | {eventual} | {events} | {lost} | {dup} | {mismatch} | {p50} | {p99} | "
              "{attempts} | {amp} | {late} | {susp} |".format(**{k: ("-" if v is None else v) for k, v in r.items()}))
    print()
    print("Definitions: batch capture = share of batches that got 2xx (first attempt / within ENS's 3 retries). "
          "Event capture = distinct generated valid events present in Postgres after drain / generated. "
          "Server batches/batch = batches the sync server fully processed / batches generated (>1 = work for "
          "requests the client had abandoned and retried).")


if __name__ == "__main__":
    main(sys.argv[1])
