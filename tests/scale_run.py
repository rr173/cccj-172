#!/usr/bin/env python3
"""Scale run: 1M rows, one hot group, high-cardinality distinct domain.

Generates data at runtime into the given directory (nothing large is
committed), executes the same declaration under several spill thresholds,
and compares every result cell across thresholds while reporting real peak
RSS (from /proc, sampled in a child process) and the published spill stats.

Usage (isolated mode)::

    python3 -I /workspace/tests/scale_run.py --rows 1000000 --workdir /workspace/data/scale
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spillagg import read_result, run  # noqa: E402

DECL = {
    "fields": [
        {"name": "region", "type": "text"},
        {"name": "user", "type": "text"},
        {"name": "amount", "type": "int"},
        {"name": "approved", "type": "bool"},
    ],
    "grouping_sets": [["region"], []],
    "aggregates": [
        {"name": "n", "op": "count"},
        {"name": "sum_amount", "op": "sum", "field": "amount"},
        {"name": "sumd_amount", "op": "sum_distinct", "field": "amount"},
        {
            "name": "approved_users",
            "op": "count_distinct",
            "field": "user",
            "filter": {
                "op": "eq",
                "left": {"field": "approved"},
                "right": {"literal": True},
            },
        },
        {"name": "min_amount", "op": "min", "field": "amount"},
        {"name": "max_region", "op": "max", "field": "region"},
    ],
}


def generate(path: str, rows: int, hot_share: float, seed: int) -> dict:
    """Write *rows* JSONL lines; one region holds hot_share of all rows."""
    import random

    rng = random.Random(seed)
    hot_cut = int(rows * hot_share)
    totals = {"sum": 0, "distinct_amounts": set(), "approved_users": set()}
    with open(path, "w", encoding="utf-8") as fh:
        for i in range(rows):
            if i < hot_cut:
                region = "HOT"
            else:
                region = rng.choice(["R1", "R2", "R3", "R4"])
            # High distinct domain: mostly unique users, occasional repeats
            user = f"u{rng.randrange(int(rows * 0.9))}"
            amount = rng.randrange(-1_000_000, 1_000_000)
            approved = rng.random() < 0.7
            fh.write(
                json.dumps(
                    {
                        "region": region,
                        "user": user,
                        "amount": amount,
                        "approved": approved,
                    }
                )
                + "\n"
            )
            totals["sum"] += amount
            totals["distinct_amounts"].add(amount)
            if approved:
                totals["approved_users"].add(user)
    return {
        "expected_sum": totals["sum"],
        "expected_distinct_amount_sum": sum(totals["distinct_amounts"]),
        "expected_approved_users": len(totals["approved_users"]),
    }


def _run_one_threshold(args, data_path: str, decl: dict, mem: int) -> dict:
    """Execute one threshold in a fresh child process.

    The child's peak RSS is obtained from ``wait4`` (per-process high-water
    mark), so different thresholds get honest, independent RSS numbers
    instead of one cumulative parent high-water mark.
    """
    result_path = os.path.join(args.workdir, f"result-{mem}.json")
    scratch = os.path.join(args.workdir, f"scratch-{mem}")
    os.makedirs(scratch, exist_ok=True)
    child_cmd = [
        sys.executable, "-I", os.path.abspath(__file__),
        "--child",
        "--rows", str(args.rows),
        "--workdir", args.workdir,
        "--data", data_path,
        "--result", result_path,
        "--scratch", scratch,
        "--threshold", str(mem),
        "--fan-in", str(args.fan_in),
    ]
    t0 = time.time()
    pid = os.spawnv(os.P_NOWAIT, child_cmd[0], child_cmd)
    wait_pid, wait_status, ru = os.wait4(pid, 0)
    if wait_status != 0:
        raise RuntimeError(
            f"child threshold {mem} failed with status {wait_status}"
        )
    elapsed = time.time() - t0
    payload_path = os.path.join(args.workdir, f"child-stats-{mem}.json")
    with open(payload_path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    stats = payload["stats"]
    result = read_result(result_path)
    return {
        "memory_entries": mem,
        "elapsed_s": round(elapsed, 2),
        "peak_rss_mb": round(ru.ru_maxrss / 1024.0, 1),
        "cells": stats["cells"],
        "records_emitted": stats["sort"]["records_emitted"],
        "intermediate_records_written":
            stats["sort"]["intermediate_records_written"],
        "spill_rounds": stats["sort"]["spill_rounds"],
        "merge_passes": stats["sort"]["merge_passes"],
        "peak_resident_entries":
            stats["sort"]["peak_resident_entries"],
        "spill_mb_written":
            round(stats["sort"]["spill_bytes_written"] / 1e6, 1),
        "grand_totals": next(
            c["aggregates"] for c in result["cells"]
            if all(v == {"all": True} for v in c["coords"].values())
        ),
    }


def _child_main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--result", required=True)
    ap.add_argument("--scratch", required=True)
    ap.add_argument("--threshold", type=int, required=True)
    ap.add_argument("--fan-in", type=int, default=32)
    a = ap.parse_args()
    stats = run(
        [a.data], DECL, a.result, a.scratch,
        memory_entries=a.threshold, fan_in=a.fan_in,
    )
    with open(os.path.join(a.workdir,
                           f"child-stats-{a.threshold}.json"),
              "w", encoding="utf-8") as fh:
        json.dump({"stats": stats}, fh)
    return 0


def main() -> int:
    if "--child" in sys.argv:
        sys.argv.remove("--child")
        return _child_main()

    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--thresholds",
        type=int,
        nargs="+",
        default=[20_000, 50_000, 150_000],
    )
    ap.add_argument("--fan-in", type=int, default=32)
    args = ap.parse_args()

    os.makedirs(args.workdir, exist_ok=True)
    data_path = os.path.join(args.workdir, "rows.jsonl")
    if not os.path.exists(data_path):
        t0 = time.time()
        expected = generate(
            data_path, args.rows, hot_share=0.6, seed=args.seed
        )
        print(f"generated {args.rows} rows in {time.time() - t0:.1f}s")
    else:
        expected = None
        print("reusing existing rows.jsonl")

    summary = []
    baseline_cells = None
    for mem in args.thresholds:
        row = _run_one_threshold(args, data_path, DECL, mem)
        result_path = os.path.join(args.workdir, f"result-{mem}.json")
        canonical = json.dumps(
            read_result(result_path)["cells"], sort_keys=True
        )
        if baseline_cells is None:
            baseline_cells = canonical
        row["cells_match_baseline"] = canonical == baseline_cells
        summary.append(row)
        print(json.dumps(row, ensure_ascii=False))

    if expected is not None:
        grand = json.loads(baseline_cells)
        total_cell = next(
            c["aggregates"]
            for c in grand
            if all(v == {"all": True} for v in c["coords"].values())
        )
        checks = {
            "sum_amount":
                total_cell["sum_amount"] == expected["expected_sum"],
            "sumd_amount":
                total_cell["sumd_amount"]
                == expected["expected_distinct_amount_sum"],
            "approved_users":
                total_cell["approved_users"]
                == expected["expected_approved_users"],
        }
        print("independent total checks:", json.dumps(checks))
        if not all(checks.values()):
            return 5

    if not all(r["cells_match_baseline"] for r in summary):
        print("THRESHOLD MISMATCH", file=sys.stderr)
        return 6
    out = os.path.join(args.workdir, "scale-summary.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False)
    print("summary written to", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
