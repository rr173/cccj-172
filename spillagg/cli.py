"""Command line entry point: check / run / read."""

from __future__ import annotations

import argparse
import json
import sys

from .declaration import DeclarationError
from .engine import RowError, check, read_result, run
from .spill import SpillError


def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="spillagg",
        description="Spillable grouped aggregation executor "
        "(external sort/merge, no SQL GROUP BY, no data frames)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_check = sub.add_parser("check", help="validate an aggregation declaration")
    p_check.add_argument("declaration")

    p_run = sub.add_parser("run", help="execute an aggregation")
    p_run.add_argument("--decl", required=True)
    p_run.add_argument(
        "--input", required=True, nargs="+",
        help="one or more typed JSONL input shards",
    )
    p_run.add_argument("--result", required=True)
    p_run.add_argument("--scratch", required=True)
    p_run.add_argument("--memory-entries", type=int, default=100_000)
    p_run.add_argument("--fan-in", type=int, default=64)
    p_run.add_argument(
        "--simulate-write-failure", action="store_true",
        help="fault injection: first spill flush fails (tests cleanup)",
    )

    p_read = sub.add_parser("read", help="print a result file as JSON")
    p_read.add_argument("result")

    args = parser.parse_args(argv)

    if args.cmd == "check":
        try:
            normalized = check(_load_json(args.declaration))
        except (DeclarationError, OSError, json.JSONDecodeError) as exc:
            print(f"declaration rejected: {exc}", file=sys.stderr)
            return 2
        print(
            json.dumps(
                {
                    "ok": True,
                    "digest": normalized["digest"],
                    "grouping_sets": [list(gs) for gs in
                                      normalized["grouping_sets"]],
                    "aggregate_count": len(normalized["aggregates"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if args.cmd == "read":
        print(json.dumps(read_result(args.result), ensure_ascii=False, indent=2))
        return 0

    # run
    try:
        decl = _load_json(args.decl)
        stats = run(
            args.input,
            decl,
            args.result,
            args.scratch,
            memory_entries=args.memory_entries,
            fan_in=args.fan_in,
            simulate_write_failure=args.simulate_write_failure,
        )
    except DeclarationError as exc:
        print(f"declaration rejected: {exc}", file=sys.stderr)
        return 2
    except RowError as exc:
        print(f"input rejected: {exc}", file=sys.stderr)
        return 3
    except SpillError as exc:
        print(f"spill failure: {exc}", file=sys.stderr)
        return 4
    print(json.dumps({"ok": True, "stats": stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
