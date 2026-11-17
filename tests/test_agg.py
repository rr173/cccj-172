"""Full semantic test suite for the spillable aggregation executor.

Run::

    python3 -I /workspace/tests/run_tests.py
"""

from __future__ import annotations

import json
import os
import random
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spillagg import (  # noqa: E402
    ALL_MARKER,
    DeclarationError,
    RowError,
    SpillError,
    check,
    read_result,
    run,
)
from spillagg.types import NULL  # noqa: E402
from reference import reference_run  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIX = os.path.join(ROOT, "fixtures")


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def cell_map(result):
    return {(c["grouping_set"], _key(c["coords"])): c for c in result["cells"]}


def _key(coords):
    return tuple(
        (None if v is None else ("ALL" if v == ALL_MARKER else v))
        for v in coords.values()
    )


# ---------------------------------------------------------------------------
# Declarations reused across tests
# ---------------------------------------------------------------------------

DEMO_DECL = json.load(open(os.path.join(FIX, "demo_decl.json"), encoding="utf-8"))

NULL_ALL_DECL = {
    "fields": [
        {"name": "region", "type": "text"},
        {"name": "user", "type": "text"},
        {"name": "amount", "type": "int"},
        {"name": "approved", "type": "bool"},
    ],
    "grouping_sets": [["region"], []],
    "aggregates": [
        {"name": "n", "op": "count"},
        {
            "name": "u_a",
            "op": "count_distinct",
            "field": "user",
            "filter": {"op": "eq", "left": {"field": "approved"},
                       "right": {"literal": True}},
        },
        {
            "name": "u_b",
            "op": "count_distinct",
            "field": "user",
            "filter": {"op": "or", "args": [
                {"op": "is_null", "arg": {"field": "region"}},
                {"op": "lt", "left": {"field": "amount"},
                 "right": {"literal": 0}},
            ]},
        },
        {"name": "s", "op": "sum", "field": "amount"},
        {"name": "sd", "op": "sum_distinct", "field": "amount"},
        {"name": "cv", "op": "count_value", "field": "amount"},
        {"name": "mn", "op": "min", "field": "amount"},
        {"name": "mx", "op": "max", "field": "region"},
    ],
}


class AggregationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="spillagg-test-")
        self.scratch = os.path.join(self.tmp, "scratch")
        os.makedirs(self.scratch)
        self.result = os.path.join(self.tmp, "result.json")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run(self, rows, decl, **kw):
        path = os.path.join(self.tmp, "in.jsonl")
        write_jsonl(path, rows)
        stats = run([path], decl, self.result, self.scratch,
                    memory_entries=kw.pop("memory_entries", 8),
                    fan_in=kw.pop("fan_in", 4), **kw)
        return read_result(self.result), stats

    # ------------------------------------------------------------------
    # 1. Hand-computed three-row demo (A 10/2/10, B 20/0/20, total 30/2/30)
    # ------------------------------------------------------------------

    def test_01_demo_handwritten_cells(self):
        result, stats = self._run(
            [
                {"region": "A", "user": "u", "amount": 10, "approved": True},
                {"region": "B", "user": "u", "amount": 20, "approved": False},
                {"region": "A", "user": "v", "amount": None, "approved": True},
            ],
            DEMO_DECL,
            memory_entries=3,
        )
        cm = cell_map(result)
        expected = {
            (0, ("A", "ALL", "ALL", "ALL")): (10, 2, 10),
            (0, ("B", "ALL", "ALL", "ALL")): (20, 0, 20),
            (1, ("ALL", "ALL", "ALL", "ALL")): (30, 2, 30),
        }
        self.assertEqual(set(cm), set(expected))
        for key, (s, du, sd) in expected.items():
            a = cm[key]["aggregates"]
            self.assertEqual((a["sum_amount"], a["approved_users"],
                              a["distinct_amount"]), (s, du, sd))
        # grouping_id: region present -> other 3 bits set = 14; empty -> 15
        self.assertEqual(cm[(0, ("A", "ALL", "ALL", "ALL"))]["grouping_id"], 14)
        self.assertEqual(cm[(1, ("ALL",) * 4)]["grouping_id"], 15)
        # B cell retained even though its FILTER rejects every row
        self.assertEqual(cm[(0, ("B",) + ("ALL",) * 3)]["aggregates"]
                         ["approved_users"], 0)
        # scratch is fully cleaned
        self.assertEqual(os.listdir(self.scratch), [])
        self.assertEqual(stats["sort"]["peak_resident_entries"], 3)

    def test_01b_demo_matches_committed_fixture_via_cli_data(self):
        stats = run(
            [os.path.join(FIX, "demo_rows.jsonl")], DEMO_DECL,
            self.result, self.scratch, memory_entries=2,
        )
        result = read_result(self.result)
        self.assertEqual(stats["cells"], 3)
        self.assertEqual(len(result["cells"]), 3)
        self.assertTrue(stats["sort"]["spill_rounds"] >= 1)

    # ------------------------------------------------------------------
    # 2. Same distinct value across two spill shards must not double count
    # ------------------------------------------------------------------

    def test_02_distinct_across_spill_segments(self):
        rows = [
            {"region": "A", "user": "u", "amount": 5, "approved": True},
            {"region": "A", "user": "u", "amount": 5, "approved": True},
            {"region": "A", "user": "u", "amount": 5, "approved": True},
            {"region": "A", "user": "w", "amount": 7, "approved": True},
        ]
        p1 = os.path.join(self.tmp, "s1.jsonl")
        p2 = os.path.join(self.tmp, "s2.jsonl")
        write_jsonl(p1, rows[:2])
        write_jsonl(p2, rows[2:])
        run([p1, p2], DEMO_DECL, self.result, self.scratch,
            memory_entries=2, fan_in=2)
        cm = cell_map(read_result(self.result))
        total = cm[(1, ("ALL",) * 4)]["aggregates"]
        # sum keeps multiplicity: 5+5+5+7 = 22
        self.assertEqual(total["sum_amount"], 22)
        # distinct user count = u,w = 2 (never 3)
        self.assertEqual(total["approved_users"], 2)
        # distinct amount sum = 5+7 = 12
        self.assertEqual(total["distinct_amount"], 12)

    # ------------------------------------------------------------------
    # 3. Real null group coordinate vs ALL, and text "ALL" literal
    # ------------------------------------------------------------------

    def test_03_null_group_and_text_ALL_literal(self):
        rows = [
            {"region": None, "user": "ALL", "amount": 1, "approved": True},
            {"region": "ALL", "user": "ALL", "amount": 2, "approved": True},
            {"region": None, "user": "z", "amount": 3, "approved": False},
        ]
        result, _ = self._run(rows, NULL_ALL_DECL, memory_entries=3)
        cm = cell_map(result)
        null_cell = cm[(0, (None, "ALL", "ALL", "ALL"))]
        alltext_cell = cm[(0, ("ALL", "ALL", "ALL", "ALL"))]
        # The coordinate JSON: null vs {"all": true} vs literal "ALL"
        self.assertIsNone(null_cell["coords"]["region"])
        self.assertEqual(alltext_cell["coords"]["region"], "ALL")
        grand = cm[(1, ("ALL",) * 4)]
        self.assertEqual(grand["coords"]["region"], ALL_MARKER)
        # distinct users in null group (both filters): u_a only approved
        self.assertEqual(null_cell["aggregates"]["u_a"], 1)
        self.assertEqual(null_cell["aggregates"]["u_b"], 2)
        # mx is max(region), not max(user): region values are
        # None, "ALL", None -> the single non-null text is "ALL".
        self.assertEqual(grand["aggregates"]["mx"], "ALL")

    # ------------------------------------------------------------------
    # 4. Two different FILTERs on distinct aggregates stay independent
    # ------------------------------------------------------------------

    def test_04_two_distinct_filters_never_share_sets(self):
        rows = [
            {"region": "A", "user": "u", "amount": -5, "approved": True},
            {"region": "A", "user": "v", "amount": 9, "approved": True},
            {"region": "A", "user": "w", "amount": -1, "approved": False},
        ]
        result, _ = self._run(rows, NULL_ALL_DECL, memory_entries=2)
        a = cell_map(result)[(1, ("ALL",) * 4)]["aggregates"]
        self.assertEqual(a["u_a"], 2)          # u, v approved
        self.assertEqual(a["u_b"], 2)          # u, w (null region or amount<0)

    # ------------------------------------------------------------------
    # 5. Repeated identical rows: multiplicity preserved for ordinary aggs
    # ------------------------------------------------------------------

    def test_05_duplicate_rows_multiplicity(self):
        rows = [
            {"region": "A", "user": "u", "amount": 4, "approved": True},
            {"region": "A", "user": "u", "amount": 4, "approved": True},
            {"region": "A", "user": "u", "amount": 4, "approved": True},
        ]
        result, _ = self._run(rows, NULL_ALL_DECL, memory_entries=2)
        a = cell_map(result)[(0, ("A", "ALL", "ALL", "ALL"))]["aggregates"]
        self.assertEqual(a["n"], 3)
        self.assertEqual(a["cv"], 3)
        self.assertEqual(a["s"], 12)
        self.assertEqual(a["sd"], 4)           # distinct once
        self.assertEqual(a["u_a"], 1)

    # ------------------------------------------------------------------
    # 6. Negative sums cancel; sum with no non-null contribution is null
    # ------------------------------------------------------------------

    def test_06_negative_cancel_and_null_sum(self):
        rows = [
            {"region": "A", "user": "u", "amount": -100, "approved": False},
            {"region": "A", "user": "v", "amount": 100, "approved": False},
            {"region": "A", "user": "w", "amount": None, "approved": False},
        ]
        result, _ = self._run(rows, NULL_ALL_DECL, memory_entries=2)
        a = cell_map(result)[(0, ("A", "ALL", "ALL", "ALL"))]["aggregates"]
        self.assertEqual(a["s"], 0)
        # mn has no FILTER; the int amounts are -100 and 100.
        self.assertEqual(a["mn"], -100)
        self.assertEqual(a["cv"], 2)
        # u_a carries the approved FILTER; nothing passes -> count 0.
        self.assertEqual(a["u_a"], 0)
        # sd has no FILTER: the distinct amounts -100 and 100 cancel to 0
        # (a real zero, not NULL).
        self.assertEqual(a["sd"], 0)
        self.assertEqual(a["n"], 3)            # count(*) unaffected

    # ------------------------------------------------------------------
    # 7. Integers beyond 64-bit are summed exactly
    # ------------------------------------------------------------------

    def test_07_arbitrary_precision_ints(self):
        big1 = 10 ** 40 + 7
        big2 = 10 ** 40 + 3
        decl = {
            "fields": [{"name": "x", "type": "int"}],
            "grouping_sets": [[]],
            "aggregates": [
                {"name": "s", "op": "sum", "field": "x"},
                {"name": "sd", "op": "sum_distinct", "field": "x"},
                {"name": "mn", "op": "min", "field": "x"},
            ],
        }
        result, _ = self._run(
            [{"x": big1}, {"x": big2}, {"x": big2}, {"x": -2 * 10 ** 40}],
            decl, memory_entries=2,
        )
        a = result["cells"][0]["aggregates"]
        # sum keeps every row: big1 + 2*big2 - 2*10^40
        self.assertEqual(a["s"], big1 + 2 * big2 - 2 * 10 ** 40)
        # distinct values are big1, big2, -2*10^40 -> their sum is 10
        self.assertEqual(a["sd"], big1 + big2 - 2 * 10 ** 40)
        self.assertEqual(a["sd"], 10)
        self.assertEqual(a["mn"], -2 * 10 ** 40)

    # ------------------------------------------------------------------
    # 8. Empty input: only the empty grouping set emits one null/count-0 cell
    # ------------------------------------------------------------------

    def test_08_empty_input(self):
        result, stats = self._run([], NULL_ALL_DECL, memory_entries=4)
        self.assertEqual(len(result["cells"]), 1)
        c = result["cells"][0]
        self.assertEqual(c["grouping_set"], 1)
        self.assertTrue(all(v == ALL_MARKER for v in c["coords"].values()))
        count_family = {"n", "cv", "u_a", "u_b"}
        for name, agg in c["aggregates"].items():
            if name in count_family:
                self.assertEqual(agg, 0)
            else:
                self.assertIsNone(agg)
        self.assertEqual(stats["rows_in"], 0)

    def test_08b_empty_input_without_empty_grouping_set(self):
        decl = {
            "fields": [{"name": "r", "type": "text"}],
            "grouping_sets": [["r"]],
            "aggregates": [{"name": "n", "op": "count"}],
        }
        result, _ = self._run([], decl)
        self.assertEqual(result["cells"], [])

    # ------------------------------------------------------------------
    # 9. Bad last row must fail and leave no readable result / no scratch
    # ------------------------------------------------------------------

    def test_09_bad_last_row_no_result(self):
        good = os.path.join(self.tmp, "g.jsonl")
        bad = os.path.join(self.tmp, "b.jsonl")
        write_jsonl(good, [
            {"region": "A", "user": "u", "amount": 1, "approved": True}])
        with open(bad, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(
                {"region": "A", "user": "u", "amount": "not-an-int",
                 "approved": True}) + "\n")
        with self.assertRaises(RowError) as ctx:
            run([good, bad], DEMO_DECL, self.result, self.scratch,
                memory_entries=2)
        self.assertIn("expected arbitrary int", str(ctx.exception))
        self.assertFalse(os.path.exists(self.result))
        self.assertEqual(os.listdir(self.scratch), [])

    def test_09b_malformed_json_last_line(self):
        path = os.path.join(self.tmp, "x.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(
                {"region": "A", "user": "u", "amount": 1,
                 "approved": True}) + "\n")
            fh.write("{broken json\n")
        with self.assertRaises(RowError):
            run([path], DEMO_DECL, self.result, self.scratch)
        self.assertFalse(os.path.exists(self.result))

    # ------------------------------------------------------------------
    # 10. Bad declarations rejected before any disk work
    # ------------------------------------------------------------------

    def test_10_duplicate_grouping_set_rejected(self):
        decl = json.loads(json.dumps(DEMO_DECL))
        decl["grouping_sets"].append(["region"])
        with self.assertRaises(DeclarationError):
            check(decl)

    def test_10b_bad_declarations(self):
        base = {
            "fields": [{"name": "a", "type": "text"},
                       {"name": "b", "type": "bool"},
                       {"name": "c", "type": "int"}],
            "grouping_sets": [["a"]],
            "aggregates": [{"name": "n", "op": "count"}],
        }
        bad_copies = [
            {"fields": [], "grouping_sets": [[]],
             "aggregates": [{"name": "n", "op": "count"}]},
            {**base, "grouping_sets": [["nope"]]},
            {**base, "aggregates": [{"name": "n", "op": "sum", "field": "a"}]},
            {**base, "aggregates": [{"name": "n", "op": "min", "field": "b"}]},
            {**base, "aggregates": [{"name": "n", "op": "lt"}]},
            {**base, "aggregates": [{"name": "n", "op": "count",
                                     "field": "a"}]},
            {**base, "aggregates": [{"name": "n", "op": "count_distinct",
                                     "field": "c",
                                     "filter": {"op": "lt",
                                                "left": {"field": "b"},
                                                "right": {"literal": True}}}]},
            {**base, "aggregates": [{"name": "n", "op": "count_distinct",
                                     "field": "c",
                                     "filter": {"op": "eq",
                                                "left": {"field": "a"},
                                                "right": {"literal": 3}}}]},
        ]
        for decl in bad_copies:
            with self.assertRaises(DeclarationError):
                check(decl)
        # bool field rejecting a bool-where-int-expected row type
        path = os.path.join(self.tmp, "t.jsonl")
        write_jsonl(path, [{"a": "x", "b": True, "c": True}])
        with self.assertRaises(RowError):
            run([path], base, self.result, self.scratch)
        self.assertFalse(os.path.exists(self.result))

    # ------------------------------------------------------------------
    # 11. Real (injected) write failure: cleanup only, no result
    # ------------------------------------------------------------------

    def test_11_simulated_write_failure_cleans_up(self):
        rows = [
            {"region": "A", "user": "u", "amount": 1, "approved": True}
        ] * 10
        with self.assertRaises(SpillError):
            self._run(rows, DEMO_DECL, memory_entries=2,
                      simulate_write_failure=True)
        self.assertFalse(os.path.exists(self.result))
        self.assertEqual(os.listdir(self.scratch), [])

    def test_11b_real_unwritable_scratch_cleans_up(self):
        if os.geteuid() == 0:
            self.skipTest("root bypasses directory permissions")
        rows = [
            {"region": "A", "user": "u", "amount": 1, "approved": True}
        ] * 6
        ro = os.path.join(self.tmp, "readonly")
        os.makedirs(ro)
        os.chmod(ro, 0o500)  # r-x, no write
        in_path = os.path.join(self.tmp, "in.jsonl")
        write_jsonl(in_path, rows)
        try:
            with self.assertRaises((SpillError, OSError, PermissionError)):
                run([in_path], DEMO_DECL, self.result, ro,
                    memory_entries=2)
            self.assertFalse(os.path.exists(self.result))
        finally:
            os.chmod(ro, 0o700)

    # ------------------------------------------------------------------
    # 12. Input order / shard partitioning never changes cell results
    # ------------------------------------------------------------------

    def test_12_shard_and_order_invariance(self):
        rng = random.Random(1234)
        rows = [
            {"region": rng.choice(["A", "B", None]),
             "user": rng.choice(["u", "v", "w"]),
             "amount": rng.choice([1, 2, 2, None, -3]),
             "approved": rng.choice([True, False])}
            for _ in range(60)
        ]
        ref = reference_run(
            [self._dump(rows, "all.jsonl")], NULL_ALL_DECL)

        shuffled = rows[:]
        rng.shuffle(shuffled)
        p1 = self._dump(shuffled[:21], "p1.jsonl")
        p2 = self._dump(shuffled[21:47], "p2.jsonl")
        p3 = self._dump(shuffled[47:], "p3.jsonl")
        run([p1, p2, p3], NULL_ALL_DECL, self.result, self.scratch,
            memory_entries=5, fan_in=2)
        got = read_result(self.result)["cells"]
        self.assertEqual(
            sorted(map(_cell_sort_key, got)),
            sorted(map(_cell_sort_key, ref)),
        )

    def _dump(self, rows, name):
        path = os.path.join(self.tmp, name)
        write_jsonl(path, rows)
        return path

    # ------------------------------------------------------------------
    # 13. Multi-pass merge when run count exceeds fan-in
    # ------------------------------------------------------------------

    def test_13_multiple_merge_passes(self):
        rows = [
            {"region": r, "user": f"u{i}", "amount": i, "approved": True}
            for i, r in enumerate("ABCDEFGHIJ")
        ]
        result, stats = self._run(rows, DEMO_DECL,
                                  memory_entries=2, fan_in=2)
        self.assertGreaterEqual(stats["sort"]["merge_passes"], 2)
        cm = cell_map(result)
        self.assertEqual(
            cm[(1, ("ALL",) * 4)]["aggregates"]["sum_amount"],
            sum(range(10)),
        )
        self.assertEqual(
            cm[(1, ("ALL",) * 4)]["aggregates"]["approved_users"], 10)

    # ------------------------------------------------------------------
    # 14. Three-valued FILTER logic incl NOT/AND/OR and null comparisons
    # ------------------------------------------------------------------

    def test_14_three_valued_filter(self):
        decl = {
            "fields": [{"name": "t", "type": "text"},
                       {"name": "n", "type": "int"},
                       {"name": "b", "type": "bool"}],
            "grouping_sets": [[]],
            "aggregates": [
                {"name": "k1", "op": "count", "filter":
                    {"op": "not", "arg": {"op": "is_null",
                                          "arg": {"field": "t"}}}},
                {"name": "k2", "op": "count", "filter":
                    {"op": "and", "args": [
                        {"op": "lt", "left": {"field": "n"},
                         "right": {"literal": 10}},
                        {"op": "eq", "left": {"field": "b"},
                         "right": {"literal": True}}]}},
                {"name": "k3", "op": "count", "filter":
                    {"op": "or", "args": [
                        {"op": "eq", "left": {"field": "n"},
                         "right": {"literal": None}},
                        {"op": "eq", "left": {"field": "t"},
                         "right": {"literal": "x"}}]}},
            ],
        }
        rows = [
            {"t": "x", "n": 5, "b": True},    # k1 yes, k2 yes, k3 yes
            {"t": None, "n": None, "b": True},  # k1 no, k2 null, k3 yes
            {"t": "y", "n": 20, "b": False},  # k1 yes, k2 false, k3 no
            {"t": "z", "n": None, "b": True},  # k1 yes, k2 null, k3 null/no
        ]
        result, _ = self._run(rows, decl, memory_entries=3)
        a = result["cells"][0]["aggregates"]
        self.assertEqual(a["k1"], 3)
        self.assertEqual(a["k2"], 1)
        # eq against a NULL literal is UNKNOWN, never true: only row 1.
        self.assertEqual(a["k3"], 1)

    # ------------------------------------------------------------------
    # 15. Randomised enumeration: engine vs independent reference, many
    #     spill thresholds and orderings
    # ------------------------------------------------------------------

    def test_15_random_enumeration_vs_reference(self):
        rng = random.Random(20261117)
        for trial in range(40):
            n = rng.randint(0, 25)
            rows = [
                {"region": rng.choice(["A", "B", "ALL", None]),
                 "user": rng.choice(["u", "v", None]),
                 "amount": rng.choice([1, 2, 2, -1, None, 100000000000000000000]),
                 "approved": rng.choice([True, False])}
                for _ in range(n)
            ]
            decl = NULL_ALL_DECL
            ref_path = self._dump(rows, f"ref{trial}.jsonl")
            expected = reference_run([ref_path], decl)
            for mem in (1, 3, 7, 10_000):
                order = rows[:]
                rng.shuffle(order)
                in_path = self._dump(order, f"got{trial}_{mem}.jsonl")
                out = os.path.join(self.tmp, f"r{trial}_{mem}.json")
                run([in_path], decl, out, self.scratch,
                    memory_entries=mem, fan_in=3)
                got = read_result(out)["cells"]
                self.assertEqual(
                    sorted(map(_cell_sort_key, got)),
                    sorted(map(_cell_sort_key, expected)),
                    msg=f"trial {trial} mem {mem}",
                )

    # ------------------------------------------------------------------
    # 16. Stats publication: intermediate records, rounds, resident entries
    # ------------------------------------------------------------------

    def test_16_published_spill_stats(self):
        rows = [
            {"region": "A", "user": "u", "amount": 1, "approved": True}
            for _ in range(20)
        ]
        _, stats = self._run(rows, DEMO_DECL, memory_entries=5, fan_in=4)
        s = stats["sort"]
        # Multi-pass merges rewrite intermediate records, so total writes
        # are >= the originally emitted records; counts never lose rows.
        self.assertGreaterEqual(
            s["intermediate_records_written"], s["records_emitted"])
        self.assertGreater(s["spill_rounds"], 0)
        self.assertLessEqual(s["peak_resident_entries"], 5)
        self.assertGreaterEqual(s["spill_bytes_written"], 1)
        self.assertGreaterEqual(s["spill_bytes_read"], 1)

    # ------------------------------------------------------------------
    # 17. min/max on text uses code-point order; bool never behaves as int
    # ------------------------------------------------------------------

    def test_17_text_ordering_and_bool_isolation(self):
        decl = {
            "fields": [{"name": "t", "type": "text"},
                       {"name": "b", "type": "bool"}],
            "grouping_sets": [[]],
            "aggregates": [
                {"name": "mn", "op": "min", "field": "t"},
                {"name": "mx", "op": "max", "field": "t"},
                {"name": "nb", "op": "count_value", "field": "b"},
            ],
        }
        result, _ = self._run(
            [{"t": "é", "b": True}, {"t": "A", "b": False},
             {"t": "z", "b": None}, {"t": None, "b": True}],
            decl, memory_entries=2,
        )
        a = result["cells"][0]["aggregates"]
        self.assertEqual(a["mn"], "A")
        # Code-point order without normalisation: A(65) < z(122) < é(233)
        self.assertEqual(a["mx"], "é")
        self.assertEqual(a["nb"], 3)     # null ignored


def _safe(v):
    """Null-safe sort token: None/ALL-marker first, then plain values."""
    if v is None or v == ALL_MARKER:
        return (0, "")
    return (1, v)


def _cell_sort_key(cell):
    return (
        cell["grouping_set"],
        tuple((k, _safe(v)) for k, v in cell["coords"].items()),
        tuple(sorted(
            (k, 0 if v is None else v) for k, v in
            cell["aggregates"].items()
        )),
        cell["grouping_id"],
    )


if __name__ == "__main__":
    unittest.main(verbosity=2)
