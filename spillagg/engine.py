"""Spillable grouped-aggregation executor.

Pipeline (no SQL GROUP BY, no data frames, no third-party aggregation engine)::

  typed JSONL rows
      -> declaration-driven record emission (one record per
         (row x grouping_set x aggregate) contribution, plus cell markers)
      -> ExternalSorter: bounded-memory chunk sort + k-way merge on disk
      -> single ordered stream consumed by the combiner, which owns only
         one cell's states and at most one "last distinct record" at a time
      -> atomic result JSON

Cell markers guarantee that a group defined by raw rows survives even when
every aggregate FILTER rejects all of its rows.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .declaration import check_declaration
from .filter import eval_filter
from .spill import ExternalSorter, TempArea
from .types import NULL, check_value

__all__ = [
    "ALL_MARKER",
    "RowError",
    "check",
    "run",
    "read_result",
    "check_declaration",
]

ALL_MARKER = {"all": True}


class RowError(ValueError):
    """Raised when an input row is malformed or has a wrong value type."""


# Regular contribution kinds
KIND_REGULAR = 0
KIND_DISTINCT = 1
# Aggregate index used by cell-presence markers
MARKER_AGG = -1
# Placeholder payload for count(*), which has no source field.  Keeping a
# fixed int here makes every payload position type-homogeneous so Python
# tuple comparison never sees int-vs-None during external sorting.
COUNT_PLACEHOLDER = 1


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------


def _convert_row(obj: Any, fields: Sequence[Tuple[str, str]]) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise RowError("row must be a JSON object")
    expected = {name for name, _ in fields}
    if set(obj) != expected:
        raise RowError(
            f"row keys {sorted(obj)!r} do not match declared fields "
            f"{sorted(expected)!r}"
        )
    row: Dict[str, Any] = {}
    for name, ftype in fields:
        raw = obj[name]
        if raw is None:
            value = NULL
        elif ftype == "text":
            if not isinstance(raw, str):
                raise RowError(f"field {name!r}: expected text")
            value = raw
        elif ftype == "bool":
            if not isinstance(raw, bool):
                raise RowError(f"field {name!r}: expected bool")
            value = raw
        else:  # int: reject floats and booleans
            if isinstance(raw, bool) or not isinstance(raw, int):
                raise RowError(f"field {name!r}: expected arbitrary int")
            value = raw
        check_value(value, ftype)
        row[name] = value
    return row


def _iter_rows(
    input_paths: Sequence[str], fields: Sequence[Tuple[str, str]]
) -> Iterable[Tuple[str, int, Dict[str, Any]]]:
    for path in input_paths:
        try:
            fh = open(path, "r", encoding="utf-8")
        except OSError as exc:
            raise RowError(f"cannot open input {path!r}: {exc}") from exc
        with fh:
            for lineno, line in enumerate(fh, start=1):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RowError(
                        f"{path}:{lineno}: malformed JSON: {exc}"
                    ) from exc
                try:
                    yield path, lineno, _convert_row(obj, fields)
                except RowError as exc:
                    raise RowError(f"{path}:{lineno}: {exc}") from exc


# ---------------------------------------------------------------------------
# Record emission
# ---------------------------------------------------------------------------


def _emit_records(
    row: Dict[str, Any],
    normalized: Dict[str, Any],
    sorter: ExternalSorter,
) -> None:
    sets: List[Tuple[str, ...]] = normalized["grouping_sets"]
    aggs = normalized["aggregates"]
    filter_ok: List[bool] = []
    for agg in aggs:
        flt = agg["filter"]
        if flt is None:
            filter_ok.append(True)
        else:
            filter_ok.append(eval_filter(flt, row) is True)

    for gs_idx, gs in enumerate(sets):
        gk = tuple(row[f] for f in gs)
        # Cell-presence marker: one per raw row per grouping set, so the
        # group exists regardless of every FILTER's outcome.
        sorter.add((gs_idx, *gk, MARKER_AGG))
        for ai, agg in enumerate(aggs):
            if not filter_ok[ai]:
                continue
            op = agg["op"]
            # count(*) has no source field; every other aggregate reads its
            # declared field and keeps the NULL sentinel intact.
            value = COUNT_PLACEHOLDER if agg["field"] is None \
                else row[agg["field"]]
            if op == "count":
                # Uniform 5-position shape; payload is an unused int token.
                sorter.add((gs_idx, *gk, ai, KIND_REGULAR, COUNT_PLACEHOLDER))
                continue
            # All remaining ops ignore NULL values; distinct never sees NULL.
            if value is NULL:
                continue
            if agg["distinct"]:
                sorter.add((gs_idx, *gk, ai, KIND_DISTINCT, value))
            else:
                sorter.add((gs_idx, *gk, ai, KIND_REGULAR, value))


# ---------------------------------------------------------------------------
# Combining phase
# ---------------------------------------------------------------------------


def _default_state(op: str) -> Dict[str, Any]:
    # Plain counts and distinct counts start at 0 even when no row
    # contributes; sum/min/max (including sum_distinct) start at NULL until
    # a non-null contribution arrives.
    if op in ("count", "count_value", "count_distinct"):
        return {"value": 0}
    return {"value": None}


class _CellWriter:
    """Streams cells to a temporary JSON file (bounded memory)."""

    def __init__(self, normalized: Dict[str, Any], path: str):
        self.normalized = normalized
        self.path = path
        self.fh = open(path, "w", encoding="utf-8")
        self.field_names = [n for n, _ in normalized["fields"]]
        self.aggregates = normalized["aggregates"]
        self.first = True
        self.cells = 0
        self.groups_per_set: Dict[int, int] = {}
        self.fh.write('{"declaration_digest": "')
        self.fh.write(normalized["digest"])
        self.fh.write('", "cells": [')

    def _coord(self, gs_idx: int, gk: Tuple[Any, ...]) -> Dict[str, Any]:
        gs = self.normalized["grouping_sets"][gs_idx]
        vals = dict(zip(gs, gk))
        coords: Dict[str, Any] = {}
        for name in self.field_names:
            if name in vals:
                coords[name] = None if vals[name] is NULL else vals[name]
            else:
                coords[name] = ALL_MARKER
        return coords

    def _grouping_id(self, gs_idx: int) -> int:
        gs = set(self.normalized["grouping_sets"][gs_idx])
        gid = 0
        for i, (name, _) in enumerate(self.normalized["fields"]):
            if name not in gs:
                gid |= 1 << i
        return gid

    def flush_cell(
        self,
        gs_idx: int,
        gk: Tuple[Any, ...],
        states: List[Dict[str, Any]],
    ) -> None:
        agg_values = {
            agg["name"]: states[ai]["value"]
            for ai, agg in enumerate(self.aggregates)
        }
        cell = {
            "grouping_set": gs_idx,
            "grouping_id": self._grouping_id(gs_idx),
            "coords": self._coord(gs_idx, gk),
            "aggregates": agg_values,
        }
        if not self.first:
            self.fh.write(",")
        self.first = False
        json.dump(cell, self.fh, ensure_ascii=False)
        self.cells += 1
        self.groups_per_set[gs_idx] = self.groups_per_set.get(gs_idx, 0) + 1

    def finish(self, stats: Dict[str, Any]) -> None:
        self.fh.write('], "stats": ')
        json.dump(stats, self.fh, ensure_ascii=False)
        self.fh.write("}")
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()


def _new_states(aggs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [_default_state(agg["op"]) for agg in aggs]


def _apply(
    state: Dict[str, Any],
    op: str,
    kind: int,
    payload: Any,
    record: Tuple[Any, ...],
    last_distinct: List[Any],
    ai: int,
) -> List[Any]:
    """Fold one sorted record into one aggregate's state."""
    if kind == KIND_DISTINCT:
        # The stream is fully ordered, so adjacent equal distinct keys are
        # duplicates.  Compare only the key portion (cell prefix + ai +
        # payload); record arities differ between regular and distinct
        # records, so whole-tuple equality is not used.
        if last_distinct[ai] is not None and last_distinct[ai] == payload:
            return last_distinct
        last_distinct[ai] = payload
        if op == "count_distinct":
            state["value"] += 1
        else:  # sum_distinct, int payload
            state["value"] = (
                payload if state["value"] is None else state["value"] + payload
            )
        return last_distinct

    if op in ("count", "count_value"):
        state["value"] += 1
    elif op == "sum":
        state["value"] = (
            payload if state["value"] is None else state["value"] + payload
        )
    elif op == "min":
        if state["value"] is None or payload < state["value"]:
            state["value"] = payload
    elif op == "max":
        if state["value"] is None or payload > state["value"]:
            state["value"] = payload
    return last_distinct


def _combine(
    stream: Iterable[Tuple[Any, ...]],
    normalized: Dict[str, Any],
    writer: _CellWriter,
) -> None:
    aggs = normalized["aggregates"]
    set_lengths = [len(gs) for gs in normalized["grouping_sets"]]

    cur_gs: Optional[int] = None
    cur_gk: Optional[Tuple[Any, ...]] = None
    states: List[Dict[str, Any]] = []
    last_distinct: List[Any] = [None] * len(aggs)

    for record in stream:
        gs_idx = record[0]
        L = set_lengths[gs_idx]
        gk = tuple(record[1 : 1 + L])
        ai = record[1 + L]
        # Marker layout: (gs, *gk, -1); contribution layout:
        # (gs, *gk, ai, kind[, payload]).  kind therefore sits at L+2.
        kind_pos = 2 + L

        if ai == MARKER_AGG:
            if cur_gs != gs_idx or cur_gk != gk:
                if cur_gs is not None:
                    writer.flush_cell(cur_gs, cur_gk, states)  # type: ignore[arg-type]
                cur_gs, cur_gk = gs_idx, gk
                states = _new_states(aggs)
                # Distinct memory is per (cell, aggregate): reset on every
                # new cell so equal values in different cells both count.
                last_distinct = [None] * len(aggs)
            continue

        if cur_gs != gs_idx or cur_gk != gk:  # pragma: no cover
            # Markers always precede a cell's aggregate records.
            raise RuntimeError("internal error: record without cell marker")
        kind = record[kind_pos]
        # No explicit ai switching is needed: records of one (cell, ai) are
        # contiguous in the ordered stream, and the cell reset above already
        # cleared every slot.  Interleaved regular records of other aggs
        # never touch this aggregate's slot.
        payload = record[kind_pos + 1] if len(record) > kind_pos + 1 else None
        _apply(states[ai], aggs[ai]["op"], kind, payload, record,
               last_distinct, ai)

    if cur_gs is not None:
        writer.flush_cell(cur_gs, cur_gk, states)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def check(declaration: Any) -> Dict[str, Any]:
    """Validate a declaration; return its normalised form.

    Raises :class:`DeclarationError` (a ``ValueError``) on any problem,
    including duplicate grouping sets.  Pure function, touches no disk.
    """
    return check_declaration(declaration)


def run(
    input_paths: Sequence[str],
    declaration: Dict[str, Any],
    result_path: str,
    scratch_dir: str,
    *,
    memory_entries: int = 100_000,
    fan_in: int = 64,
    simulate_write_failure: bool = False,
) -> Dict[str, Any]:
    """Execute one aggregation and write the result atomically.

    On any failure (bad declaration, bad row, real/simulated disk error) no
    result is published and every scratch file of this run is removed.
    Returns the statistics block embedded in the result file.
    """
    normalized = check_declaration(declaration)  # raises before disk use
    area = TempArea(scratch_dir)
    rows_in = 0
    stream = None
    try:
        sorter = ExternalSorter(
            area, memory_entries=memory_entries, fan_in=fan_in
        )
        if simulate_write_failure:
            area.simulate_write_failure()

        fields = normalized["fields"]
        for _path, _lineno, row in _iter_rows(input_paths, fields):
            rows_in += 1
            _emit_records(row, normalized, sorter)

        result_tmp = os.path.join(area.root, "result.json")
        writer = _CellWriter(normalized, result_tmp)
        try:
            if rows_in == 0:
                # Empty input: only the empty grouping set yields one cell,
                # with counts 0 and every other aggregate NULL.
                for gs_idx, gs in enumerate(normalized["grouping_sets"]):
                    if len(gs) == 0:
                        writer.flush_cell(
                            gs_idx, (), _new_states(normalized["aggregates"])
                        )
            else:
                stream = sorter.merged_stream()
                _combine(stream, normalized, writer)
            stats = {
                "rows_in": rows_in,
                "cells": writer.cells,
                "groups_per_set": {
                    str(k): v for k, v in writer.groups_per_set.items()
                },
                "sort": sorter.stats,
            }
            writer.finish(stats)
        except BaseException:
            writer.fh.close()
            raise
        finally:
            # Close the merge generator deterministically so its own finally
            # block (file-handle close + run-file unlink) runs before the
            # scratch area is purged, rather than waiting for garbage
            # collection.
            if stream is not None:
                close = getattr(stream, "close", None)
                if close is not None:
                    close()

        os.makedirs(os.path.dirname(os.path.abspath(result_path)), exist_ok=True)
        os.replace(result_tmp, result_path)
        return stats
    finally:
        # Runs on both success and failure; rmtree purges every artifact of
        # this execution, so a failed run never leaves readable scratch data.
        area.cleanup()


def read_result(result_path: str) -> Dict[str, Any]:
    """Load a result file. ALL coordinates use the {"all": true} marker;
    real nulls appear as JSON null and stay distinct from it."""
    with open(result_path, "r", encoding="utf-8") as fh:
        return json.load(fh)
