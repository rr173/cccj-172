"""External (spill-to-disk) sorting with multi-pass k-way merge.

The engine never keeps a whole group or a whole distinct set in memory.
Instead every contribution is written as a *record* (a tuple of typed
values), records are sorted externally on disk, and a single ordered stream
is consumed by the combining phase.  Distinct values are discovered while
adjacent records flow past, so an arbitrarily large distinct domain and an
arbitrarily hot group require only O(merge fan-in) resident state.

Implementation notes
--------------------
* Spill files are newline-delimited JSON using the typed codec in
  :mod:`spillagg.types`, so big ints and the NULL sentinel round-trip
  exactly.
* In-memory chunks are plain lists of tuples; Python's own total tuple
  ordering gives the required ordering (NULL first, then code-point text,
  numeric int, bool) because every position has one fixed declared type.
* Merge fan-in is bounded: when the number of runs exceeds ``fan_in`` the
  sorter performs additional merge passes, every pass still streaming.
* No SQL, no data frames, no third-party aggregation machinery anywhere.
"""

from __future__ import annotations

import gc
import heapq
import json
import os
import uuid
from typing import Any, Dict, Iterator, List, Tuple

from .types import decode_record, encode_record


class SpillError(RuntimeError):
    """Raised when real disk I/O for staging/sorting fails."""


class TempArea:
    """Owns the scratch directory of one execution.

    On failure the whole area is removed, so a crashed run never leaves a
    readable result or partial scratch data behind.
    """

    def __init__(self, scratch_root: str):
        self.root = os.path.join(
            scratch_root, "spillagg-" + uuid.uuid4().hex
        )
        os.makedirs(self.root, exist_ok=False)
        self._files: List[str] = []
        # Test hook: when set, the next flush raises SpillError before data
        # is durably written.
        self.fail_next_write = False

    def new_file(self) -> str:
        path = os.path.join(self.root, "run-" + uuid.uuid4().hex + ".jsonl")
        self._files.append(path)
        return path

    def unregister(self, path: str) -> None:
        if path in self._files:
            self._files.remove(path)

    def cleanup(self) -> None:
        """Recursively remove every scratch artifact of this execution.

        Walks the directory rather than relying on the registered file list,
        because merge passes retire and rewrite files under new names.
        """
        import shutil

        shutil.rmtree(self.root, ignore_errors=False)

    def simulate_write_failure(self) -> None:
        self.fail_next_write = True


class ExternalSorter:
    """Bounded-memory external sorter for typed tuple records."""

    def __init__(
        self,
        area: TempArea,
        *,
        memory_entries: int,
        fan_in: int = 64,
    ) -> None:
        if memory_entries < 1:
            raise ValueError("memory_entries must be >= 1")
        if fan_in < 2:
            raise ValueError("fan_in must be >= 2")
        self.area = area
        self.memory_entries = memory_entries
        self.fan_in = fan_in

        self._buffer: List[Tuple[Any, ...]] = []
        self._runs: List[str] = []
        self._peak_resident = 0

        # Published statistics
        self.stats: Dict[str, Any] = {
            "records_emitted": 0,
            "intermediate_records_written": 0,
            "spill_rounds": 0,          # number of flush rounds
            "merge_passes": 0,
            "spill_files_created": 0,
            "spill_bytes_written": 0,
            "spill_bytes_read": 0,
            "peak_resident_entries": 0,
            "memory_entries_limit": memory_entries,
            "fan_in": fan_in,
        }

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def add(self, record: Tuple[Any, ...]) -> None:
        self._buffer.append(record)
        if len(self._buffer) > self._peak_resident:
            self._peak_resident = len(self._buffer)
        self.stats["records_emitted"] += 1
        if len(self._buffer) >= self.memory_entries:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        if self.area.fail_next_write:
            self.area.fail_next_write = False
            raise SpillError("simulated disk write failure")
        self._buffer.sort()
        path = self.area.new_file()
        self.stats["spill_files_created"] += 1
        self.stats["spill_rounds"] += 1
        bytes_written = 0
        try:
            with open(path, "w", encoding="utf-8") as fh:
                for rec in self._buffer:
                    line = json.dumps(
                        encode_record(rec), ensure_ascii=False
                    )
                    fh.write(line)
                    fh.write("\n")
                    bytes_written += len(line) + 1
        except OSError as exc:
            raise SpillError(f"failed to write spill file {path}: {exc}") from exc
        self._runs.append(path)
        self.stats["spill_bytes_written"] += bytes_written
        self.stats["intermediate_records_written"] += len(self._buffer)
        self.stats["peak_resident_entries"] = max(
            self.stats["peak_resident_entries"], self._peak_resident
        )
        self._buffer = []
        self._peak_resident = 0
        gc.collect()

    # ------------------------------------------------------------------
    # Merge machinery
    # ------------------------------------------------------------------

    @staticmethod
    def _iter_run(path: str) -> Iterator[Tuple[Any, ...]]:
        try:
            fh = open(path, "r", encoding="utf-8")
        except OSError as exc:
            raise SpillError(f"failed to open spill file {path}: {exc}") from exc
        with fh:
            for line in fh:
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise SpillError(f"corrupt spill file {path}: {exc}") from exc
                yield decode_record(raw)

    def _merge_runs(
        self, runs: List[str]
    ) -> Iterator[Tuple[Any, ...]]:
        open_iters = []
        try:
            for path in runs:
                it = self._iter_run(path)
                open_iters.append(it)
            # Heap entries: (record, stream_index); stream_index breaks ties
            # deterministically when identical records appear in several runs.
            heap: List[Tuple[Tuple[Any, ...], int]] = []
            for idx, it in enumerate(open_iters):
                try:
                    first = next(it)
                except StopIteration:
                    continue
                heapq.heappush(heap, (first, idx))
            while heap:
                record, idx = heapq.heappop(heap)
                yield record
                try:
                    nxt = next(open_iters[idx])
                except StopIteration:
                    continue
                heapq.heappush(heap, (nxt, idx))
        finally:
            # Generators own their file handles; closing them closes files.
            for it in open_iters:
                close = getattr(it, "close", None)
                if close is not None:
                    close()

    def _rewrite_merged(
        self, runs: List[str]
    ) -> str:
        """Merge *runs* into a brand-new spill file (one extra merge pass)."""
        out_path = self.area.new_file()
        self.stats["spill_files_created"] += 1
        count = 0
        bytes_written = 0
        bytes_read = 0
        try:
            with open(out_path, "w", encoding="utf-8") as out:
                for rec in self._merge_runs(runs):
                    line = json.dumps(
                        encode_record(rec), ensure_ascii=False
                    )
                    out.write(line)
                    out.write("\n")
                    bytes_written += len(line) + 1
                    count += 1
        except OSError as exc:
            raise SpillError(
                f"failed to rewrite merged spill {out_path}: {exc}"
            ) from exc
        for path in runs:
            try:
                bytes_read += os.path.getsize(path)
                os.remove(path)
                self.area.unregister(path)
            except OSError as exc:
                raise SpillError(f"failed to retire spill {path}: {exc}") from exc
        self.stats["spill_bytes_written"] += bytes_written
        self.stats["spill_bytes_read"] += bytes_read
        self.stats["intermediate_records_written"] += count
        return out_path

    def merged_stream(self) -> Iterator[Tuple[Any, ...]]:
        """Return the fully ordered record stream.

        Remaining buffered records are flushed first; merge passes are added
        whenever the run count exceeds ``fan_in``.  At most ``fan_in`` file
        handles/iterators are open at any moment.
        """
        self.flush()
        self.stats["peak_resident_entries"] = max(
            self.stats["peak_resident_entries"], self._peak_resident
        )
        runs = list(self._runs)
        while len(runs) > self.fan_in:
            next_runs: List[str] = []
            for i in range(0, len(runs), self.fan_in):
                chunk = runs[i : i + self.fan_in]
                if len(chunk) == 1:
                    next_runs.append(chunk[0])
                else:
                    next_runs.append(self._rewrite_merged(chunk))
                    self.stats["merge_passes"] += 1
            runs = next_runs
        self.stats["merge_passes"] += 1 if runs else 0
        if not runs:
            return
        # Track bytes read for the final pass lazily; sizes are known now.
        for path in runs:
            try:
                self.stats["spill_bytes_read"] += os.path.getsize(path)
            except OSError:
                pass
        stream = self._merge_runs(runs)
        try:
            yield from stream
        finally:
            # Closing the generators closes their file handles; only then
            # can the run files be unlinked on every platform.
            close = getattr(stream, "close", None)
            if close is not None:
                close()
            for path in runs:
                try:
                    os.remove(path)
                    self.area.unregister(path)
                except FileNotFoundError:
                    pass
                except OSError:
                    # TempArea.cleanup() still performs a recursive purge.
                    pass
