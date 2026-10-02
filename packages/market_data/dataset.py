"""Versioned local datasets of historical bars (phase 3).

Layout: `{root}/{name}/manifest.json` plus one gzip CSV per symbol with REGULAR-SESSION 1-minute bars only.
The manifest records the source (provider, feed, adjustment), the range, the symbols, the official trading calendar
of the range (holidays and early closes) and a SHA-256 per file. `dataset_version` is a hash of all of that, so a
backtest stores exactly which data it saw and any change to the data changes the version.

Survivorship: the symbol list is fixed by whoever builds the dataset. A fixed list of today's large caps is biased
towards winners; record that limitation with the results.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

from packages.common.calendar import RegularHoursCalendar
from packages.common.entities import MarketBar
from packages.common.enums import Timeframe
from packages.common.errors import DataError
from packages.market_data.alpaca_history import AlpacaHistoricalClient, CalendarDay, RawBar

MANIFEST = "manifest.json"
COLUMNS = ("t", "o", "h", "l", "c", "v", "n", "vw")
NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,80}$")
REGULAR_OPEN = "09:30"


@dataclass(frozen=True)
class Dataset:
    path: Path
    manifest: dict[str, Any]

    @property
    def name(self) -> str:
        return str(self.manifest["name"])

    @property
    def version(self) -> str:
        return str(self.manifest["dataset_version"])

    @property
    def symbols(self) -> list[str]:
        return list(self.manifest["symbols"])

    @property
    def start(self) -> date:
        return date.fromisoformat(self.manifest["start"])

    @property
    def end(self) -> date:
        return date.fromisoformat(self.manifest["end"])

    @property
    def source(self) -> str:
        m = self.manifest
        return f"{m['provider']}:{m['feed']}:{m['adjustment']}:{self.version[:8]}"

    def calendar(self, timezone: str = "America/New_York") -> RegularHoursCalendar:
        return calendar_from_days(
            [
                CalendarDay(date.fromisoformat(d["date"]), d["open"], d["close"])
                for d in self.manifest["calendar"]
            ],
            self.start,
            self.end,
            timezone,
        )

    def verify(self) -> None:
        """Recompute file hashes; raises if any file changed since the manifest was written."""
        for symbol, info in self.manifest["files"].items():
            if _sha256(self.path / info["path"]) != info["sha256"]:
                raise DataError(f"dataset {self.name}: file for {symbol} does not match its manifest hash")

    def bars(self, symbol: str, start: date | None = None, end: date | None = None) -> Iterator[MarketBar]:
        info = self.manifest["files"].get(symbol)
        if info is None:
            raise DataError(f"dataset {self.name} has no symbol {symbol}")
        source = self.source
        with gzip.open(self.path / info["path"], "rt", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                started = datetime.fromisoformat(row["t"])
                day = started.date()  # bars are within the regular session: the UTC date is the trading date
                if (start is not None and day < start) or (end is not None and day > end):
                    continue
                yield MarketBar(
                    symbol=symbol,
                    timeframe=Timeframe.MIN_1,
                    start=started,
                    open=float(row["o"]),
                    high=float(row["h"]),
                    low=float(row["l"]),
                    close=float(row["c"]),
                    volume=float(row["v"]),
                    vwap=float(row["vw"]) if row["vw"] else None,
                    trade_count=int(row["n"]) if row["n"] else None,
                    source=source,
                )


def calendar_from_days(
    days: Sequence[CalendarDay], start: date, end: date, timezone: str = "America/New_York"
) -> RegularHoursCalendar:
    """Exact sessions for `start..end`: weekdays missing from the official calendar are holidays."""
    known = {d.day: d for d in days}
    for d in days:
        if d.open != REGULAR_OPEN:
            raise DataError(f"unsupported session open {d.open} on {d.day} (regular open is {REGULAR_OPEN})")
    holidays = [
        day
        for day in (date.fromordinal(o) for o in range(start.toordinal(), end.toordinal() + 1))
        if day.weekday() < 5 and day not in known
    ]
    early = {d.day: time.fromisoformat(d.close) for d in days if d.close != "16:00"}
    return RegularHoursCalendar(timezone, REGULAR_OPEN, "16:00", holidays=holidays, early_closes=early)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dataset_version(manifest: dict[str, Any]) -> str:
    identity = {k: v for k, v in manifest.items() if k not in ("dataset_version", "created_at", "name")}
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()[:16]


class DatasetStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def path(self, name: str) -> Path:
        if not NAME_PATTERN.match(name):
            raise DataError(f"invalid dataset name {name!r}")
        return self.root / name

    def names(self) -> list[str]:
        if not self.root.exists():
            return []
        return sorted(p.name for p in self.root.iterdir() if (p / MANIFEST).exists())

    def load(self, name: str) -> Dataset:
        path = self.path(name)
        manifest_path = path / MANIFEST
        if not manifest_path.exists():
            raise DataError(
                f"dataset {name!r} not found in {self.root} (python -m apps.trading_engine data list)"
            )
        return Dataset(path, json.loads(manifest_path.read_text(encoding="utf-8")))

    def download(
        self,
        client: AlpacaHistoricalClient,
        *,
        name: str,
        symbols: Sequence[str],
        start: date,
        end: date,
        feed: str,
        adjustment: str,
        today: date | None = None,
        timezone: str = "America/New_York",
        progress: Any = None,
    ) -> Dataset:
        today = today or datetime.now(UTC).date()
        if end >= today:
            raise DataError(
                "the dataset must end before today (the free plan has no SIP data for the last 15 minutes)"
            )
        if end < start:
            raise DataError("end date must not be before start date")
        symbols = sorted({s.strip().upper() for s in symbols if s.strip()})
        if not symbols:
            raise DataError("no symbols given")
        path = self.path(name)
        if (path / MANIFEST).exists():
            raise DataError(f"dataset {name!r} already exists; choose another name or delete {path}")
        path.mkdir(parents=True, exist_ok=True)

        days = client.calendar(start, end)
        calendar = calendar_from_days(days, start, end, timezone)
        files: dict[str, dict[str, Any]] = {}
        dropped = 0
        for symbol in symbols:
            kept, skipped = self._write_symbol(
                path / f"{symbol}.csv.gz", client.bars([symbol], start, end), calendar
            )
            dropped += skipped
            files[symbol] = {
                "path": f"{symbol}.csv.gz",
                "bars": kept,
                "sha256": _sha256(path / f"{symbol}.csv.gz"),
            }
            if progress is not None:
                progress(symbol, kept)
        manifest: dict[str, Any] = {
            "name": name,
            "provider": "alpaca",
            "feed": feed,
            "adjustment": adjustment,
            "timeframe": Timeframe.MIN_1.value,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "symbols": symbols,
            "calendar": [{"date": d.day.isoformat(), "open": d.open, "close": d.close} for d in days],
            "files": files,
            "dropped_outside_regular_session": dropped,
            "created_at": datetime.now(UTC).isoformat(),
        }
        manifest["dataset_version"] = dataset_version(manifest)
        (path / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        return Dataset(path, manifest)

    @staticmethod
    def _write_symbol(
        target: Path, bars: Iterator[RawBar], calendar: RegularHoursCalendar
    ) -> tuple[int, int]:
        """Keep regular-session bars only, in time order, without duplicates."""
        kept = skipped = 0
        last: datetime | None = None
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(COLUMNS)
        for bar in bars:
            started = datetime.fromisoformat(bar.t.replace("Z", "+00:00")).astimezone(UTC)
            if not calendar.is_open(started) or (last is not None and started <= last):
                skipped += 1
                continue
            last = started
            writer.writerow(
                (started.isoformat(), bar.o, bar.h, bar.low, bar.c, bar.v, "" if bar.n is None else bar.n,
                 "" if bar.vw is None else bar.vw)
            )  # fmt: skip
            kept += 1
        with gzip.open(target, "wt", encoding="utf-8", newline="") as handle:
            handle.write(buffer.getvalue())
        return kept, skipped
