"""Alpaca historical client and dataset builder against an in-memory Alpaca (no network)."""

from __future__ import annotations

import gzip
from datetime import UTC, date, datetime, time
from pathlib import Path

import pytest

from packages.common.config import AlpacaDataSection
from packages.common.entities import MarketBar
from packages.common.enums import Timeframe
from packages.common.errors import ConfigError, DataError
from packages.market_data.alpaca_history import AlpacaHistoricalClient, credentials
from packages.market_data.dataset import DatasetStore, dataset_version
from packages.market_data.historical import HistoricalMarketDataAdapter
from packages.market_data.spreads import (
    DEFAULT_TIMES,
    calibrate_spreads,
    load_spreads,
    sample_days,
    typical_spreads,
    write_spreads,
)
from tests.fake_alpaca import EARLY_CLOSE, HOLIDAY, INDEPENDENCE_DAY, QUOTED_SPREAD_BPS, FakeAlpaca

CFG = AlpacaDataSection(min_request_interval_seconds=0)


def client(fake: FakeAlpaca, key: str = "test-key") -> AlpacaHistoricalClient:
    return AlpacaHistoricalClient(
        CFG, key=key, secret="secret", transport=fake.transport(), sleep=lambda s: None
    )


def test_credentials_come_from_the_environment() -> None:
    assert credentials({"APCA_API_KEY_ID": "k", "APCA_API_SECRET_KEY": "s"}) == ("k", "s")
    with pytest.raises(ConfigError):
        credentials({"APCA_API_KEY_ID": "k"})


def test_bars_are_paginated_and_sent_with_the_documented_parameters() -> None:
    fake = FakeAlpaca(page_size=100)
    with client(fake) as c:
        bars = list(c.bars(["SPY"], date(2024, 3, 25), date(2024, 3, 25)))
    assert len(bars) == 390 + 2  # regular session + one pre-market and one after-hours bar
    assert len(fake.requests) == 4
    first = fake.requests[0]
    assert first.url.path == "/v2/stocks/bars"
    assert first.headers["APCA-API-KEY-ID"] == "test-key" and first.headers["APCA-API-SECRET-KEY"] == "secret"
    params = first.url.params
    assert (params["feed"], params["adjustment"], params["timeframe"], params["limit"]) == (
        "sip",
        "split",
        "1Min",
        "10000",
    )
    assert "page_token" not in params and fake.requests[1].url.params["page_token"] == "100"


def test_rate_limits_are_retried_and_auth_errors_explained() -> None:
    fake = FakeAlpaca()
    fake.fail_next = [429, 503]
    with client(fake) as c:
        assert len(c.calendar(date(2024, 3, 25), date(2024, 3, 29))) == 4  # Good Friday is a holiday
    assert len(fake.requests) == 3
    with client(FakeAlpaca(), key="wrong") as c, pytest.raises(DataError, match="15 minutes"):
        c.calendar(date(2024, 3, 25), date(2024, 3, 29))


@pytest.fixture
def downloaded(tmp_path: Path):  # type: ignore[no-untyped-def]
    fake = FakeAlpaca(page_size=1000)
    store = DatasetStore(tmp_path / "datasets")
    with client(fake) as c:
        ds = store.download(
            c, name="test", symbols=["spy", "AAPL"], start=date(2024, 3, 25), end=date(2024, 4, 5),
            feed="sip", adjustment="split", today=date(2026, 1, 1),
        )  # fmt: skip
    return store, ds


def test_dataset_keeps_regular_sessions_only(downloaded) -> None:  # type: ignore[no-untyped-def]
    store, ds = downloaded
    assert ds.symbols == ["AAPL", "SPY"]
    assert {f["bars"] for f in ds.manifest["files"].values()} == {9 * 390}  # 9 sessions: Good Friday excluded
    assert ds.manifest["dropped_outside_regular_session"] == 2 * 9 * 2
    bars = list(ds.bars("SPY"))
    calendar = ds.calendar()
    assert all(calendar.is_open(b.start) for b in bars)
    assert HOLIDAY not in {b.start.date() for b in bars}
    assert [b.start for b in bars] == sorted({b.start for b in bars})
    assert store.names() == ["test"] and store.load("test").version == ds.version


def test_dataset_version_is_content_addressed(downloaded) -> None:  # type: ignore[no-untyped-def]
    _, ds = downloaded
    assert dataset_version(ds.manifest) == ds.version
    changed = {**ds.manifest, "adjustment": "raw"}
    assert dataset_version(changed) != ds.version
    renamed = {**ds.manifest, "name": "other", "created_at": "x"}
    assert dataset_version(renamed) == ds.version
    ds.verify()
    path = ds.path / ds.manifest["files"]["SPY"]["path"]
    with gzip.open(path, "at", encoding="utf-8") as handle:
        handle.write("tampered\n")
    with pytest.raises(DataError, match="hash"):
        ds.verify()


def test_download_refuses_unsafe_requests(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path)
    with client(FakeAlpaca()) as c:
        with pytest.raises(DataError, match="before today"):
            store.download(c, name="x", symbols=["SPY"], start=date(2024, 1, 2), end=date(2024, 1, 5),
                           feed="sip", adjustment="split", today=date(2024, 1, 5))  # fmt: skip
        with pytest.raises(DataError, match="invalid dataset name"):
            store.download(c, name="../escape", symbols=["SPY"], start=date(2024, 1, 2), end=date(2024, 1, 3),
                           feed="sip", adjustment="split", today=date(2026, 1, 1))  # fmt: skip


def test_early_closes_come_from_the_official_calendar(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path)
    with client(FakeAlpaca(page_size=2000)) as c:
        ds = store.download(c, name="july", symbols=["SPY"], start=date(2024, 7, 2), end=date(2024, 7, 5),
                            feed="sip", adjustment="split", today=date(2026, 1, 1))  # fmt: skip
    calendar = ds.calendar()
    session = calendar.session_on(EARLY_CLOSE)
    assert session is not None and session.close.astimezone(calendar.timezone).time() == time(13, 0)
    early_bars = [b for b in ds.bars("SPY") if b.start.date() == EARLY_CLOSE]
    assert len(early_bars) == 210  # 09:30 to 13:00
    assert calendar.session_on(INDEPENDENCE_DAY) is None  # weekday missing from the official calendar


async def test_historical_adapter_streams_in_time_order(downloaded) -> None:  # type: ignore[no-untyped-def]
    _, ds = downloaded
    adapter = HistoricalMarketDataAdapter(ds, start=date(2024, 3, 26), end=date(2024, 3, 27))
    await adapter.connect()
    await adapter.subscribe_bars(["SPY", "AAPL"], Timeframe.MIN_1)
    events = [e async for e in adapter.stream()]
    assert len(events) == 2 * 2 * 390 and all(isinstance(e, MarketBar) for e in events)
    keys = [(e.end, e.symbol) for e in events]
    assert keys == sorted(keys)
    assert events[0].start == datetime(2024, 3, 26, 13, 30, tzinfo=UTC)
    with pytest.raises(DataError):
        HistoricalMarketDataAdapter(ds, start=date(2024, 3, 1), end=date(2024, 3, 27))
    with pytest.raises(DataError):
        await adapter.subscribe_bars(["TSLA"], Timeframe.MIN_1)
    window = await adapter.get_historical_bars(
        "SPY", datetime(2024, 3, 26, 14, 0, tzinfo=UTC), datetime(2024, 3, 26, 14, 5, tzinfo=UTC)
    )
    assert len(window) == 5


def test_spread_calibration_recovers_the_quoted_spreads(downloaded) -> None:  # type: ignore[no-untyped-def]
    store, ds = downloaded
    fake = FakeAlpaca()
    with client(fake) as c:
        payload = calibrate_spreads(c, ds, days=3)
    write_spreads(ds, payload)
    assert load_spreads(store.load("test")) == payload
    spy, aapl = payload["symbols"]["SPY"], payload["symbols"]["AAPL"]
    assert spy["median_bps"] == pytest.approx(QUOTED_SPREAD_BPS["SPY"], rel=0.02)
    assert aapl["median_bps"] == pytest.approx(QUOTED_SPREAD_BPS["AAPL"], rel=0.02)
    assert spy["samples"] == 3 * len(DEFAULT_TIMES)  # every sampled moment lies inside the session
    first = fake.requests[0].url.params
    assert first["feed"] == "sip" and first["symbols"] in ("SPY", "AAPL")
    assert typical_spreads(payload) == {"AAPL": aapl["median_bps"], "SPY": spy["median_bps"]}
    assert sample_days([date(2024, 1, d) for d in range(2, 12)], 3) == [
        date(2024, 1, 2),
        date(2024, 1, 6),
        date(2024, 1, 11),
    ]


def test_spreads_measured_for_another_dataset_version_are_refused(downloaded) -> None:  # type: ignore[no-untyped-def]
    _, ds = downloaded
    with client(FakeAlpaca()) as c:
        payload = calibrate_spreads(c, ds, days=1, symbols=["SPY"])
    write_spreads(ds, {**payload, "dataset_version": "something-else"})
    with pytest.raises(DataError, match="recalibrate"):
        load_spreads(ds)
