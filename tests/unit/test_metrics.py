import logging
import tracemalloc

import pytest

from recon.service.metrics import (KeyValueFormatter, RunMetrics, configure_stderr_logging, log_event, logger,
                                   run_log, utc_iso)


def fake_clock(*ticks):
    it = iter(ticks)
    return lambda: next(it)


def test_stage_durations_from_clock():
    m = RunMetrics(clock=fake_clock(0.0, 1.0, 1.5, 2.0, 2.25, 3.0))
    m.start()
    with m.stage("bronze") as s:
        s.record_side("SOURCE", rows_in=3, rows_out=2, rows_rejected=1, blank_lines=0)
    with m.stage("silver"):
        pass
    m.stop()
    d = m.as_dict()
    assert [(s["stage"], s["duration_ms"]) for s in d["stages"]] == [("bronze", 500.0), ("silver", 250.0)]
    assert d["total_duration_ms"] == 3000.0
    assert d["stages"][0]["sides"] == {"SOURCE": {"rows_in": 3, "rows_out": 2, "rows_rejected": 1,
                                                  "blank_lines": 0}}
    assert d["peak_memory_bytes"] is None and d["profile_memory"] is False


def test_stage_duration_recorded_when_stage_raises():
    m = RunMetrics(clock=fake_clock(0.0, 0.002))
    with pytest.raises(RuntimeError):
        with m.stage("gold"):
            raise RuntimeError("boom")
    assert m.stage_named("gold").duration_ms == 2.0


def test_profile_memory_tracks_peak_and_stops_tracing():
    assert not tracemalloc.is_tracing()
    m = RunMetrics(profile_memory=True)
    m.start()
    with m.stage("bronze"):
        blob = [bytes(1000) for _ in range(100)]
    m.stop()
    del blob
    assert m.stage_named("bronze").peak_memory_bytes >= 100_000
    assert m.peak_memory_bytes == m.stage_named("bronze").peak_memory_bytes
    assert not tracemalloc.is_tracing()


def _record(**extra):
    record = logging.LogRecord("recon", logging.INFO, __file__, 1, "msg", None, None)
    record.created = 0.0
    for k, v in extra.items():
        setattr(record, k, v)
    return record


def test_key_value_format_and_quoting():
    line = KeyValueFormatter().format(_record(run_id="r-a1", stage="bronze", event="side_rows",
                                              fields={"side": "SOURCE", "rows_in": 3, "detail": 'a b="c"',
                                                      "empty": "", "none": None}))
    assert line == ('ts=1970-01-01T00:00:00Z level=INFO run_id=r-a1 stage=bronze event=side_rows side=SOURCE '
                    'rows_in=3 detail="a b=\\"c\\"" empty="" none=-')


def test_key_value_format_defaults_and_newlines_stay_on_one_line():
    line = KeyValueFormatter().format(_record(fields={"detail": "line1\nline2"}))
    assert line == 'ts=1970-01-01T00:00:00Z level=INFO run_id=- stage=- event=msg detail="line1\\nline2"'


def test_run_log_writes_lf_lines_and_detaches(tmp_path):
    path = tmp_path / "r" / "run.log"
    with run_log(path):
        log_event("run_started", run_id="r-a1", attempt=1)
        log_event("run_finished", run_id="r-a1", level=logging.ERROR, status="FAILED")
    log_event("after", run_id="r-a1")  # not written: the handler is detached
    data = path.read_bytes()
    assert b"\r" not in data
    lines = data.decode("utf-8").splitlines()
    assert len(lines) == 2
    assert lines[0].endswith("level=INFO run_id=r-a1 stage=- event=run_started attempt=1")
    assert "level=ERROR" in lines[1] and lines[1].endswith("event=run_finished status=FAILED")


def test_configure_stderr_logging_is_idempotent(capsys):
    configure_stderr_logging()
    configure_stderr_logging()
    try:
        log_event("hello", run_id="x")
        err = capsys.readouterr().err
        assert err.count("event=hello") == 1
    finally:
        for h in list(logger.handlers):
            logger.removeHandler(h)


def test_utc_iso_shape():
    assert len(utc_iso()) == 20 and utc_iso().endswith("Z")
