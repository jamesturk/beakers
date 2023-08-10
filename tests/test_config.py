import json
from databeakers.config import load_config
from structlog import get_logger
from structlog.testing import capture_logs

log = get_logger()


def log5():
    log.debug("debug", n=1)
    log.info("info", n=2)
    log.warning("warning", n=3)
    log.error("error", n=4)
    log.critical("critical", n=5)


def test_log_level():
    load_config(log_level="debug", pipeline_path="noop")
    with capture_logs() as caplog:
        log5()
        assert len(caplog) == 5
        assert caplog[0] == {"event": "debug", "n": 1, "log_level": "debug"}
        assert caplog[1] == {"event": "info", "n": 2, "log_level": "info"}
        assert caplog[2] == {"event": "warning", "n": 3, "log_level": "warning"}
        assert caplog[3] == {"event": "error", "n": 4, "log_level": "error"}
        assert caplog[4] == {"event": "critical", "n": 5, "log_level": "critical"}


def test_log_file():
    load_config(log_file="test.log", pipeline_path="noop")
    log5()
    with open("test.log") as f:
        output = f.read()
    # starts at warning
    assert "level='warning' event='warning'" in output
    assert "level='error' event='error'" in output
    assert "level='critical' event='critical'" in output


def test_log_format():
    load_config(log_format="json", pipeline_path="noop", log_file="test.log")
    log5()
    with open("test.log") as f:
        output = f.read()
    lines = output.splitlines()
    assert len(lines) == 3
    line = json.loads(lines[0])
    assert line["event"] == "warning"
