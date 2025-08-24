#!/usr/bin/env python3
"""Basic smoke tests for TG Analyzer.

Checks required files exist and simulates starting the worker
process with stubbed dependencies. Also verifies failure scenarios
when configuration or environment variables are missing.
"""
from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

import psutil

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = ["worker.py", "dashboard_app.py", "Dockerfile", "docker-compose.yml"]

# Minimal copy of start_worker() from dashboard_app to avoid heavy imports.
PID_FILE = Path("runtime") / "worker.pid"


def start_worker() -> None:
    """Spawn worker.py just like dashboard_app.start_worker."""
    with open("worker.log", "ab") as log:
        proc = subprocess.Popen([os.sys.executable, "worker.py"], stdout=log, stderr=subprocess.STDOUT)
    PID_FILE.write_text(str(proc.pid))


def assert_required_files() -> None:
    missing = [name for name in REQUIRED if not (ROOT / name).exists()]
    assert not missing, f"Missing files: {', '.join(missing)}"


def _write_stub_worker(path: Path) -> None:
    code = (
        "import os, time, pathlib\n"
        "assert pathlib.Path('config.yaml').exists()\n"
        "assert os.getenv('API_ID') and os.getenv('API_HASH')\n"
        "time.sleep(2)\n"
    )
    path.write_text(code, encoding="utf-8")


def _patch_popen(stub: Path, *, fail_on_cfg=False, fail_on_env=False):
    original = subprocess.Popen

    def patched(cmd, *args, **kwargs):
        if isinstance(cmd, list) and len(cmd) >= 2 and cmd[1] == "worker.py":
            if fail_on_cfg and not (ROOT / "config.yaml").exists():
                raise FileNotFoundError("config.yaml missing")
            if fail_on_env and (not os.getenv("API_ID") or not os.getenv("API_HASH")):
                raise RuntimeError("missing API credentials")
            cmd = [os.sys.executable, str(stub)]
        return original(cmd, *args, **kwargs)

    return original, patched


def _cleanup(pid_file: Path) -> None:
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text())
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
        pid_file.unlink(missing_ok=True)


def positive_scenario() -> None:
    os.environ.setdefault("API_ID", "1")
    os.environ.setdefault("API_HASH", "1")

    stub = ROOT / "scripts" / "_stub_worker.py"
    _write_stub_worker(stub)

    original, patched = _patch_popen(stub)
    subprocess.Popen = patched
    try:
        start_worker()
        pid = int(PID_FILE.read_text())
        time.sleep(1.2)
        assert psutil.pid_exists(pid), "worker process terminated too quickly"
    finally:
        subprocess.Popen = original
        _cleanup(PID_FILE)
        stub.unlink(missing_ok=True)


def negative_missing_config() -> None:
    cfg = ROOT / "config.yaml"
    backup = cfg.with_suffix(".smoke_bak")
    cfg.rename(backup)

    stub = ROOT / "scripts" / "_stub_worker.py"
    _write_stub_worker(stub)

    original, patched = _patch_popen(stub, fail_on_cfg=True)
    subprocess.Popen = patched
    try:
        try:
            start_worker()
        except Exception:
            pass
        assert not PID_FILE.exists(), "PID file created despite missing config"
    finally:
        subprocess.Popen = original
        backup.rename(cfg)
        stub.unlink(missing_ok=True)


def negative_missing_env() -> None:
    stub = ROOT / "scripts" / "_stub_worker.py"
    _write_stub_worker(stub)

    original, patched = _patch_popen(stub, fail_on_env=True)
    subprocess.Popen = patched
    os.environ.pop("API_ID", None)
    os.environ.pop("API_HASH", None)
    try:
        try:
            start_worker()
        except Exception:
            pass
        assert not PID_FILE.exists(), "PID file created despite missing env vars"
    finally:
        subprocess.Popen = original
        stub.unlink(missing_ok=True)


def main() -> None:
    assert_required_files()
    positive_scenario()
    negative_missing_config()
    negative_missing_env()
    print("Smoke test completed successfully.")


if __name__ == "__main__":
    main()
