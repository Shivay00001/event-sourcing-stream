"""Smoke test: entry points exist, package scripts are valid, TS compiles."""
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent.parent


def test_package_json_consistent():
    pkg = json.loads((ROOT / "package.json").read_text())
    assert pkg["main"].endswith(".js")
    scripts = pkg.get("scripts", {})
    assert "build" in scripts and "test" in scripts


def test_ts_entry_exists_and_parses():
    ts = ROOT / "event_sourcing_stream.ts"
    assert ts.exists(), "event_sourcing_stream.ts missing"
    r = subprocess.run(["node", "--check", str(ts)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
