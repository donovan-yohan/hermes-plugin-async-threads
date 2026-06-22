import json
import os
import subprocess
import sys


def test_agent_tool_scenario_harness_reports_issue_58_acceptance():
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    pieces = [os.getcwd()]
    hermes_path = env.get("HERMES_AGENT_PATH") or os.path.expanduser("~/.hermes/hermes-agent")
    if os.path.exists(os.path.join(hermes_path, "gateway", "config.py")):
        pieces.append(hermes_path)
    if existing:
        pieces.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(pieces)

    proc = subprocess.run(
        [sys.executable, "scripts/ci/run_agent_tool_scenarios.py", "--json"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )
    report = json.loads(proc.stdout)

    assert report["ok"] is True
    assert report["summary"]["scenarioCount"] == 4
    assert report["summary"]["passedScenarios"] == 4
    assert report["summary"]["failedScenarios"] == 0
    assert report["summary"]["checksPassed"] == report["summary"]["checksTotal"]
    assert report["summary"]["checksTotal"] >= 32
    by_name = {scenario["name"]: scenario for scenario in report["scenarios"]}
    assert set(by_name) == {"pr_review_lane", "local_long_job", "external_producer", "debug_admin"}
    assert by_name["pr_review_lane"]["checks"]["route_scoping"] is True
    assert by_name["pr_review_lane"]["checks"]["prompt_injection_boundary"] is True
    assert by_name["local_long_job"]["checks"]["routine_progress_coalesced"] is True
    assert by_name["local_long_job"]["checks"]["routine_events_not_individually_delivered"] is True
    assert by_name["local_long_job"]["checks"]["terminal_not_swallowed_by_coalescing"] is True
    assert by_name["external_producer"]["checks"]["handoff_secret_reference_only"] is True
    assert by_name["external_producer"]["checks"]["dedupe_replay_protection"] is True
    assert by_name["debug_admin"]["checks"]["no_source_fails_closed"] is True
    assert by_name["debug_admin"]["checks"]["no_source_does_not_guess_home_channel"] is True
    assert by_name["debug_admin"]["checks"]["valid_source_creates_exactly_one_listener"] is True
