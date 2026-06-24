import json
import os
import subprocess
import sys


SECRET_SENTINELS = [
    "ghp_" + ("a" * 36),
    "github_pat_" + ("A" * 22) + "_" + ("B" * 59),
    "sk-proj-" + ("c" * 40),
    "xoxb-" + "123456789012-123456789012-abcdefghijklmnopqrstuv",
    "agent:main:discord:channel:c:t",
]


def _run_harness():
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
        [sys.executable, "scripts/ci/run_loop_scenarios.py", "--json"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )
    return json.loads(proc.stdout)


def test_loop_scenario_harness_passes_issue_83_acceptance():
    report = _run_harness()

    assert report["ok"] is True
    summary = report["summary"]
    assert summary["scenarioCount"] == 5
    assert summary["passedScenarios"] == 5
    assert summary["failedScenarios"] == 0
    assert summary["checksPassed"] == summary["checksTotal"]
    assert summary["checksTotal"] >= 45
    assert summary["dogfoodOk"] is True

    by_name = {scenario["name"]: scenario for scenario in report["scenarios"]}
    assert set(by_name) == {
        "loop_converges",
        "duplicate_and_stale_signal",
        "stale_approval_then_fresh",
        "approval_denied_halts",
        "wait_timeout_halts",
    }

    # start -> wait -> signal -> resume -> approval -> converge -> evidence
    converges = by_name["loop_converges"]["checks"]
    assert converges["fresh_signal_advances_controller"] is True
    assert converges["controller_owned_the_merge_decision"] is True
    assert converges["ath_records_producer_declared_stage"] is True
    assert converges["prompt_injection_boundary"] is True
    assert converges["no_secret_in_rendered_messages"] is True
    assert converges["no_raw_logs_in_rendered_messages"] is True
    assert converges["debug_tail_redacted"] is True
    assert converges["signature_required"] is True
    assert converges["continuation_policy_bounded_metadata"] is True
    assert converges["fail_closed_blocks_unbounded_continuation"] is True
    assert converges["terminal_event_detected"] is True

    # duplicate replay + stale-event handling
    dup = by_name["duplicate_and_stale_signal"]["checks"]
    assert dup["dedupe_replay_protection"] is True
    assert dup["controller_rejects_stale_signal"] is True
    assert dup["ath_recorded_stale_not_passed"] is True
    assert dup["disallowed_event_rejected"] is True
    assert dup["redaction_on_stale_render_path"] is True

    # maintainer gate + stale approval protection
    stale = by_name["stale_approval_then_fresh"]["checks"]
    assert stale["public_comment_not_actionable"] is True
    assert stale["stale_approval_not_applied"] is True
    assert stale["ath_transported_inert_signals"] is True
    assert stale["maintainer_gate_single_merge"] is True
    assert stale["fresh_approval_converges"] is True

    # maintainer deny halts the loop (approve/deny AC)
    deny = by_name["approval_denied_halts"]["checks"]
    assert deny["deny_delivered_for_visibility"] is True
    assert deny["deny_blocks_merge"] is True
    assert deny["halt_after_deny_terminal"] is True

    # watchdog/timeout without cron spam + halt
    timeout = by_name["wait_timeout_halts"]["checks"]
    assert timeout["single_timeout_emitted"] is True
    assert timeout["no_polling_spam"] is True
    assert timeout["halt_is_terminal"] is True
    assert timeout["halt_carries_suggested_next_step"] is True


def test_dogfood_evidence_bundle_is_public_safe_and_complete():
    report = _run_harness()
    dogfood = report["dogfood"]

    assert dogfood["loopShape"] == {
        "controllerOwner": "dynamic-workflows",
        "runtimeOwner": "relay",
        "signalVisibilityOwner": "async-threads",
    }
    assert all(bool(value) for value in dogfood["guarantees"].values())

    # #84: evidence comment carries event ids, run id, step id, correlation key, trace refs.
    reply = dogfood["evidenceReply"]
    assert dogfood["runId"] in reply
    assert "approval:merge:" in reply
    assert "/ath trace" in reply
    assert dogfood["mergeCommit"] and dogfood["mergeCommit"] in reply
    assert any(item["stepId"] == "merge" for item in dogfood["events"])
    assert any(item["stepId"] == "build" for item in dogfood["events"])

    # public-safe: no secrets, no raw logs, anywhere in the bundle.
    rendered_bundle = json.dumps(dogfood, sort_keys=True)
    for sentinel in SECRET_SENTINELS:
        assert sentinel not in rendered_bundle
    assert "TRACE worker pid=" not in rendered_bundle

    # Dynamic Workflows finalizer (not ATH) retired the listener at loop end.
    assert dogfood["finalizer"]["action"] == "ath.listener.retire"
    assert dogfood["finalizer"]["ok"] is True
    assert dogfood["finalizer"]["listenerEnabledAfter"] is False


def test_acceptance_map_links_criteria_to_proof():
    report = _run_harness()
    acceptance = report["acceptanceMap"]

    # Every mapped criterion names a concrete scenario/check/bundle field.
    assert acceptance
    assert all(isinstance(key, str) and isinstance(value, str) and value for key, value in acceptance.items())
    joined = " ".join(acceptance.keys())
    assert "#83" in joined and "#84" in joined and "#76" in joined
