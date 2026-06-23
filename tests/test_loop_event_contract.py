import json
import re
from pathlib import Path

from jsonschema import Draft202012Validator

from async_threads.security import extract_envelope_fields

ROOT = Path(__file__).resolve().parents[1]
LOOP_DOC = ROOT / "docs" / "LOOP_EVENTS.md"
SCHEMA = ROOT / "docs" / "schemas" / "async-thread-event-v1.schema.json"
FENCE_RE = re.compile(r"```json\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)

LOOP_EVENT_TYPES = {
    "loop.started",
    "loop.sensor_failed",
    "loop.step_started",
    "loop.step_completed",
    "loop.waiting_for_event",
    "loop.wait_timeout",
    "loop.watchdog_fired",
    "loop.waiting_for_approval",
    "loop.approval_granted",
    "loop.approval_denied",
    "loop.approval_stale",
    "loop.stalled",
    "loop.halted",
    "loop.converged",
}


def _loop_doc_text() -> str:
    return LOOP_DOC.read_text(encoding="utf-8")


def _event_examples() -> list[dict]:
    examples = []
    for body in FENCE_RE.findall(_loop_doc_text()):
        data = json.loads(body)
        if isinstance(data, dict) and data.get("version") == "async-thread-event/v1":
            examples.append(data)
    return examples


def test_loop_event_contract_names_every_public_event_type():
    text = _loop_doc_text()

    for event_type in LOOP_EVENT_TYPES:
        assert f"`{event_type}`" in text


def test_loop_event_examples_validate_against_async_thread_schema():
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    examples = _event_examples()

    assert examples
    for example in examples:
        assert not list(validator.iter_errors(example))
        extract_envelope_fields(example)


def test_loop_event_examples_cover_correlation_evidence_and_next_signal_fields():
    examples = _event_examples()
    joined = json.dumps(examples, sort_keys=True)

    for required in (
        "runId",
        "stepId",
        "correlationKey",
        "idempotencyKey",
        "signalKey",
        "refs",
        "evidence",
        "nextExpectedSignal",
    ):
        assert required in joined


def test_loop_timeout_contract_rejects_cron_spam_and_covers_stale_timeouts():
    text = _loop_doc_text()
    examples = {example["eventType"]: example for example in _event_examples()}
    waiting = examples["loop.waiting_for_event"]
    timeout = examples["loop.wait_timeout"]
    watchdog = examples["loop.watchdog_fired"]

    assert waiting["nextExpectedSignal"]["waitId"] == "wait-checks-run-42-head-a1b2c3d4"
    assert waiting["nextExpectedSignal"]["signalKey"] == "github.check_suite.completed:example/repo:86:a1b2c3d4"
    assert waiting["nextExpectedSignal"]["deadlineAt"] == "2026-06-23T17:30:00Z"
    assert waiting["nextExpectedSignal"]["onTimeoutEventType"] == "loop.wait_timeout"
    assert timeout["payload"]["waitId"] == waiting["nextExpectedSignal"]["waitId"]
    assert timeout["payload"]["expectedSignalKey"] == waiting["nextExpectedSignal"]["signalKey"]
    assert timeout["payload"]["deadlineAt"] == waiting["nextExpectedSignal"]["deadlineAt"]
    assert timeout["correlation"]["idempotencyKey"] == timeout["eventId"]
    assert timeout["payload"]["stale"] is False
    assert watchdog["evidence"]["status"] == "stale"
    assert watchdog["payload"]["stale"] is True
    assert watchdog["refs"]["expectedHeadSha"] == "a1b2c3d4"
    assert watchdog["refs"]["headSha"] == "bbbb2222"
    assert "ATH does not poll this deadline" in text
    assert "must not become a scheduler" in text
    assert "cron-style polling spam" in text


def test_loop_approval_contract_covers_stale_protection_and_idempotent_decisions():
    text = _loop_doc_text()
    examples = [example for example in _event_examples() if str(example.get("eventType", "")).startswith("loop.approval_")]
    request = next(example for example in _event_examples() if example["eventType"] == "loop.waiting_for_approval")

    assert request["nextExpectedSignal"]["approvalId"] == "approval-merge-run-42-head-a1b2c3d4"
    assert request["nextExpectedSignal"]["expiresAt"] == "2026-06-23T18:25:00Z"
    assert "merge" in request["correlation"]["correlationKey"]
    assert request["refs"]["headSha"] == "a1b2c3d4"
    assert {example["eventType"] for example in examples} == {"loop.approval_granted", "loop.approval_denied", "loop.approval_stale"}
    for example in examples:
        assert example["payload"]["approvalId"] == example["refs"]["approvalId"]
        assert example["correlation"]["idempotencyKey"] == example["eventId"]
        assert example["correlation"]["correlationKey"] == "approval:merge:example/repo:86:a1b2c3d4:run-42"
        if example["eventType"] in {"loop.approval_granted", "loop.approval_denied"}:
            assert example["payload"]["trustedAction"] is True
            assert example["payload"]["trustedActor"] == "maintainer-a"
    stale = next(example for example in examples if example["eventType"] == "loop.approval_stale")
    assert stale["payload"]["trustedAction"] is False
    assert stale["evidence"]["status"] == "stale"
    assert stale["refs"]["expectedHeadSha"] == "a1b2c3d4"
    assert stale["refs"]["headSha"] == "bbbb2222"
    assert "public comments cannot approve by text alone" in text
    assert "ATH must not merge, deploy, delete, or execute destructive operations" in text


def test_loop_contract_preserves_trust_boundary_language():
    text = _loop_doc_text().lower()

    assert "payload text is untrusted data" in text
    assert "do **not** turn" in text
    assert "into instructions" in text
    assert "correlation is routing and debugging metadata only" in text
