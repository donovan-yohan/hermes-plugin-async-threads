import json
import re
from pathlib import Path

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
SIGNAL_DOC = ROOT / "docs" / "LOOP_SIGNAL_INGESTION.md"
SCHEMA = ROOT / "docs" / "schemas" / "async-thread-event-v1.schema.json"
FENCE_RE = re.compile(r"```json\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)

REQUIRED_SIGNAL_TYPES = {
    "github.pr.head_changed",
    "github.check_suite.completed",
    "github.review.submitted",
    "github.comment.created",
    "github.deployment_status.changed",
    "github.branch.updated",
    "relay.step.completed",
}


def _doc_text() -> str:
    return SIGNAL_DOC.read_text(encoding="utf-8")


def _examples() -> list[dict]:
    return [json.loads(body) for body in FENCE_RE.findall(_doc_text())]


def test_loop_signal_ingestion_examples_validate_against_event_schema():
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    examples = _examples()
    assert {example["eventType"] for example in examples} == REQUIRED_SIGNAL_TYPES
    for example in examples:
        validator.validate(example)
        assert example["version"] == "async-thread-event/v1"
        assert example["seriesKey"]
        assert example["workflowId"]
        assert example["correlation"]["correlationKey"]
        assert example["correlation"]["idempotencyKey"] == example["eventId"]
        assert example["correlation"]["signalKey"].startswith(example["eventType"] + ":")
        assert example["refs"]
        assert example["evidence"]["kind"]
        assert "trustedAction" in example["payload"]
        assert example["payload"]["trustReason"]


def test_loop_signal_ingestion_documents_trust_gate_and_stale_state():
    text = _doc_text()
    lower = text.lower()

    expected_phrases = (
        "visibility and automation are separate",
        "public text fields",
        "untrusted",
        "re-fetch live github/relay state",
        "duplicate",
        "stale-state",
        "pr head sha still equals",
        "relay session id, step id, attempt, and artifact revision",
    )
    missing = [phrase for phrase in expected_phrases if phrase not in lower]
    assert missing == []

    assert '"trustedAction": false' in text
    assert '"eventType": "github.comment.created"' in text
    assert '"reviewState": "APPROVED"' in text
    assert '"reviewState": "CHANGES_REQUESTED"' in text
    assert "changes-requested can advance brake/failure handling" in text


def test_loop_signal_ingestion_covers_issue_80_required_categories():
    text = _doc_text()

    required_categories = (
        "PR head changed",
        "Check suite completed",
        "Approval example",
        "Changes-requested example",
        "Public comment created",
        "Deployment status changed",
        "Branch updated",
        "Relay step completed",
        "Duplicate delivery and stale state",
    )
    missing_categories = [required for required in required_categories if required not in text]
    assert missing_categories == []

    expected_signals = (
        "github.check_suite.completed:example/repo:37:bbbb2222",
        "github.review.submitted:example/repo:37:bbbb2222:123",
        "github.review.submitted:example/repo:37:bbbb2222:124",
        "github.comment.created:example/repo:37:555",
        "relay.step.completed:run-42:review:1",
    )
    missing_signals = [signal for signal in expected_signals if signal not in text]
    assert missing_signals == []
