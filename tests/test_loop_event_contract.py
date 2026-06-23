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
    "loop.waiting_for_approval",
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


def test_loop_contract_preserves_trust_boundary_language():
    text = _loop_doc_text().lower()

    assert "payload text is untrusted data" in text
    assert "do **not** turn" in text
    assert "into instructions" in text
    assert "correlation is routing and debugging metadata only" in text
