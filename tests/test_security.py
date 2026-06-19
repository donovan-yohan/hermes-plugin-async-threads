import hashlib
import hmac
import json
import time

import pytest

from async_threads.security import (
    EventValidationError,
    extract_envelope_fields,
    parse_json_body,
    validate_timestamp,
    verify_hmac_signature,
)


def test_signature_accepts_sha256_prefix():
    body = b'{"hello":"world"}'
    secret = "s3cr3t"
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_hmac_signature(body, secret, f"sha256={digest}") is True
    assert verify_hmac_signature(body, secret, "sha256=bad") is False


def test_extract_envelope_fields_requires_v1_shape():
    body = json.dumps(
        {
            "version": "async-thread-event/v1",
            "eventId": "evt1",
            "eventType": "relay.session.pr_opened",
            "producer": {"id": "relay"},
            "asyncThread": {"threadKey": "ath_123"},
            "summary": "ready",
        }
    ).encode()
    fields = extract_envelope_fields(parse_json_body(body))
    assert fields == {
        "event_id": "evt1",
        "event_type": "relay.session.pr_opened",
        "producer_id": "relay",
        "thread_key": "ath_123",
        "summary": "ready",
    }


def test_replay_window_rejects_old_timestamp():
    with pytest.raises(EventValidationError, match="outside replay window"):
        validate_timestamp(time.time() - 9999, now=time.time(), replay_window_seconds=60)
