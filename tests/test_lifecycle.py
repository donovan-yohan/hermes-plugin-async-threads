from async_threads.lifecycle import (
    DEFAULT_TERMINAL_EVENT_PATTERNS,
    DEFAULT_TERMINAL_STAGES,
    LifecyclePolicy,
    is_terminal_event,
)


def test_lifecycle_policy_preserves_explicit_empty_terminal_lists():
    policy = LifecyclePolicy.from_mapping(
        {
            "terminal_event_types": [],
            "terminal_stages": [],
        }
    )

    assert policy.terminal_event_types == ()
    assert policy.terminal_stages == ()
    assert not is_terminal_event({"stage": "released"}, {"event_type": "demo.goal.finished"}, policy)


def test_lifecycle_policy_defaults_only_when_keys_missing():
    policy = LifecyclePolicy.from_mapping({})

    assert policy.terminal_event_types == DEFAULT_TERMINAL_EVENT_PATTERNS
    assert policy.terminal_stages == DEFAULT_TERMINAL_STAGES
    assert is_terminal_event({}, {"event_type": "demo.goal.finished"}, policy)
