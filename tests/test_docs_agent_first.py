from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_readme_teaches_agent_happy_path_before_manual_ath():
    text = _read("README.md")

    normal = text.index("Normal user ask:")
    manual = text.index("Manual `/ath listen` remains available")
    assert normal < manual
    assert "watch this demo job and report back here when it finishes" in text
    assert "call `ath_create_listener`" in text
    assert "call `ath_generate_producer_handoff`" in text
    assert "gateway commands for manual admin/debug" in text
    assert "Current Hermes core does not expose plugin-local hard caps" in text
    assert "fail-closed mode" in text


def test_quickstart_distinguishes_agent_happy_path_from_manual_admin():
    text = _read("docs/QUICKSTART.md")

    happy = text.index("## Agent happy path")
    manual = text.index("## Manual `/ath` path")
    signed_event = text.index("## Send a signed demo event")
    assert happy < manual < signed_event
    assert "watch this demo async job and report back here when it finishes" in text
    assert "model-facing ATH tools" in text
    assert "admin/debug/power users" in text
    assert "source contexts fail closed" in text


def test_agent_skill_contains_safe_defaults_and_antipatterns():
    text = _read("skills/async-thread-agent-tools/SKILL.md")

    required = [
        "Use ATH when",
        "Do not use ATH when",
        "Happy path",
        "ath_create_listener",
        "ath_generate_producer_handoff",
        "ATH_SECRET_FILE",
        "Do not create cron polling loops",
        "Do not hardcode Discord/Telegram/Slack ids",
        "Do not let producers post directly to Discord, Telegram, Slack, or any other chat platform",
        "Do not dump raw JSON/logs/transcripts into the agent prompt",
        "Do not start unbounded agent continuations",
        "Do not leak HMAC secrets",
        "coreEnforced: false",
        "fail_closed_without_core_bounds: true",
    ]
    missing = [item for item in required if item not in text]
    assert missing == []


def test_bridge_recipes_include_complete_natural_language_to_event_workflow():
    text = _read("docs/BRIDGE_RECIPES.md")

    assert "## Complete agent-first workflow" in text
    assert "watch this PR review lane and report back here" in text
    assert '"producer_hint": "repo-review"' in text
    assert '"eventType": "repo-review.ready"' in text
    assert "Do not paste the raw secret" in text
    assert "Manual `/ath` admin/debug path" in text
