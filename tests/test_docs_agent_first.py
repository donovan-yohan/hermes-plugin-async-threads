import re
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
    assert "The default public UX is **agent-first**" in text


def test_quickstart_distinguishes_agent_happy_path_from_manual_admin():
    text = _read("docs/QUICKSTART.md")

    happy = text.index("## Agent happy path")
    signed_event = text.index("## Send a signed demo event")
    manual = text.index("Manual `/ath` commands are the equivalent admin/debug surface")
    assert happy < signed_event < manual
    assert "watch this demo async job and report back here when it finishes" in text
    assert "model-facing ATH tools" in text
    assert "getting-started-agent-first.png" in text
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


def test_docs_cover_kanban_source_binding_dogfood_without_cron_or_raw_payloads():
    readme = _read("README.md")
    problem = _read("docs/PROBLEM_STATEMENT.md")
    recipes = _read("docs/BRIDGE_RECIPES.md")
    contract = _read("docs/EVENT_CONTRACT.md")

    for text in (readme, problem, recipes, contract):
        assert "ath_mg3BQeDs15Gm4DnF" in text
        assert "ath-kanban-bridge" in text
        assert "kanban.task.blocked" in text
        assert "kanban.task.completed" in text
        assert "kanban.task.crashed" in text
        assert "kanban.task.gave_up" in text
        assert "kanban.task.timed_out" in text
        assert "kanban.task.ready_for_review" in text

    assert "/ath bind-source kanban ath_mg3BQeDs15Gm4DnF --board ath" in recipes
    assert "ath_create_source_binding" in readme
    assert "ath_dry_run_source_binding" in readme
    assert "would_emit" in recipes
    assert "suppressed" in recipes
    assert "would_coalesce" in recipes
    assert "invalid_binding" in recipes
    assert "source_binding_runner_enabled" in recipes
    assert "not a Hermes cron job" in recipes
    assert "emergency fallback" in problem
    assert "raw task comments" in contract
    assert "prompt-like instructions" in contract


def test_source_binding_docs_do_not_model_raw_comments_logs_or_secrets_as_instructions():
    checked = {
        "README.md": _read("README.md"),
        "docs/PROBLEM_STATEMENT.md": _read("docs/PROBLEM_STATEMENT.md"),
        "docs/BRIDGE_RECIPES.md": _read("docs/BRIDGE_RECIPES.md"),
        "docs/EVENT_CONTRACT.md": _read("docs/EVENT_CONTRACT.md"),
    }

    forbidden = [
        "ignore previous instructions",
        '"rawComments"',
        '"rawLogs"',
        '"secret"',
        '"secretFile"',
        "-----BEGIN",
    ]
    offenders = []
    for path, text in checked.items():
        for needle in forbidden:
            if needle in text:
                offenders.append(f"{path}: {needle}")
    assert offenders == []


def test_public_docs_do_not_overclaim_hard_bounded_continuations():
    checked = {
        "README.md": _read("README.md"),
        "docs/PROBLEM_STATEMENT.md": _read("docs/PROBLEM_STATEMENT.md"),
        "docs/EVENT_CONTRACT.md": _read("docs/EVENT_CONTRACT.md"),
        "docs/spikes/hermes-async-thread-feasibility.md": _read("docs/spikes/hermes-async-thread-feasibility.md"),
        "skills/async-thread-agent-tools/SKILL.md": _read("skills/async-thread-agent-tools/SKILL.md"),
    }

    forbidden = [
        r"\bbounded agent continuation\b",
        r"\bbounded agent run\b",
        r"\bbounded prompt\b",
        r"\bbounded text prompt\b",
        r"\bbounded summarization\b",
        r"\bbounded_continuation_policy\b",
        r"\bpolicy enforces max turns/toolsets/model\b",
    ]
    offenders = []
    for path, text in checked.items():
        for pattern in forbidden:
            if re.search(pattern, text, flags=re.IGNORECASE):
                offenders.append(f"{path}: {pattern}")
    assert offenders == []

    problem = checked["docs/PROBLEM_STATEMENT.md"]
    assert "Current Hermes core does not yet expose plugin-local hard caps" in problem
    assert "strict hard-bound requirements must use fail-closed mode" in problem
    assert "explicit continuation policy metadata" in problem
