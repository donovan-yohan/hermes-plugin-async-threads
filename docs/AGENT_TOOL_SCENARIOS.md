# Agent-tool scenario harness

`async-threads` has a CI-runnable scenario harness for the agent-first UX. It grades workflow behavior, not microsecond performance. The matching reusable agent skill lives at [`../skills/async-thread-agent-tools/SKILL.md`](../skills/async-thread-agent-tools/SKILL.md).

Run it from the repo root:

```bash
uv run python scripts/ci/run_agent_tool_scenarios.py
uv run python scripts/ci/run_agent_tool_scenarios.py --json
```

The harness uses synthetic Hermes gateway/session objects and a fake Discord-like target adapter. It does not depend on live Discord, but it does import Hermes gateway modules; set `HERMES_AGENT_PATH=/path/to/hermes-agent` if the local checkout is not auto-detected.

## Scenarios

| Scenario | User journey | Main proof points |
| --- | --- | --- |
| `pr_review_lane` | User asks Hermes to watch a PR/review lane and report readiness/blockers here. | Model-tool listener creation from current conversation, origin correctness, HMAC signature validation, same-thread route, duplicate no-op, disallowed event rejection, missing-handle safe failure, prompt-injection boundary, rendered/diagnostic redaction, continuation-policy metadata. |
| `local_long_job` | User asks Hermes to watch a local long-running job. | Routine started/progress events coalesce into one digest; terminal finished event still delivers; event log records coalescing and terminal delivery. |
| `external_producer` | User asks for a webhook contract for another system. | Producer handoff returns file references, not raw secret; secret file exact text signs the event; direct delivery works; duplicate does not deliver twice; diagnostics stay redacted. |
| `debug_admin` | User asks what listeners exist and why events did/didn't arrive. | No-source/CLI-style setup fails with `source_unavailable`; scoped listing only shows this conversation; retire removes secret material; revoked handle rejects signed events without delivery. |

## Report shape

The JSON report has:

```json
{
  "ok": true,
  "summary": {
    "scenarioCount": 4,
    "passedScenarios": 4,
    "failedScenarios": 0,
    "checksPassed": 32,
    "checksTotal": 32
  },
  "scenarios": [
    {
      "name": "pr_review_lane",
      "journey": "PR review lane",
      "passed": true,
      "checks": {"route_scoping": true},
      "evidence": {}
    }
  ]
}
```

CI runs the harness in the Hermes gateway-dependent job. The pytest smoke test also parses the JSON shape so scenario regressions fail locally before PR review.
