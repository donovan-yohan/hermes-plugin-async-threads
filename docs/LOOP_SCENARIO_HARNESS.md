# Loop-signal scenario harness

`async-threads` has a CI-runnable harness that proves the assembled
feedback-controller loop path end to end without real GitHub credentials, live
Discord, or external secrets. It is the sample loop required by issue #83 and the
deterministic simulated dogfood fixture required by issue #84.

It grades loop behavior, not performance. It complements the agent-tool UX
harness in [`AGENT_TOOL_SCENARIOS.md`](AGENT_TOOL_SCENARIOS.md); that one proves
listener setup and single-event delivery, this one proves a multi-event loop.

Run it from the repo root:

```bash
uv run python scripts/ci/run_loop_scenarios.py
uv run python scripts/ci/run_loop_scenarios.py --json
uv run python scripts/ci/run_loop_scenarios.py --dogfood
```

Each scenario signs real `async-thread-event/v1` events, drives them through the
real `AsyncThreadsAdapter`, and delivers to a fake Discord-like gateway target.
It imports Hermes gateway modules; set `HERMES_AGENT_PATH=/path/to/hermes-agent`
if the local checkout is not auto-detected.

## Ownership boundary under test

The harness encodes the epic's split and proves ATH stays on its side of it:

| Owner | Responsibility | In the harness |
| --- | --- | --- |
| Dynamic Workflows | controller state, transitions, decisions | `SimulatedLoopController` owns state and the only `perform_merge` capability |
| Relay | bounded agent/runtime steps, artifacts | `step.backend = "relay"` evidence handles only; no transcripts |
| ATH (this plugin) | authenticate, de-dupe, record, render, wake | the real `AsyncThreadsAdapter` |

ATH never advances the loop. The controller reads ATH-recorded state, verifies
live external state (current head), and only then decides to advance, reject as
stale, merge, or halt. The fake gateway target can only `send`/`handle_message`;
it has no merge/deploy capability, so ATH structurally cannot complete work.

## Scenarios

| Scenario | Loop journey | Main proof points |
| --- | --- | --- |
| `loop_converges` | start → wait → GitHub check signal → resume → Relay step → approval → converge → evidence | full happy path; ATH records the producer-declared stage (does not invent it); merge happened only via the controller after a fresh head-matched approval, and the terminal `converged` event is emitted only when the controller advances; signature enforcement (wrong key → 401); prompt-injection boundary (framing precedes the injected text); secrets/raw logs stripped from rendered messages; debug-tail redaction (opted-in tail shown but secrets removed); bounded-continuation metadata plus a fail-closed listener that refuses an unbounded continuation |
| `duplicate_and_stale_signal` | duplicate check replay + late check for a superseded head | exact replay is de-duped (one delivery, recorded `duplicate`); head change invalidates the wait; a stale-head result is delivered for visibility (its secret-bearing summary redacted) but the controller refuses to advance and ATH records it as `stale`, never `passed`; disallowed event type rejected |
| `stale_approval_then_fresh` | public comment + stale approval cannot merge; fresh maintainer approval at the new head converges | public comment is visible but not automation-eligible; a head-mismatched approval is recorded as stale and not applied; ATH transported both inert signals for visibility yet neither advanced the merge; only a trusted maintainer decision whose head matched advanced the merge (single merge) |
| `approval_denied_halts` | maintainer denies the merge → loop halts | a trusted deny decision is delivered for visibility; the controller (not ATH) trips the brake and halts with no merge; the halt is terminal, records `cancelled`, and carries a suggested human next step |
| `wait_timeout_halts` | bounded wait expires → one timeout → halt | exactly one `loop.wait_timeout` (no cron-style heartbeats); only one `loop.waiting_for_event`; controller chose to halt; halt is terminal, records `cancelled`, and carries a suggested human next step |

## Report shape

`--json` prints a machine-readable report. `ok` is true only when all scenarios
and the dogfood guarantees pass:

```json
{
  "ok": true,
  "summary": {
    "scenarioCount": 5,
    "passedScenarios": 5,
    "failedScenarios": 0,
    "checksPassed": 52,
    "checksTotal": 52,
    "dogfoodOk": true
  },
  "scenarios": [{"name": "loop_converges", "passed": true, "checks": {}, "evidence": {}}],
  "dogfood": {"loopShape": {}, "evidenceReply": "...", "finalizer": {}, "guarantees": {}},
  "acceptanceMap": {}
}
```

`acceptanceMap` is an honest map from each #83/#84/#76 acceptance criterion to the
scenario, check, or dogfood field that proves it. The pytest smoke test
[`tests/test_loop_scenarios.py`](../tests/test_loop_scenarios.py) parses the
report and fails locally before PR review if any loop guarantee regresses.

The `--dogfood` evidence bundle is documented in
[`LOOP_DOGFOOD.md`](LOOP_DOGFOOD.md).
