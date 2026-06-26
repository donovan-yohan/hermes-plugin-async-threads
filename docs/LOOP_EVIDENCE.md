# Loop evidence: coordinated controller + runtime + signals

This is the integration evidence path for a coordinated loop using ATH as the signal/visibility surface, an external agent runtime as the bounded execution layer, and Dynamic Workflows as the controller owner.

A fully live verification needs a disposable PR, a real gateway thread, and real maintainer credentials, which CI must not carry. So this repo ships a **deterministic simulated evidence fixture** that runs in CI today, plus a **live dry-run checklist** for what to additionally capture against a throwaway PR/thread when one is available. The simulated fixture is emitted by:

```bash
uv run python scripts/ci/run_loop_scenarios.py --dogfood
```

The flag name is kept for compatibility with earlier automation; the generated artifact is public-safe release evidence.

## Coordinated loop shape

```text
Dynamic Workflows (controller)        Agent runtime              ATH (signals + visibility)
-----------------------------         -------------              ---------------------------
decide setpoint, transitions   --->   run bounded step    --->   sign loop.* + signal events
own run/step/correlation ids          emit evidence handle       authenticate, de-dupe, record
verify live state before acting <---  artifact revision   <---   render compact status, wake thread
```

ATH owns "wake up when the world changes and tell humans what happened." It does **not** own "what should happen next" (Dynamic Workflows) or "run the agent step" (the external runtime). Each loop event is a signed `async-thread-event/v1` envelope; see [`LOOP_EVENTS.md`](LOOP_EVENTS.md) for the event shapes and [`LOOP_SIGNAL_INGESTION.md`](LOOP_SIGNAL_INGESTION.md) for how GitHub/runtime facts become signals.

One trusted bridge producer signs every event for a loop listener: lifecycle events from Dynamic Workflows plus external signals it translates. The semantic owner is carried in `loop`, `step.backend`, and `correlation` — not in the envelope `producer.id`. Upstream payload text stays untrusted even though the bridge is trusted to sign.

## What the simulated fixture proves

The converging-loop fixture emits one signed event per transition and produces a public-safe evidence bundle. The bundle is the "evidence reply" a controller would post back into the human thread when the loop converges:

```text
✅ Loop converged — release-readiness (run-301)
PR example/repo#86 @ aaaa1111 → merged merge-d13af0c4
Owners: controller=dynamic-workflows · runtime=relay · signals+visibility=async-threads
Signals: check_suite#9001 passed · relay build passed · maintainer-a approved (head-matched)
Steps: build (relay) · merge (github)
Evidence: actions/runs/9001 · relay/build-1 · pull/86
Correlation: approval:merge:example/repo:86:aaaa1111:run-301
Trace: eventIds run-301-started … run-301-converged (/ath trace <eventId>)
ATH did not own loop state: every advance/merge was a controller decision after live-state verification.
```

The structured bundle behind that reply carries event ids, run id, step ids, correlation keys, signal keys, and evidence handles:

```json
{
  "loopShape": {"controllerOwner": "dynamic-workflows", "runtimeOwner": "relay", "signalVisibilityOwner": "async-threads"},
  "runId": "run-301",
  "specId": "release-readiness",
  "workflowId": "loop:release-readiness:run-301",
  "mergeCommit": "merge-d13af0c4",
  "events": [
    {"eventId": "run-301-started", "eventType": "loop.started", "stepId": "", "correlationKey": "release-readiness:run-301:head-aaaa1111", "signalKey": "loop.started:release-readiness:run-301", "evidenceUrl": "https://example.invalid/loops/run-301", "athOutcome": "accepted"},
    {"eventId": "github-pr-86-check-9001-completed", "eventType": "github.check_suite.completed", "stepId": "", "correlationKey": "release-readiness:run-301:checks:example/repo:86:aaaa1111", "signalKey": "github.check_suite.completed:example/repo:86:aaaa1111", "evidenceUrl": "https://example.invalid/repo/actions/runs/9001", "athOutcome": "accepted"},
    {"eventId": "run-301-step-build-completed", "eventType": "loop.step_completed", "stepId": "build", "correlationKey": "release-readiness:run-301:build:head-aaaa1111", "signalKey": "relay.step.completed:run-301:build:1", "evidenceUrl": "https://example.invalid/relay/build-1", "athOutcome": "accepted"},
    {"eventId": "approval-merge-run-301-aaaa1111-approved", "eventType": "loop.approval_granted", "stepId": "merge", "correlationKey": "approval:merge:example/repo:86:aaaa1111:run-301", "signalKey": "approval.merge.decided:example/repo:86:aaaa1111", "evidenceUrl": "https://example.invalid/repo/pull/86#issuecomment-1", "athOutcome": "accepted"},
    {"eventId": "run-301-converged", "eventType": "loop.converged", "stepId": "merge", "correlationKey": "release-readiness:run-301:converged:aaaa1111", "signalKey": "loop.converged:release-readiness:run-301", "evidenceUrl": "https://example.invalid/repo/pull/86", "athOutcome": "accepted"}
  ],
  "finalizer": {"action": "ath.listener.retire", "ownerEnforced": true, "ok": true, "listenerEnabledAfter": false},
  "guarantees": {
    "athDidNotOwnStateMachine": true,
    "evidenceReplyHasRequiredIds": true,
    "noSecretsInEvidenceReply": true,
    "noRawLogsInEvidenceReply": true,
    "evidenceReplyCompact": true,
    "finalizerRetiredListener": true
  }
}
```

The example ids, correlation keys, and `mergeCommit` above are generated output. Re-run the scenario harness instead of hand-editing them.

Mapped to the release-readiness guarantees:

- **Evidence comment has event ids, run id, step id, correlation key, trace refs** — `dogfood.events` and `dogfood.evidenceReply` carry all of them; `/ath trace <eventId>` resolves each recorded event.
- **Duplicate/stale events ignored or recorded without advancing** — the `duplicate_and_stale_signal` and `stale_approval_then_fresh` scenarios prove a duplicate is de-duped, a stale-head signal is recorded for visibility but does not advance, and the workflow never reaches `released` on stale input.
- **No raw logs, secrets, public-comment instructions, or noisy polling** — a hostile payload (injection text + a fake token + a raw transcript blob) is rendered under an untrusted-data boundary with the token and transcript stripped; public comment text is visible but not automation-eligible; the wait emits exactly one `loop.waiting_for_event` and one `loop.wait_timeout` with no heartbeat spam.
- **Maintainer-gated automation + stale approval protection** — only a trusted maintainer decision whose head matched the current live head advanced the merge. A public comment and a head-mismatched approval were both inert, and a trusted maintainer *deny* decision halts the loop; ATH only transports the deny signal.
- **ATH did not own the loop state machine** — the controller made every advance/merge decision after live-state verification; the gateway target has no merge capability; ATH only authenticated, recorded, rendered, and woke the thread. Cleanup at loop end is performed by the Dynamic Workflows finalizer registry calling the `ath.listener.retire` action, not by ATH self-retiring.

## Maintainer-gated automation framing

Visibility and automation are separate. A bridge may surface public comments, reviews, and checks so humans see what happened, but it must set `payload.trustedAction: false` unless the event is a maintainer-controlled action that is safe to automate from, and the controller must re-verify live state (current head, current check status, current run/step ids) before any risky action. Approval and deny are separate signed events bound to the current `correlation.correlationKey`; a head change invalidates them. ATH never merges, deploys, or runs destructive operations from any event. See [`SECURITY.md`](SECURITY.md) and [`LOOP_EVENTS.md`](LOOP_EVENTS.md#trust-boundary).

## Live dry-run checklist

Run this against a throwaway repo/PR and a disposable gateway thread, with a maintainer account you control, to collect the live evidence the simulated fixture cannot. Do not paste raw logs, secrets, or transcripts into the issue, PR, or thread — capture handles and screenshots only.

1. **Setup.** Create a listener from the disposable thread with the agent tools or `/ath listen`, scoped to the loop + signal event types, with continuation policy metadata. Record the `threadKey` (not the secret).
2. **Start + wait.** Have the controller emit `loop.started` then `loop.waiting_for_event` for a real check suite on the PR head. Confirm the thread shows two compact status posts and no raw logs.
3. **Signal + resume.** Push a commit or let CI run so a real `github.check_suite.completed` is translated to a signed event. Confirm ATH woke the controller and the controller advanced only after re-fetching the live head.
4. **Duplicate + stale.** Redeliver the same signal with the same `eventId` and confirm ATH returns `duplicate` with no second post. Push a new commit to move the head, then deliver a late check for the old head; confirm it is recorded as stale and the loop did not advance.
5. **Approval + stale approval.** Request approval; first try a public non-maintainer comment and confirm it cannot approve. Approve as maintainer on the old head after the head moved and confirm it is rejected as stale. Approve again at the current head and confirm convergence.
6. **Timeout.** Configure a short wait deadline and let it expire with no signal; confirm exactly one `loop.wait_timeout` arrives, with no heartbeats, and the controller halts or retries by its own decision.
7. **Collect.** Capture: the `threadKey`; the ordered `eventId`s and their `/ath trace` output; run id, step ids, correlation keys; evidence URLs (checks, runtime session, PR/review); a screenshot of the compact thread; and the listener cleanup result. Confirm no secret, token, header, or transcript appears anywhere in the thread, PR comment, or trace output.

Record results as handles + screenshots in the release-readiness write-up. The simulated fixture already gates regressions in CI; the live dry run confirms the same guarantees hold against real GitHub/runtime deliveries.
