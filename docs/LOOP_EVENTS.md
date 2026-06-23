# Loop event contract

`loop.*` events are the ATH convention for feedback-controller visibility and wakeups. Dynamic Workflows owns controller state and decisions. Relay owns agent execution and artifacts. ATH transports signed signals, records diagnostics, wakes mapped conversations, and renders compact status.

Payload text is untrusted data. Correlation fields help route, debug, and reject stale signals; they do **not** turn `summary`, `payload`, GitHub comment text, evidence text, or producer-provided strings into instructions for Hermes or an agent.

## Event types

| Event type | Purpose | Usual stage | Priority |
| --- | --- | --- | --- |
| `loop.started` | A controller run began and published its setpoint. | `started` | normal |
| `loop.sensor_failed` | A sensor failed to read external state or evidence. | `blocked` | priority |
| `loop.step_started` | A bounded backend/agent step began. | `progress` | normal |
| `loop.step_completed` | A bounded backend/agent step completed and evidence is available. | `progress` or gate-specific stage | normal/priority by verdict |
| `loop.waiting_for_event` | The controller is parked until a specific external signal arrives. | `blocked` | normal |
| `loop.waiting_for_approval` | A risky or irreversible action needs explicit current-state approval. | `needs_attention` | priority |
| `loop.approval_granted` | A trusted approval decision arrived for the current correlation. | `needs_attention` or gate-specific stage | priority |
| `loop.approval_denied` | A trusted denial decision arrived for the current correlation. | `cancelled` or gate-specific stage | priority |
| `loop.approval_stale` | An approval/deny decision was received but no longer matches current loop state. | `blocked` | priority |
| `loop.stalled` | Expected progress did not happen before a bounded deadline or repeated blocker threshold. | `blocked` | priority |
| `loop.halted` | A brake fired and the controller stopped. | `cancelled` | priority |
| `loop.converged` | The controller reached its setpoint and is done. | `released` | priority/terminal |

## Required loop correlation fields

Every `loop.*` event still uses the normal `async-thread-event/v1` envelope. Put loop metadata in producer-specific objects so the stable envelope stays compatible with existing bridges.

| Field | Type | Contract |
| --- | --- | --- |
| `workflowId` | string | Stable workflow id for ATH workflow tracking. For loops, usually `loop:<runId>` or the external workflow id. |
| `stage` | string | Existing ATH workflow stage. Use the usual stages, not a new state machine. |
| `seriesKey` | string | Stable event series, for example `loop:<specId>:<runId>` or `github-pr:<repo>:<number>`. |
| `supersedesEventId` | string | Previous event id in the same series, when this event replaces a known older signal. |
| `loop.runId` | string | Immutable controller run id. Required for all `loop.*` events. |
| `loop.specId` | string | Stable loop specification id. |
| `loop.specName` | string | Short human name for the loop spec. |
| `loop.state` | string | Controller-observed state such as `running`, `waiting`, `approval_required`, `stalled`, `halted`, or `converged`. This is data for the controller/human, not an ATH state machine. |
| `step.stepId` | string | Required for step-scoped events. Stable within the run. |
| `step.attempt` | integer | Current attempt number for a retryable step. |
| `step.backend` | string | Execution backend such as `relay`, `github`, `local`, or another producer-owned backend id. |
| `correlation.correlationKey` | string | Current state key that approval/signal handlers must match before acting. Include run id and current artifact revision/head when relevant. |
| `correlation.idempotencyKey` | string | Stable key for dedupe across producer retries and downstream controller processing. Often equals or derives from `eventId`. |
| `correlation.signalKey` | string | The external signal this loop waits for or emits, for example `github.check_suite.completed:repo:pr:head`. |
| `refs` | object | External refs: repo, issue, PR, head SHA, branch, deployment, Discord thread, Relay session, etc. |
| `evidence` | object | Compact handles/URLs/statuses for verification. Do not inline giant logs. |
| `nextExpectedSignal` | object | What signal/action/deadline the controller expects next. |

Recommended top-level shape:

```json
{
  "version": "async-thread-event/v1",
  "eventId": "loop-run-42-started",
  "eventType": "loop.started",
  "producer": {"id": "dynamic-workflows"},
  "occurredAt": "2026-06-23T17:00:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "release readiness loop started for PR 86",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "started",
  "seriesKey": "loop:release-readiness:run-42",
  "loop": {
    "runId": "run-42",
    "specId": "release-readiness",
    "specName": "Release readiness loop",
    "state": "running"
  },
  "correlation": {
    "correlationKey": "release-readiness:run-42:head-a1b2c3d4",
    "idempotencyKey": "loop-run-42-started",
    "signalKey": "loop.started:release-readiness:run-42"
  },
  "refs": {
    "repo": "example/repo",
    "pullRequest": 86,
    "headSha": "a1b2c3d4"
  },
  "evidence": {
    "kind": "loop_run",
    "status": "unknown",
    "url": "https://example.invalid/loops/run-42"
  },
  "nextExpectedSignal": {
    "signalKey": "github.check_suite.completed:example/repo:86:a1b2c3d4",
    "deadlineAt": "2026-06-23T17:15:00Z",
    "onTimeoutEventType": "loop.stalled"
  }
}
```

## Event-specific contracts

### `loop.started`

Use when a controller run is created and the setpoint is known.

- `loop.state`: `running`
- `stage`: `started`
- include `refs` for the primary external artifact, if any
- include `nextExpectedSignal` when the run immediately waits for external state

### `loop.sensor_failed`

Use when a sensor could not read or validate external state.

- `loop.state`: `sensor_failed` or `blocked`
- `stage`: `blocked`
- `evidence.kind`: `sensor`
- `evidence.status`: `failed`
- include a compact error class/message; never include raw headers, cookies, tokens, or full response bodies
- include `nextExpectedSignal` only if a retry/wait is scheduled

### `loop.step_started`

Use when a bounded backend step begins.

- `loop.state`: `running`
- `stage`: `progress`
- `step.stepId`, `step.attempt`, and `step.backend` required
- `evidence.status`: `unknown` until a completion event arrives

```json
{
  "version": "async-thread-event/v1",
  "eventId": "loop-run-42-step-review-started-attempt-1",
  "eventType": "loop.step_started",
  "producer": {"id": "dynamic-workflows"},
  "occurredAt": "2026-06-23T17:02:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "review step started for PR 86",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "progress",
  "seriesKey": "loop:release-readiness:run-42:step:review",
  "loop": {"runId": "run-42", "specId": "release-readiness", "specName": "Release readiness loop", "state": "running"},
  "step": {"stepId": "review", "attempt": 1, "backend": "relay"},
  "correlation": {
    "correlationKey": "release-readiness:run-42:review:head-a1b2c3d4",
    "idempotencyKey": "loop-run-42-step-review-started-attempt-1",
    "signalKey": "relay.session.started:run-42:review"
  },
  "refs": {"repo": "example/repo", "pullRequest": 86, "headSha": "a1b2c3d4", "relaySession": "relay://sessions/review-1"},
  "evidence": {"kind": "relay_session", "status": "unknown", "url": "https://example.invalid/relay/review-1"},
  "nextExpectedSignal": {"signalKey": "relay.session.completed:run-42:review", "deadlineAt": "2026-06-23T17:20:00Z"}
}
```

### `loop.step_completed`

Use when a bounded backend step finishes.

- `loop.state`: usually `running` unless this step caused a wait/halt/convergence
- `stage`: match the gate when useful, for example `review_passed`, `review_failed`, `qa_passed`, or `qa_failed`
- include `step.*`
- include evidence handles and verdicts; do not inline transcripts
- include `nextExpectedSignal` for the next wait or approval

### `loop.waiting_for_event`

Use when the controller intentionally parks until an external signal arrives.

- `loop.state`: `waiting`
- `stage`: `blocked` or the current gate stage
- `nextExpectedSignal.signalKey` required
- `nextExpectedSignal.deadlineAt` recommended when timeout handling exists
- `correlation.signalKey` should match the expected signal family

### `loop.waiting_for_approval`

Use when a risky action requires a fresh, current-state decision.

- `loop.state`: `approval_required`
- `stage`: `needs_attention`
- `correlation.correlationKey` must bind run id, step id/action, current external revision/head/state, and approval kind
- `nextExpectedSignal.signalKey` should name the approval channel/action
- approval producers must compare current live state before accepting an approval/deny signal
- include `nextExpectedSignal.approvalId`, action/risk metadata, and an expiry so humans and controllers know exactly what decision is being requested

```json
{
  "version": "async-thread-event/v1",
  "eventId": "loop-run-42-approval-merge-head-a1b2c3d4",
  "eventType": "loop.waiting_for_approval",
  "producer": {"id": "dynamic-workflows"},
  "occurredAt": "2026-06-23T17:25:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "approval needed before merging PR 86 at head a1b2c3d4",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "needs_attention",
  "seriesKey": "loop:release-readiness:run-42:approval:merge",
  "loop": {"runId": "run-42", "specId": "release-readiness", "specName": "Release readiness loop", "state": "approval_required"},
  "step": {"stepId": "merge", "attempt": 1, "backend": "github"},
  "correlation": {
    "correlationKey": "approval:merge:example/repo:86:a1b2c3d4:run-42",
    "idempotencyKey": "loop-run-42-approval-merge-head-a1b2c3d4",
    "signalKey": "approval.merge.requested:example/repo:86:a1b2c3d4"
  },
  "refs": {"repo": "example/repo", "pullRequest": 86, "headSha": "a1b2c3d4", "review": "https://example.invalid/repo/pull/86#pullrequestreview-1"},
  "evidence": {"kind": "merge_gate", "status": "passed", "url": "https://example.invalid/repo/actions/runs/123"},
  "nextExpectedSignal": {
    "signalKey": "approval.merge.decided:example/repo:86:a1b2c3d4",
    "approvalId": "approval-merge-run-42-head-a1b2c3d4",
    "expiresAt": "2026-06-23T18:25:00Z",
    "allowedDecisions": ["approve", "deny"]
  }
}
```

### `loop.approval_granted`, `loop.approval_denied`, and `loop.approval_stale`

Use these when an approval producer reports a human decision. ATH records, renders, de-dupes, and wakes; Dynamic Workflows decides whether the decision still matches live state and whether to act.

- `payload.approvalId` must match the request's approval id.
- `correlation.correlationKey` must match the request and bind run id, step/action, current head/artifact revision, and approval kind.
- `payload.decision` should be `approve`, `deny`, or `stale`.
- `payload.trustedAction` must be false unless trusted actor/acted-by checks passed and live state still matches.
- stale decisions should use `loop.approval_stale` and `evidence.status: stale`, not `loop.approval_granted`.
- approval and denial decisions are idempotent: retry the same real-world decision with the same `eventId` and `correlation.idempotencyKey`.
- public comments cannot approve by text alone; a bridge must prove trusted actor/acted-by provenance and re-check the current head/state.
- ATH must not merge, deploy, delete, or execute destructive operations from these events.

```json
{
  "version": "async-thread-event/v1",
  "eventId": "approval-merge-run-42-head-a1b2c3d4-approved",
  "eventType": "loop.approval_granted",
  "producer": {"id": "approval-bridge"},
  "occurredAt": "2026-06-23T17:30:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "trusted maintainer approved merge for PR 86 at head a1b2c3d4",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "needs_attention",
  "seriesKey": "loop:release-readiness:run-42:approval:merge",
  "supersedesEventId": "loop-run-42-approval-merge-head-a1b2c3d4",
  "loop": {"runId": "run-42", "specId": "release-readiness", "specName": "Release readiness loop", "state": "approval_granted"},
  "step": {"stepId": "merge", "attempt": 1, "backend": "github"},
  "correlation": {
    "correlationKey": "approval:merge:example/repo:86:a1b2c3d4:run-42",
    "idempotencyKey": "approval-merge-run-42-head-a1b2c3d4-approved",
    "signalKey": "approval.merge.decided:example/repo:86:a1b2c3d4"
  },
  "refs": {"repo": "example/repo", "pullRequest": 86, "headSha": "a1b2c3d4", "approvalId": "approval-merge-run-42-head-a1b2c3d4"},
  "evidence": {"kind": "approval", "status": "passed", "url": "https://example.invalid/repo/pull/86#issuecomment-1"},
  "payload": {"approvalId": "approval-merge-run-42-head-a1b2c3d4", "decision": "approve", "trustedAction": true, "trustedActor": "maintainer-a", "trustReason": "trusted maintainer command and current PR head matched a1b2c3d4 at decision time"}
}
```

```json
{
  "version": "async-thread-event/v1",
  "eventId": "approval-merge-run-42-head-a1b2c3d4-denied",
  "eventType": "loop.approval_denied",
  "producer": {"id": "approval-bridge"},
  "occurredAt": "2026-06-23T17:31:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "trusted maintainer denied merge for PR 86 at head a1b2c3d4",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "cancelled",
  "seriesKey": "loop:release-readiness:run-42:approval:merge",
  "supersedesEventId": "loop-run-42-approval-merge-head-a1b2c3d4",
  "loop": {"runId": "run-42", "specId": "release-readiness", "specName": "Release readiness loop", "state": "approval_denied"},
  "step": {"stepId": "merge", "attempt": 1, "backend": "github"},
  "correlation": {
    "correlationKey": "approval:merge:example/repo:86:a1b2c3d4:run-42",
    "idempotencyKey": "approval-merge-run-42-head-a1b2c3d4-denied",
    "signalKey": "approval.merge.decided:example/repo:86:a1b2c3d4"
  },
  "refs": {"repo": "example/repo", "pullRequest": 86, "headSha": "a1b2c3d4", "approvalId": "approval-merge-run-42-head-a1b2c3d4"},
  "evidence": {"kind": "approval", "status": "failed", "url": "https://example.invalid/repo/pull/86#issuecomment-2"},
  "payload": {"approvalId": "approval-merge-run-42-head-a1b2c3d4", "decision": "deny", "trustedAction": true, "trustedActor": "maintainer-a", "trustReason": "trusted maintainer command and current PR head matched a1b2c3d4 at decision time"}
}
```

```json
{
  "version": "async-thread-event/v1",
  "eventId": "approval-merge-run-42-head-a1b2c3d4-stale-after-head-bbbb2222",
  "eventType": "loop.approval_stale",
  "producer": {"id": "approval-bridge"},
  "occurredAt": "2026-06-23T17:32:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "approval ignored because PR #86 moved from a1b2c3d4 to bbbb2222",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "blocked",
  "seriesKey": "loop:release-readiness:run-42:approval:merge",
  "supersedesEventId": "loop-run-42-approval-merge-head-a1b2c3d4",
  "loop": {"runId": "run-42", "specId": "release-readiness", "specName": "Release readiness loop", "state": "approval_stale"},
  "step": {"stepId": "merge", "attempt": 1, "backend": "github"},
  "correlation": {
    "correlationKey": "approval:merge:example/repo:86:a1b2c3d4:run-42",
    "idempotencyKey": "approval-merge-run-42-head-a1b2c3d4-stale-after-head-bbbb2222",
    "signalKey": "approval.merge.decided:example/repo:86:a1b2c3d4"
  },
  "refs": {"repo": "example/repo", "pullRequest": 86, "headSha": "bbbb2222", "expectedHeadSha": "a1b2c3d4", "approvalId": "approval-merge-run-42-head-a1b2c3d4"},
  "evidence": {"kind": "approval", "status": "stale", "url": "https://example.invalid/repo/pull/86#issuecomment-3"},
  "payload": {"approvalId": "approval-merge-run-42-head-a1b2c3d4", "decision": "stale", "trustedAction": false, "staleReason": "head_changed", "trustReason": "current head bbbb2222 did not match approval correlation head a1b2c3d4"}
}
```

### `loop.stalled`

Use when a deadline expires or repeated failures mean the loop is not making progress.

- `loop.state`: `stalled`
- `stage`: `blocked`
- include the missed `nextExpectedSignal.signalKey` or the blocker classification in `correlation.signalKey`
- include compact evidence handles for the last observed state
- this is a notification/brake signal; the controller still decides retry/halt

### `loop.halted`

Use when the controller stops because a brake fired, a stale signal was rejected, or a human denied an action.

- `loop.state`: `halted`
- `stage`: `cancelled`
- terminal by lifecycle convention when listener policy includes `loop.halted` or terminal stage `cancelled`
- include final evidence and suggested human next step

### `loop.converged`

Use when the controller reaches the setpoint.

- `loop.state`: `converged`
- `stage`: `released`
- terminal by lifecycle convention when listener policy includes `loop.converged` or terminal stage `released`
- include final artifacts/evidence handles
- producers for single-goal listeners should self-exit after emitting this event

```json
{
  "version": "async-thread-event/v1",
  "eventId": "loop-run-42-converged",
  "eventType": "loop.converged",
  "producer": {"id": "dynamic-workflows"},
  "occurredAt": "2026-06-23T17:40:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "release readiness loop converged for PR 86",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "released",
  "seriesKey": "loop:release-readiness:run-42",
  "supersedesEventId": "loop-run-42-approval-merge-head-a1b2c3d4",
  "loop": {"runId": "run-42", "specId": "release-readiness", "specName": "Release readiness loop", "state": "converged"},
  "step": {"stepId": "merge", "attempt": 1, "backend": "github"},
  "correlation": {
    "correlationKey": "release-readiness:run-42:converged:a1b2c3d4",
    "idempotencyKey": "loop-run-42-converged",
    "signalKey": "loop.converged:release-readiness:run-42"
  },
  "refs": {"repo": "example/repo", "pullRequest": 86, "headSha": "a1b2c3d4", "mergeCommit": "deadbeef"},
  "evidence": {"kind": "release_gate", "status": "passed", "url": "https://example.invalid/repo/pull/86"},
  "nextExpectedSignal": {"signalKey": "none", "reason": "loop converged"}
}
```

## Compatibility with existing workflow fields

Loop events intentionally reuse `workflowId`, `stage`, `artifact`, `candidate`, `evidence`, `seriesKey`, and `supersedesEventId`. Existing ATH workflow tracking can record them without knowing about every loop-specific nested object.

Use `artifact` for the thing being moved through gates and `refs` for external lookup handles. If both are present, keep them consistent:

```json
{
  "version": "async-thread-event/v1",
  "eventId": "loop-run-42-step-review-completed",
  "eventType": "loop.step_completed",
  "producer": {"id": "dynamic-workflows"},
  "occurredAt": "2026-06-23T17:12:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "review step passed for PR 86",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "review_passed",
  "seriesKey": "loop:release-readiness:run-42:step:review",
  "loop": {"runId": "run-42", "specId": "release-readiness", "specName": "Release readiness loop", "state": "running"},
  "step": {"stepId": "review", "attempt": 1, "backend": "relay"},
  "artifact": {"kind": "pull_request", "id": "86", "url": "https://example.invalid/repo/pull/86", "revision": "a1b2c3d4"},
  "candidate": {"id": "pr-86", "kind": "pull_request", "readiness": "review_passed"},
  "correlation": {"correlationKey": "release-readiness:run-42:review:head-a1b2c3d4", "idempotencyKey": "loop-run-42-step-review-completed", "signalKey": "relay.session.completed:run-42:review"},
  "refs": {"repo": "example/repo", "pullRequest": 86, "headSha": "a1b2c3d4", "relaySession": "relay://sessions/review-1"},
  "evidence": {"kind": "review", "status": "passed", "url": "https://example.invalid/reviews/1"},
  "nextExpectedSignal": {"signalKey": "github.check_suite.completed:example/repo:86:a1b2c3d4", "deadlineAt": "2026-06-23T17:30:00Z"}
}
```

## Trust boundary

Correlation is routing and debugging metadata only. It helps a controller decide whether an event belongs to the current run, whether a decision is stale, and which live external state to verify. It does not authorize actions by itself.

Before risky actions, a controller or trusted producer must verify live state: current PR head, current check status, current run id, current step id, and current correlation key. Public comments, webhook payload text, summaries, and evidence descriptions remain untrusted data even when they carry matching ids.

Approval and deny events should be separate signed events with the same current `correlation.correlationKey` plus a maintainer-authored or maintainer-acted provenance field. Stale approvals must be recorded as stale/ignored, not applied to newer loop state. The concrete approval/deny event contract is implemented in the later approval child issue.
