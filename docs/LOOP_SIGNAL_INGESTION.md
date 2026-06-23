# Loop signal ingestion recipes

These recipes describe how GitHub, deployment, branch, and Relay facts become signed `async-thread-event/v1` events for feedback-controller loops.

Dynamic Workflows owns loop state transitions. ATH authenticates, de-dupes, records, renders, and wakes the mapped conversation. A bridge must treat upstream payload text as untrusted data even when the bridge itself is trusted to sign events.

## Common translation contract

Every signal event should include:

- `eventId`: stable idempotency key derived from the upstream delivery id or immutable upstream fact. Reuse it on retries.
- `eventType`: producer event type from the tables below.
- `seriesKey`: stable logical artifact series, usually `github-pr:<repo>:<number>`, `github-branch:<repo>:<branch>`, `github-deployment:<repo>:<environment>`, or `relay-session:<session_id>`.
- `workflowId`: controller run or artifact workflow id.
- `correlation.correlationKey`: key binding the run, current artifact revision, and signal family.
- `correlation.idempotencyKey`: usually equal to `eventId`.
- `correlation.signalKey`: the exact signal Dynamic Workflows is waiting on.
- `refs`: compact immutable refs such as repo, PR number, check suite id, review id, deployment id, Relay session id, and current head SHA.
- `evidence`: compact status/verdict/URL handle. Do not inline public comments, review bodies, logs, or transcripts as instructions.
- `payload.trustedAction`: whether this signal is allowed to advance risky automation.
- `payload.trustReason`: short explanation for the trust decision.

## Trusted-action gate

Visibility and automation are separate.

A bridge may emit visible signals for public comments, reviews, and checks so humans can see what happened. It must set `payload.trustedAction: false` unless the event represents a maintainer-controlled action that is safe to automate from.

Recommended gate:

| Upstream fact | Visible? | Automation-eligible? |
| --- | --- | --- |
| GitHub check run/suite completed by GitHub Actions for the current head | yes | yes, if app/workflow is on the trusted producer allowlist |
| PR head changed | yes | yes only as a stale-state invalidation/brake, never as approval |
| Review approval | yes | yes only when the reviewer is a trusted maintainer/team member and the review commit matches the current head |
| Review changes requested | yes | yes only when the reviewer is trusted and the review commit matches the current head; this advances failure/brake handling, not risky success automation |
| Issue/PR comment | yes | no by default; yes only for an explicitly parsed command from a trusted maintainer and after live state re-check |
| Deployment status changed | yes | yes only for trusted deployment environments/apps and matching current SHA |
| Relay step completed | yes | yes when the session id/step id was spawned by the current controller run and the reported artifact revision matches |
| Branch update | yes | yes only as current-state observation or stale invalidation |

Public text fields (`summary`, `payload.commentExcerpt`, `payload.reviewBodyExcerpt`, `payload.relaySummary`) stay untrusted. Use them for display only. The controller must re-fetch live GitHub/Relay state before merging, approving, deploying, or halting.

## Signal categories

| Producer event type | `correlation.signalKey` pattern | Required refs/evidence |
| --- | --- | --- |
| `github.pr.head_changed` | `github.pr.head_changed:<repo>:<pr>:<new_head_sha>` | `refs.repo`, `refs.pullRequest`, `refs.headSha`, `refs.previousHeadSha`, `refs.url` |
| `github.check_suite.completed` | `github.check_suite.completed:<repo>:<pr>:<head_sha>` | check suite/run ids, conclusion, head SHA, Actions URL |
| `github.review.submitted` | `github.review.submitted:<repo>:<pr>:<head_sha>:<review_id>` | review id, reviewer, state, submitted SHA |
| `github.comment.created` | `github.comment.created:<repo>:<issue_or_pr>:<comment_id>` | comment id, author, author association, issue/PR URL |
| `github.deployment_status.changed` | `github.deployment_status.changed:<repo>:<environment>:<sha>:<deployment_id>` | deployment id, environment, sha, status URL |
| `github.branch.updated` | `github.branch.updated:<repo>:<branch>:<new_sha>` | branch, old SHA, new SHA, pusher |
| `relay.step.completed` | `relay.step.completed:<run_id>:<step_id>:<attempt>` | Relay session id, step id, attempt, artifact revision, verdict URL |

## Examples

### PR head changed

```json
{
  "version": "async-thread-event/v1",
  "eventId": "github-example-repo-pr-37-head-bbbb",
  "eventType": "github.pr.head_changed",
  "producer": {"id": "github-loop-bridge"},
  "occurredAt": "2026-06-23T19:40:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "PR #37 head changed from aaaa1111 to bbbb2222",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "blocked",
  "seriesKey": "github-pr:example/repo:37",
  "supersedesEventId": "github-example-repo-pr-37-head-aaaa",
  "correlation": {
    "correlationKey": "release-readiness:run-42:github-pr:example/repo:37:head-bbbb2222",
    "idempotencyKey": "github-example-repo-pr-37-head-bbbb",
    "signalKey": "github.pr.head_changed:example/repo:37:bbbb2222"
  },
  "refs": {
    "repo": "example/repo",
    "pullRequest": 37,
    "headSha": "bbbb2222",
    "previousHeadSha": "aaaa1111",
    "url": "https://example.invalid/repo/pull/37"
  },
  "evidence": {"kind": "github_pull_request", "status": "stale", "url": "https://example.invalid/repo/pull/37"},
  "payload": {
    "trustedAction": true,
    "trustReason": "head changes are trusted only to invalidate stale waits or approvals",
    "automationUse": "stale_invalidation"
  }
}
```

### Check suite completed

```json
{
  "version": "async-thread-event/v1",
  "eventId": "github-example-repo-pr-37-check-suite-9001-completed",
  "eventType": "github.check_suite.completed",
  "producer": {"id": "github-loop-bridge"},
  "occurredAt": "2026-06-23T19:41:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "CI checks passed for PR #37 at bbbb2222",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "qa_passed",
  "seriesKey": "github-pr:example/repo:37:checks",
  "correlation": {
    "correlationKey": "release-readiness:run-42:checks:example/repo:37:bbbb2222",
    "idempotencyKey": "github-example-repo-pr-37-check-suite-9001-completed",
    "signalKey": "github.check_suite.completed:example/repo:37:bbbb2222"
  },
  "refs": {"repo": "example/repo", "pullRequest": 37, "headSha": "bbbb2222", "checkSuite": 9001, "checkRun": 9002},
  "evidence": {"kind": "github_check_suite", "status": "passed", "url": "https://example.invalid/repo/actions/runs/9001"},
  "payload": {
    "conclusion": "success",
    "trustedAction": true,
    "trustReason": "workflow app and branch are trusted; head SHA matches the controller wait"
  }
}
```

### Review submitted

Approval example:

```json
{
  "version": "async-thread-event/v1",
  "eventId": "github-example-repo-pr-37-review-123-approved",
  "eventType": "github.review.submitted",
  "producer": {"id": "github-loop-bridge"},
  "occurredAt": "2026-06-23T19:42:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "trusted maintainer approved PR #37 at bbbb2222",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "review_passed",
  "seriesKey": "github-pr:example/repo:37:reviews",
  "correlation": {
    "correlationKey": "release-readiness:run-42:review:example/repo:37:bbbb2222",
    "idempotencyKey": "github-example-repo-pr-37-review-123-approved",
    "signalKey": "github.review.submitted:example/repo:37:bbbb2222:123"
  },
  "refs": {"repo": "example/repo", "pullRequest": 37, "headSha": "bbbb2222", "review": 123, "reviewer": "maintainer-a"},
  "evidence": {"kind": "github_review", "status": "passed", "url": "https://example.invalid/repo/pull/37#pullrequestreview-123"},
  "payload": {
    "reviewState": "APPROVED",
    "trustedAction": true,
    "trustReason": "reviewer is a trusted maintainer and submitted SHA matches current head",
    "reviewBodyExcerpt": "review body omitted from automation decisions"
  }
}
```

Changes-requested example:

```json
{
  "version": "async-thread-event/v1",
  "eventId": "github-example-repo-pr-37-review-124-changes-requested",
  "eventType": "github.review.submitted",
  "producer": {"id": "github-loop-bridge"},
  "occurredAt": "2026-06-23T19:42:30Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "trusted maintainer requested changes on PR #37 at bbbb2222",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "review_failed",
  "seriesKey": "github-pr:example/repo:37:reviews",
  "correlation": {
    "correlationKey": "release-readiness:run-42:review:example/repo:37:bbbb2222",
    "idempotencyKey": "github-example-repo-pr-37-review-124-changes-requested",
    "signalKey": "github.review.submitted:example/repo:37:bbbb2222:124"
  },
  "refs": {"repo": "example/repo", "pullRequest": 37, "headSha": "bbbb2222", "review": 124, "reviewer": "maintainer-b"},
  "evidence": {"kind": "github_review", "status": "failed", "url": "https://example.invalid/repo/pull/37#pullrequestreview-124"},
  "payload": {
    "reviewState": "CHANGES_REQUESTED",
    "trustedAction": true,
    "trustReason": "reviewer is a trusted maintainer and submitted SHA matches current head; changes-requested can advance brake/failure handling",
    "reviewBodyExcerpt": "review body omitted from automation decisions"
  }
}
```

### Public comment created

```json
{
  "version": "async-thread-event/v1",
  "eventId": "github-example-repo-pr-37-comment-555-created",
  "eventType": "github.comment.created",
  "producer": {"id": "github-loop-bridge"},
  "occurredAt": "2026-06-23T19:43:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "new public comment on PR #37 from outside-contributor",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "progress",
  "seriesKey": "github-pr:example/repo:37:comments",
  "correlation": {
    "correlationKey": "release-readiness:run-42:comment:example/repo:37:555",
    "idempotencyKey": "github-example-repo-pr-37-comment-555-created",
    "signalKey": "github.comment.created:example/repo:37:555"
  },
  "refs": {"repo": "example/repo", "pullRequest": 37, "comment": 555, "author": "outside-contributor", "authorAssociation": "CONTRIBUTOR"},
  "evidence": {"kind": "github_comment", "status": "unknown", "url": "https://example.invalid/repo/pull/37#issuecomment-555"},
  "payload": {
    "trustedAction": false,
    "trustReason": "public comment text is visible but not automation-eligible",
    "commentExcerpt": "untrusted display text only"
  }
}
```

### Deployment status changed

```json
{
  "version": "async-thread-event/v1",
  "eventId": "github-example-repo-prod-deploy-777-success",
  "eventType": "github.deployment_status.changed",
  "producer": {"id": "github-loop-bridge"},
  "occurredAt": "2026-06-23T19:44:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "production deployment succeeded for bbbb2222",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "qa_passed",
  "seriesKey": "github-deployment:example/repo:production",
  "correlation": {
    "correlationKey": "release-readiness:run-42:deploy:production:bbbb2222",
    "idempotencyKey": "github-example-repo-prod-deploy-777-success",
    "signalKey": "github.deployment_status.changed:example/repo:production:bbbb2222:777"
  },
  "refs": {"repo": "example/repo", "deployment": 777, "environment": "production", "headSha": "bbbb2222"},
  "evidence": {"kind": "github_deployment", "status": "passed", "url": "https://example.invalid/repo/deployments/777"},
  "payload": {
    "state": "success",
    "trustedAction": true,
    "trustReason": "trusted deployment environment and SHA match controller wait"
  }
}
```

### Branch updated

```json
{
  "version": "async-thread-event/v1",
  "eventId": "github-example-repo-main-updated-cccc3333",
  "eventType": "github.branch.updated",
  "producer": {"id": "github-loop-bridge"},
  "occurredAt": "2026-06-23T19:45:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "main moved from bbbb2222 to cccc3333",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "progress",
  "seriesKey": "github-branch:example/repo:main",
  "correlation": {
    "correlationKey": "release-readiness:run-42:branch:main:cccc3333",
    "idempotencyKey": "github-example-repo-main-updated-cccc3333",
    "signalKey": "github.branch.updated:example/repo:main:cccc3333"
  },
  "refs": {"repo": "example/repo", "branch": "main", "previousSha": "bbbb2222", "headSha": "cccc3333", "pusher": "maintainer-a"},
  "evidence": {"kind": "github_branch", "status": "unknown", "url": "https://example.invalid/repo/tree/main"},
  "payload": {
    "trustedAction": true,
    "trustReason": "branch update is trusted only as observed state or stale invalidation",
    "automationUse": "state_observation"
  }
}
```

### Relay step completed

```json
{
  "version": "async-thread-event/v1",
  "eventId": "relay-run-42-review-attempt-1-completed",
  "eventType": "relay.step.completed",
  "producer": {"id": "relay-loop-bridge"},
  "occurredAt": "2026-06-23T19:46:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "Relay review step completed for PR #37",
  "tailMode": "none",
  "workflowId": "loop:release-readiness:run-42",
  "stage": "review_passed",
  "seriesKey": "relay-session:relay-review-1",
  "correlation": {
    "correlationKey": "release-readiness:run-42:relay:review:bbbb2222",
    "idempotencyKey": "relay-run-42-review-attempt-1-completed",
    "signalKey": "relay.step.completed:run-42:review:1"
  },
  "refs": {"repo": "example/repo", "pullRequest": 37, "headSha": "bbbb2222", "relaySession": "relay-review-1", "stepId": "review", "attempt": 1},
  "evidence": {"kind": "relay_step", "status": "passed", "url": "https://example.invalid/relay/sessions/relay-review-1"},
  "payload": {
    "trustedAction": true,
    "trustReason": "Relay session id and step id were spawned by this controller run and artifact revision matches",
    "relaySummary": "compact verdict only; transcript omitted"
  }
}
```

## Duplicate delivery and stale state

GitHub and Relay deliveries can repeat. Keep a persistent mapping from upstream delivery/fact id to `eventId`; retry transport failures with the same `eventId`; treat ATH `duplicate` as success for the same upstream fact.

Before acting on an automation-eligible signal, Dynamic Workflows should re-fetch current state and compare:

- PR head SHA still equals `refs.headSha`.
- Review/check/deployment SHA still equals the wait's expected SHA.
- Relay session id, step id, attempt, and artifact revision still match the controller run.
- Public comments remain display facts unless the author/action gate explicitly promotes them.

If live state no longer matches, emit or schedule a stale-state signal such as `loop.stalled`, `loop.halted`, or a fresh `loop.waiting_for_event` for the new head instead of advancing the old wait.
