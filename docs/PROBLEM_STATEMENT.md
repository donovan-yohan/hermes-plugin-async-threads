# Problem statement: event-driven async thread continuation for Hermes

## Background

Hermes is already useful as a multi-platform conversational agent: a user can work with Hermes in a Discord thread, Telegram topic, CLI session, or another gateway surface. Hermes also has durable systems like cron, webhooks, background tasks, delegation, and workflow tooling.

An external workbench/control plane can observe agent sessions, terminals, nodes, work contexts, repository automation, handoffs, and cross-device execution. It can see meaningful state changes that should pull the operator back into the same conversation: an agent session opened a PR, a session became idle, an agent hit a permission prompt, a node paired, a work-context artifact was published, a long-running workflow finished, or a remote node went offline.

The current workaround is a finite quiet cron notifier:

1. create a script-only cron job;
2. poll producer/GitHub/session state every N minutes;
3. print only when something changed;
4. deliver the cron output back to the origin thread.

That is better than noisy watchdog spam, but it is still a hack. The user explicitly wants to avoid polling and keep visibility in the same working conversation.

## The problem

Hermes lacks a first-class event-driven way for an external system to re-wake an existing conversation/thread when asynchronous work reaches a meaningful transition.

The desired behavior is:

> A long-running external process emits a signed event. Hermes de-dupes it, resolves the existing conversation origin, optionally runs a bounded agent continuation, and posts the result back into the same Discord/Telegram/etc. thread where the work started.

Today, cron has convenient `deliver: origin` semantics, but webhook-triggered runs and arbitrary external events do not have an equally ergonomic “resume this exact working thread” surface. Webhooks can trigger agent runs, and Hermes can deliver to platforms, but the missing abstraction is an **async thread**: a durable routing + context handle that external producers can target without hand-configuring a static channel for every workflow.

## Motivating scenario

A user starts long-running work from a gateway conversation. The work runs outside the chat surface: a CI job, repository automation task, local script, background agent session, or deployment process. When that work reaches a meaningful transition, the desired workflow is:

1. keep the original Hermes conversation as the operator cockpit;
2. let the external producer work independently;
3. send a signed event when the work needs attention;
4. have Hermes resolve the existing conversation and either post a direct notification or queue a bounded continuation;
5. avoid polling loops and hardcoded chat-platform delivery code in the producer.

The workaround is a finite quiet cron job that polls external state and posts only when it sees a transition. That avoids chat spam, but it still adds polling, scheduler state, and lifecycle cleanup burden.

A better system would let an external producer emit:

```txt
producer.session.pr_opened
```

or:

```txt
producer.work_context.chunk_ready
```

and have Hermes wake the same Discord thread directly.

## Goals

### 1. Event-driven continuation, not polling

External systems should be able to POST an event to Hermes when a meaningful state transition occurs. Hermes should not need a cron job that repeatedly asks “are we there yet?”

### 2. Same-conversation visibility

The continuation should land in the same platform conversation/thread/topic where the work began whenever an origin handle exists. The user should not need to watch a separate scheduler dashboard or a random home-channel notification.

### 3. Durable async-thread handles

Hermes should maintain a durable mapping from an async workflow/thread key to delivery and session context:

- platform, chat/channel, thread/topic id;
- Hermes session id or session lineage;
- originating user and authorization scope;
- optional work context id / repo / issue / PR / external run id;
- last event id / de-dupe state;
- policy for direct delivery vs agent continuation.

### 4. Safe wake policy

Not every event should blindly launch an expensive agent run. A route should be able to choose:

- direct delivery only;
- bounded summarization;
- full agent continuation;
- ask-for-approval / suggest next action;
- suppress/no-op if event is not actionable.

### 5. Producer-agnostic receiver

A workflow workbench is one motivating producer, but the Hermes plugin should stay producer-agnostic. It should work for:

- external session/work-context/node events;
- GitHub PR/check events;
- CI deploy events;
- long-running local jobs;
- home automation alerts;
- any signed webhook producer with an async-thread handle.

### 6. Minimal secure contract

The event receiver must authenticate producers, de-dupe events, validate route scope, and avoid prompt-injection from untrusted payload text.

## Non-goals

- Do not make producers speak directly to every chat platform. Hermes gateway owns Discord/Telegram/etc. delivery.
- Do not make a producer reimplement Hermes, the Hermes dashboard, or a Hermes workspace.
- Do not turn every webhook payload into an unconditional agent prompt.
- Do not trust public GitHub issue/comment body text as instructions.
- Do not require every workflow to use cron.
- Do not require a full task board just to wake a thread on an event.
- Do not preserve unbounded context or raw logs in event payloads.
- Do not leak secrets, pair tokens, credentials, terminal bytes, environment values, bearer headers, or raw transcript blobs into events or delivery messages.

## Security and trust requirements

### Authenticated producers

Events must be signed or authenticated. Candidate mechanisms:

- per-route HMAC secrets;
- short-lived producer tokens;
- mTLS or trusted local socket for local-only producers;
- explicit producer registration with allowed event types.

### De-dupe and replay protection

Each event needs:

- stable `eventId`;
- `eventType`;
- producer id;
- timestamp;
- optional nonce/signature;
- replay window policy.

Hermes should persist processed event ids per producer/thread and ignore duplicates.

### Route scoping

A producer should not be able to wake arbitrary chats by forging platform ids. The async-thread handle should be created by Hermes when the original workflow starts, or explicitly approved by the user/operator.

### Prompt-injection resistance

Event payloads are data, not instructions. Agent prompts should render payloads under a clear untrusted-data boundary, and route templates should be controlled by trusted local config/plugin code.

For GitHub or producer automation, routes should be able to enforce trusted-actor policy, such as only allowing events from approved actors to trigger automation.

### Capability and cost controls

Routes should declare max turn count, toolsets, model/budget, direct-delivery vs agent-run policy, and rate limits. A burst of events must not spawn unbounded expensive agent runs.

## Proposed async event envelope

```json
{
  "version": "async-thread-event/v1",
  "eventId": "evt_01J...",
  "eventType": "producer.session.pr_opened",
  "producer": {
    "id": "demo-producer",
    "kind": "ci",
    "signature": "..."
  },
  "occurredAt": "2026-06-18T12:34:56Z",
  "asyncThread": {
    "threadKey": "ath_01J...",
    "hermesSessionId": "optional-known-session-id",
    "platform": "discord",
    "chatId": "1494215934519283732",
    "threadId": "1516979297536049323"
  },
  "routing": {
    "deliver": "origin",
    "policy": "agent-continuation",
    "priority": "normal"
  },
  "subject": {
    "repo": "example/repo",
    "pr": 123,
    "workflowId": "wf_demo_123",
    "externalSessionId": "external-run-123"
  },
  "summary": "demo workflow opened PR 123 and is ready for review.",
  "payload": {
    "safe": "producer-specific JSON only; treated as untrusted data"
  }
}
```

## Proposed plugin responsibilities

`hermes-plugin-async-threads` should provide:

1. **Async-thread registry**
   - create/update thread handles;
   - map handles to gateway delivery origin;
   - store policy, producer scope, de-dupe state, and session references.

2. **Webhook receiver**
   - accept signed events;
   - validate producer and route scope;
   - de-dupe/replay-protect;
   - normalize into Hermes continuation requests.

3. **Delivery/continuation dispatcher**
   - direct-deliver small notifications when policy says so;
   - start a bounded agent run when policy says agent continuation;
   - deliver output back into the mapped origin thread;
   - preserve enough context to feel like the same working conversation.

4. **Config surface**
   - route definitions;
   - producer secrets/tokens;
   - max turn/tool/model policy;
   - rate limits;
   - actor trust filters;
   - redaction settings.

5. **CLI/admin surface**
   - list async threads;
   - create/register a thread manually;
   - revoke/pause a thread;
   - inspect recent events;
   - replay a safe event for testing.

## Producer-side complementary work

The producer should not own chat-platform delivery. The producer should emit events.

A producer needs a durable event/outbox/subscription layer for events like:

- `producer.session.idle`
- `producer.session.needs_attention`
- `producer.session.permission_prompt`
- `producer.session.blocked`
- `producer.session.pr_opened`
- `producer.branch.pushed`
- `producer.work_context.artifact_published`
- `producer.work_context.chunk_ready`
- `producer.node.paired`
- `producer.node.offline`

Each event should include safe routing metadata such as work-context id, producer session id, repo, issue/PR refs, safe summary, redacted diagnostics, and an async-thread handle if Hermes registered one.

## Hermes-side open questions

1. Should async-thread registry live as a plugin-local SQLite DB, or should it reuse Hermes session/gateway routing storage?
2. Is `deliver: origin` a cron-only concept today, or should it become a general delivery target for webhook/plugin-triggered continuations?
3. Should the plugin resume the original Hermes session lineage or start a new session with a compact context packet?
4. What is the right API for a plugin to ask the gateway to deliver into an existing platform thread?
5. How should concurrent events for the same async thread be queued, collapsed, or sequenced?
6. What should happen if the original platform thread no longer exists or delivery fails?
7. Which route policies should be built in vs supplied by config/templates?

## MVP shape

A useful MVP can be small:

1. Plugin-local async-thread registry with `threadKey -> delivery origin + policy`.
2. Signed webhook receiver for `async-thread-event/v1`.
3. De-dupe by `producerId + eventId`.
4. Direct delivery mode into the registered origin thread.
5. Agent-continuation mode with a bounded prompt and restricted toolsets.
6. One producer example: job finished / needs attention / blocked.
7. CLI command to register/list/revoke async threads.
8. Tests for signature validation, de-dupe, prompt injection boundary, origin routing, and fallback behavior.

## Success criteria

- An external agent session can open a PR and trigger Hermes to post back into the original gateway thread without cron polling.
- Duplicate events do not duplicate messages or agent runs.
- Untrusted payload text is not treated as instruction.
- A missing/revoked async-thread handle fails closed.
- The user can see and manage registered async threads.
- The plugin can direct-deliver or run a bounded continuation based on route policy.
- The pattern is generic enough for multiple producer types.

## Anti-patterns to avoid

- Cron jobs that poll forever and need cleanup.
- Per-workflow ad hoc scripts with hardcoded Discord channel/thread ids.
- Producers posting directly to chat platforms instead of emitting events to Hermes.
- Webhook routes that dump raw JSON into an unconstrained agent prompt.
- Event payloads that contain secrets, raw logs, terminal bytes, credentials, pair tokens, or browser cookies.
- Treating a public GitHub comment as a trusted instruction source.
- Spawning unbounded agent runs on every event.

## Why this matters

The working conversation should be the cockpit. If a user starts a long-running agent workflow in a Discord thread, Telegram topic, CLI session, desktop surface, or future Hermes mobile surface, meaningful async events should flow back there naturally. The operator should not have to remember to check cron output, producer dashboards, GitHub, task boards, and chat threads separately.

This plugin is the missing bridge between event-producing systems and Hermes' conversational gateway: wake the right thread, with the right context, at the right time, without polling.
