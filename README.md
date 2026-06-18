# hermes-plugin-async-threads

Problem statement and seed design for event-driven async thread continuation in Hermes.

Hermes can already run in Discord/Telegram/etc., schedule cron jobs, receive webhooks, and deliver messages back to channels. What is missing is a first-class way for an external system like Relay to say:

> “Something meaningful happened for this existing conversation. Re-wake the same thread/session and continue from there.”

Today the workaround is usually a finite quiet cron notifier that polls for state changes and delivers back to `origin`. That works, but it is gross plumbing: it wastes cycles, adds latency, creates zombie-watchdog risk, and scatters workflow visibility across scheduler state instead of keeping the working conversation as the cockpit.

See [`docs/PROBLEM_STATEMENT.md`](docs/PROBLEM_STATEMENT.md) for the full captured context, goals, non-goals, and proposed direction.

## Working name

`hermes-plugin-async-threads`

The name is intentionally about **async continuation of conversation threads**, not Relay specifically. Relay is the motivating dogfood producer, but the receiver pattern should apply to GitHub, CI, home automation, long-running agent sessions, and any event source that wants to resume a specific Hermes conversation.

## Core idea

An external producer sends a signed event with a stable async-thread key:

```json
{
  "eventId": "evt_...",
  "eventType": "relay.session.pr_opened",
  "asyncThread": {
    "sessionId": "...",
    "platform": "discord",
    "chatId": "...",
    "threadId": "..."
  },
  "summary": "#980 opened PR #123 and is ready for review",
  "payload": {}
}
```

Hermes receives it, de-dupes it, restores enough origin/session context, runs a bounded agent prompt or direct-delivery policy, and posts back into the same thread.

## Status

Seed repo. No implementation yet.

## License

MIT, unless changed before implementation.
