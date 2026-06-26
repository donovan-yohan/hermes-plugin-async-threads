---
source: README.md, docs/QUICKSTART.md, and current public docs
illustration_set: async-threads-public-release-assets
updated_for: agent-first ATH positioning and getting-started docs
---

# Async Threads visual analysis

## Content type

Technical/product infographics for public README and Quickstart docs.

## Purpose

Explain the current async-threads MVP at a glance without making users learn `/ath listen` first. The visuals should support the public docs framing: ATH is primarily a model-facing agent tool workflow; `/ath` is the manual admin/debug surface.

## Core message

1. A user asks Hermes in the current gateway conversation to watch/report on work.
2. Hermes uses model-facing ATH tools (`ath_create_listener`, `ath_generate_producer_handoff`) to create/reuse the route and generate a safe producer handoff.
3. The producer uses the contract/helper/`ATH_SECRET_FILE` path, not a raw secret pasted into chat.
4. A signed `async-thread-event/v1` arrives at the local receiver and is validated for HMAC, replay window, route scope, and de-dupe.
5. The registry resolves `threadKey` to the stored origin session, then policy chooses direct notification or `agent_queue` continuation metadata.
6. The event returns to the same mapped gateway conversation, with payload boxed as untrusted data.

## Existing flow image notes

`baoyu-async-thread-flow.png` uses the desired dark technical vector style: deep navy background, violet cards, cyan arrows, amber active route, and a red untrusted-data boundary. Vision QA confirmed it is already framed as an agent-first flow: it starts with "User asks Hermes", shows `ath_create_listener` and `ath_generate_producer_handoff`, presents safe handoff files, and keeps `/ath` as an admin/debug badge.

The image remains useful for the README "How it works" section. It is concept-level, not a step-by-step setup card.

## Getting-started image target

The Quickstart needs a more procedural first-run infographic. It should preserve the same visual language while showing the concrete setup journey:

- "Enable plugin"
- "User asks Hermes"
- "ATH tools create listener"
- "Producer gets handoff"
- "Signed event wakes thread"
- Safety badges: "MVP: gateway-local", "tool-first; /ath = admin/debug", and "payload is untrusted data"

Avoid platform logos, universal support claims, desktop/CLI imagery, raw secret text, raw logs, or literal manual command terminals as the primary visual.

## Generated getting-started QA notes

`getting-started-agent-first.png` correctly starts from plugin enablement and a natural user ask, keeps manual `/ath` as a bottom safety badge, shows producer files/secret-file reference rather than raw secret text, includes HMAC/timestamp/scope/de-dupe filters, and routes back to the current gateway conversation. Text is readable enough for Quickstart/README use; minor generated label imperfections are limited to small supporting text and do not change the public safety story.
