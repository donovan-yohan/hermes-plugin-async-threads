---
source: README.md and current public docs
illustration_set: async-threads-public-release-assets
updated_for: agent-first ATH positioning
---

# Async Threads visual analysis

## Content type

Technical/product infographic for a public README.

## Purpose

Explain the current async-threads MVP at a glance without making users learn `/ath listen` first. The image should support the docs audit that reframed ATH from a manual command surface to a model-facing agent tool workflow.

## Core message

1. A user asks Hermes in the current gateway conversation to watch/report on work.
2. Hermes uses model-facing ATH tools (`ath_create_listener`, `ath_generate_producer_handoff`) to create/reuse the route and generate a safe producer handoff.
3. The producer uses the contract/`ATH_SECRET_FILE` path, not a raw secret pasted into chat.
4. A signed `async-thread-event/v1` arrives at the local receiver and is validated for HMAC, replay window, route scope, and de-dupe.
5. The registry resolves `threadKey` to the stored origin session, then policy chooses direct notification or `agent_queue` continuation metadata.
6. The event returns to the same mapped gateway conversation, with payload boxed as untrusted data.

## Existing image notes

The existing `baoyu-async-thread-flow.png` uses the desired dark technical vector style: deep navy background, violet cards, cyan arrows, amber active route, and a red untrusted-data boundary. It is readable and consistent with the banner.

However it begins with "Producer event" and centers the technical receiver path. That is now slightly stale after the docs audit: the public story should start with a normal user asking Hermes to watch work, then show Hermes using model-facing ATH tools and producing a safe handoff. `/ath` should be absent or clearly not the primary path.

## Regeneration target

Regenerate only the high-level flow image, keeping the same dark technical vector palette and README-friendly 16:9 format. Simplify text enough for image generation, but include these exact-ish labels where possible:

- "User asks Hermes"
- "ATH tools create route"
- "Safe producer handoff"
- "Signed event"
- "Validate + de-dupe"
- "Registry + policy"
- "Same conversation"
- "untrusted payload data"
- "MVP: gateway-local"
- "tool-first, /ath is admin/debug"

Avoid platform logos, universal support claims, desktop/CLI imagery, raw secret text, or literal manual command terminals as the primary visual.
