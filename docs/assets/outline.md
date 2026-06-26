---
illustration_set: async-threads-public-release-assets
style: minimal technical vector, dark-mode friendly
palette: deep navy, electric cyan, violet, warm amber accents
image_count: 3
---

# Async Threads public visuals

## Illustration 1
**Position**: README top banner.
**Purpose**: Give the project an identifiable visual without overstating support.
**Visual Content**: Abstract Hermes conversation thread connected to signed external event pulses and a local gateway node. No claims about every platform or desktop/CLI support.
**Filename**: async-threads-banner.png
**Type**: banner / scene-infographic hybrid
**Aspect**: 16:9 landscape

## Illustration 2
**Position**: README "How the agent-first flow works" section.
**Purpose**: Explain the agent-first gateway-local MVP flow at a glance.
**Visual Content**: User asks Hermes → model-facing ATH tools create/reuse listener → safe producer handoff with contract/secret-file path → producer sends signed event → receiver validates/de-dupes → registry and policy route direct or agent_queue continuation metadata → same mapped gateway conversation. Include explicit untrusted payload boundary, gateway-local caveat, and `/ath` as admin/debug note.
**Filename**: baoyu-async-thread-flow.png
**Type**: Baoyu infographic / flowchart
**Aspect**: 16:9 landscape

## Illustration 3
**Position**: Quickstart top section.
**Purpose**: Show the concrete first-run getting-started path, not just the conceptual architecture.
**Visual Content**: Enable plugin → user asks Hermes in current gateway conversation → ATH tools create listener/handoff → producer gets `contract.json`, helper, and `ATH_SECRET_FILE` path → signed event validates through HMAC/timestamp/scope/de-dupe filters and wakes the same conversation. Include badges for gateway-local MVP, tool-first/manual-admin distinction, and untrusted payload data.
**Filename**: getting-started-agent-first.png
**Type**: Baoyu infographic / flowchart
**Aspect**: 16:9 landscape
