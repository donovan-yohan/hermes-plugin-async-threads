---
filename: getting-started-agent-first.png
illustration_type: infographic
style: minimal technical vector
palette: deep navy, electric cyan, violet, warm amber accents, red untrusted-data boundary
aspect: 16:9
source: docs/QUICKSTART.md agent-first getting started workflow
---

Create a clear Baoyu-style technical infographic for the public `hermes-plugin-async-threads` getting-started docs.

GOAL:
- Show the first successful user journey for ATH as agent-first: a user asks Hermes to watch work and report back here, then Hermes uses model-facing ATH tools.
- Make the manual `/ath` commands secondary admin/debug, not the main setup path.
- Show safe producer handoff and signed event delivery without exposing raw secrets.
- Keep MVP caveats honest: gateway-local receiver, live gateway origin required, payload stays untrusted data.

LAYOUT:
- Landscape 16:9, dark-mode friendly.
- Title at top: "Getting started: agent-first ATH".
- Five large numbered cards from left to right, connected by cyan arrows.
- Bottom strip with three compact safety badges.
- Use large readable labels; keep supporting text short.

MAIN CARDS AND LABELS:
1. "Enable plugin" — small receiver tile: `POST /async-threads/v1/events`; health check dot; supporting text: "gateway process + adapter".
2. "User asks Hermes" — chat bubble in the current gateway conversation; supporting text: "watch this job and report here".
3. "ATH tools create listener" — two tool chips: `ath_create_listener` and `ath_generate_producer_handoff`; supporting text: "current conversation → threadKey".
4. "Producer gets handoff" — file cards: `contract.json`, `emit_async_thread_event.py`, `ATH_SECRET_FILE`; supporting text: "paths, not raw secret".
5. "Signed event wakes thread" — signed packet passes HMAC, timestamp, route scope, de-dupe filters and lights up the same conversation lane.

SAFETY BADGES:
- Amber badge: "MVP: gateway-local".
- Violet/cyan badge: "tool-first; /ath = admin/debug".
- Red dashed badge: "payload is untrusted data".
- Tiny note near tool policy: "agent_queue policy metadata; fail-closed for strict bounds".

STYLE:
- Minimal technical vector, polished README/docs infographic, clean iconography.
- Deep navy background; violet cards; cyan arrows and security marks; warm amber active conversation lane; red dashed untrusted-data boundary.
- No platform logos. No Discord/Telegram/Slack logos. No desktop/CLI universal-support imagery.
- Do not show a literal HMAC secret. Do not show raw logs. Do not show manual command terminals as the main path.

QUALITY:
- Optimized for README/Quickstart legibility.
- Main labels should be spelled correctly and large enough to read.
- If small supporting text is imperfect, prioritize the five main labels and three safety badges.
