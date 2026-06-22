---
filename: baoyu-async-thread-flow.png
illustration_type: infographic
style: minimal technical vector
palette: deep navy, electric cyan, violet, warm amber accents, red untrusted-data boundary
aspect: 16:9
source: README.md agent-first ATH workflow
---

Create a clear Baoyu-style technical infographic explaining the agent-first `hermes-plugin-async-threads` MVP flow.

GOAL:
- Reframe ATH as model-facing agent tools first, not a manual `/ath listen` command.
- Show the producer handoff as a safe artifact path/contract, not a raw secret pasted into chat.
- Keep the MVP caveat visible: gateway-local, not universal platform support.

LAYOUT:
- Landscape 16:9, dark-mode friendly.
- Top row: seven numbered connected cards from left to right.
- Bottom row: a simplified packet/receiver/conversation-lanes visual that echoes the top row.
- Use connected panels/cards with cyan arrows. Keep spacing generous and labels large.
- Make the flow readable in a README, not a dense poster.

TOP ROW ZONES AND LABELS:
1. "User asks Hermes" — icon: chat bubble in the current conversation; small supporting text: "watch this job and report here".
2. "ATH tools create route" — show two small tool chips: `ath_create_listener` and `ath_generate_producer_handoff`.
3. "Safe producer handoff" — show `contract.json` plus `ATH_SECRET_FILE`; supporting text: "paths, not raw secret".
4. "Signed event" — external producer emits `async-thread-event/v1` with `threadKey`.
5. "Validate + de-dupe" — HMAC seal, replay window clock, route scope target, duplicate filter.
6. "Registry + policy" — resolve `threadKey`; branch to `direct` or `agent_queue` continuation policy metadata.
7. "Same conversation" — one mapped gateway conversation lane lights up; other lanes remain muted.

BOUNDARY CALLOUTS:
- Add a red dashed boundary around summary/subject/payload labeled "untrusted payload data".
- Add a small amber caveat badge: "MVP: gateway-local".
- Add a small violet/cyan badge: "tool-first; /ath is admin/debug".
- Add a tiny policy note near `agent_queue`: "coreEnforced: false unless fail-closed". Keep it small but legible if possible.

VISUAL METAPHOR:
- The user conversation creates a durable route before the producer event exists.
- The safe handoff is a document/file path passed to the producer.
- The signed packet enters a local receiver, passes validation filters, unlocks registry routing, then wakes the correct existing conversation lane.

STYLE:
- Minimal technical vector, clean iconography, legible large labels, no clutter.
- Deep navy background; violet cards; cyan arrows and security marks; warm amber active conversation lane; red dashed untrusted payload boundary.
- No platform logos. No Discord/Telegram/Slack logos. No desktop or CLI support imagery. No claims of universal portability.
- Do not show raw HMAC secret text.
- Do not make manual `/ath` command terminals the main image; `/ath` appears only as the small admin/debug note.
- Avoid tiny dense text; prioritize the labels above.

QUALITY:
- The image should feel like a polished technical README infographic.
- Strong hierarchy: title, numbered flow, caveats, untrusted-data boundary.
- If some small supporting text cannot be perfect, keep the main labels clean and readable.
