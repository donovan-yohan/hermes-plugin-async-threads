---
filename: baoyu-async-thread-flow.png
illustration_type: infographic
style: minimal technical vector
palette: deep navy, electric cyan, violet, warm amber accents
aspect: 16:9
---

Create a clear Baoyu-style technical infographic explaining the hermes-plugin-async-threads MVP flow.

LAYOUT:
- Landscape 16:9, dark-mode friendly, left-to-right flow with six numbered zones.
- Use connected panels/cards with arrows. Keep spacing generous and text large.

ZONES AND LABELS:
1. "Producer event" — external system emits `async-thread-event/v1` with `threadKey`.
2. "HMAC + replay check" — validate signature, timestamp window, route scope.
3. "De-dupe" — producer/event id prevents repeated wakeups.
4. "Registry" — resolve `threadKey` to stored Hermes `SessionSource`.
5. "Policy" — branch into `direct` notification or `agent_queue` continuation.
6. "Same gateway conversation" — Hermes posts or queues into the mapped origin thread.

BOUNDARY CALLOUTS:
- Add a visible boxed boundary around summary/subject/payload labeled "untrusted payload data".
- Add a small caveat badge: "MVP: gateway-local".
- Add a small evidence badge: "Discord-shaped path tested".

VISUAL METAPHOR:
- Signed packet enters a local receiver.
- Filters validate it.
- Registry key unlocks an existing conversation lane.
- One conversation lane lights up while other lanes stay inactive.

STYLE:
- Minimal technical vector, clean iconography, legible large labels, no clutter.
- Deep navy background, cyan arrows, violet panels, amber active lane.
- No platform logos, no desktop/CLI support imagery, no claims of universal portability.
- Avoid tiny dense text; prioritize the labels above exactly.
