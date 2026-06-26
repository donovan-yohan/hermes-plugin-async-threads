# Changelog

## 0.2.0 - 2026-06-26

### Added

- Producer-agnostic source-binding registry for connecting external event streams to existing ATH listeners without retargeting the listener.
- Kanban `task_events` source adapter with material-transition filtering for `blocked`, `completed`, `crashed`, `gave_up`, `timed_out`, and `ready_for_review` task events.
- Dry-run and inspection surfaces for source bindings, including cursor preview, compatibility checks, suppressed-event counts, and redacted diagnostics.
- Config-gated native source-binding runner with durable cursor and outbox state; no Hermes cron job is required for the intended path.
- Reusable ATH emitter helper and producer handoff improvements for local producers and bridge authors.
- Kanban source-binding docs and a runnable example walkthrough under `examples/kanban-source-binding/`.

### Changed

- Public docs now describe Kanban source-binding dogfood, runner lifecycle, trace diagnostics, and cron as emergency fallback only.
- Source-runner emit diagnostics now classify failures instead of persisting raw receiver bodies or transcript-like sentinel text.
- Nonretryable emit failures become terminal cursor-advancing error rows so poison events do not re-emit forever; retryable transport failures stay pending and cursor-pinned.

### Fixed

- `async_threads` gateway adapter now accepts gateway reconnect keyword arguments such as `is_reconnect`, fixing post-restart reconnect failures in newer Hermes gateway runs.

### Dogfood evidence

- The `ath` Kanban board was bound to listener `ath_mg3BQeDs15Gm4DnF` through source binding `athb_DwdgXejUywToFc95`.
- Native post-restart runner delivery was verified for `ath:t_04fc5876:541` with HTTP `202`, receiver status `queued`, and ATH trace outcome `queued_active_session`.
- The old interim SQL daemon and cron-style polling path were retired for the dogfood flow.

## 0.1.0 - 2026-06-22

- Initial MVP release for signed async-thread event delivery into mapped Hermes gateway conversations.
