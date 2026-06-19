#!/usr/bin/env python3
"""Emit compact ATH profile-lane events from shell/background jobs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from async_threads.profile_lane import build_profile_lane_event, emit_signed_event, load_registry_handle

DEFAULT_URL = "http://127.0.0.1:8765/async-threads/v1/events"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--thread-key", required=True)
    parser.add_argument("--registry", type=Path, help="Optional ATH registry sqlite path. If omitted, use --producer-id plus ATH_SECRET env var.")
    parser.add_argument("--producer-id", default="")
    parser.add_argument("--secret-env", default="ATH_SECRET")
    parser.add_argument("--type", required=True, dest="event_type")
    parser.add_argument("--event-id", default="")
    parser.add_argument("--summary", default="")
    parser.add_argument("--lane", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--issue", default="")
    parser.add_argument("--pr", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--log-path", default="")
    parser.add_argument("--status", default="")
    parser.add_argument("--process-id", default="")
    parser.add_argument("--delegation-id", default="")
    parser.add_argument("--tail-mode", default="compact", choices=["none", "compact", "debug"])
    parser.add_argument("--payload-json", default="{}")
    parser.add_argument("--telemetry-json", default="{}")
    args = parser.parse_args()

    if args.registry:
        producer_id, secret = load_registry_handle(args.registry, args.thread_key)
    else:
        producer_id = args.producer_id
        secret = os.environ.get(args.secret_env, "")
    if not producer_id:
        raise SystemExit("missing producer id; pass --producer-id or --registry")
    if not secret:
        raise SystemExit(f"missing secret; set {args.secret_env} or pass --registry")

    try:
        payload = json.loads(args.payload_json)
        telemetry = json.loads(args.telemetry_json)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid JSON argument: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(telemetry, dict):
        raise SystemExit("--payload-json and --telemetry-json must decode to JSON objects")

    event = build_profile_lane_event(
        thread_key=args.thread_key,
        producer_id=producer_id,
        event_type=args.event_type,
        lane=args.lane,
        profile=args.profile,
        summary=args.summary,
        issue=args.issue,
        pr=args.pr,
        head=args.head,
        log_path=args.log_path,
        status=args.status,
        process_id=args.process_id,
        delegation_id=args.delegation_id,
        telemetry=telemetry,
        payload=payload,
        tail_mode=args.tail_mode,
        event_id=args.event_id,
    )
    status, body = emit_signed_event(url=args.url, event=event, secret=secret)
    print(json.dumps({"status": status, "body": body}, sort_keys=True))
    return 0 if 200 <= status < 300 else 1


if __name__ == "__main__":
    raise SystemExit(main())
