import os
import json
import csv
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def raw_partition_dir(base_path: str, date_str: str) -> str:
    return os.path.join(base_path, "raw", "telegram_messages", date_str)


def processed_partition_dir(base_path: str, date_str: str) -> str:
    return os.path.join(base_path, "processed", "telegram_messages", date_str)


def write_manifest(
    *,
    base_path: str,
    date_str: str,
    channel_message_counts: Dict[str, int],
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    ensure_dir(raw_partition_dir(base_path, date_str))

    payload: Dict[str, Any] = {
        "date": date_str,
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "channels": channel_message_counts,
        "total_messages": sum(channel_message_counts.values()),
    }

    if extra:
        payload.update(extra)

    path = os.path.join(raw_partition_dir(base_path, date_str), "_manifest.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return path


def write_combined_csv(
    *,
    base_path: str,
    date_str: str,
    rows: List[Dict[str, Any]],
) -> str:
    if not rows:
        return ""

    out_dir = processed_partition_dir(base_path, date_str)
    ensure_dir(out_dir)

    path = os.path.join(out_dir, "telegram_messages.csv")
    fieldnames = sorted(rows[0].keys())

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return path
