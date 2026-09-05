"""Repair purchase transactions whose reference contains a one-order batch ID.

Dry-run (default):
    python scripts/fix_single_order_transaction_references.py

Apply:
    python scripts/fix_single_order_transaction_references.py --apply

Multi-order batches are deliberately not rewritten because one wallet transaction
represents the whole batch. They are reported for review instead.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from pymongo import UpdateOne


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import db  # noqa: E402


MIGRATION_ID = "single-order-transaction-reference-v1"


def main(apply: bool) -> int:
    orders_col = db["orders"]
    transactions_col = db["transactions"]
    backups_col = db["migration_backups"]

    orders_by_batch: dict[str, list[dict]] = defaultdict(list)
    for order in orders_col.find(
        {"batch_id": {"$type": "string"}, "order_id": {"$type": "string"}},
        {"order_id": 1, "batch_id": 1, "user_id": 1, "charged_amount": 1},
    ):
        orders_by_batch[order["batch_id"]].append(order)

    repairs: list[tuple[dict, dict]] = []
    multi_order_matches = 0
    user_mismatches = 0
    amount_mismatches = 0

    for transaction in transactions_col.find(
        {"type": "purchase", "reference": {"$type": "string"}},
        {"reference": 1, "user_id": 1, "amount": 1, "order_id": 1, "batch_id": 1, "order_ids": 1},
    ):
        batch_orders = orders_by_batch.get(transaction["reference"], [])
        if not batch_orders:
            continue

        matching_orders = [
            order for order in batch_orders
            if order.get("user_id") == transaction.get("user_id")
        ]
        if len(matching_orders) != len(batch_orders):
            user_mismatches += 1
            continue
        if len(matching_orders) > 1:
            multi_order_matches += 1
            continue
        if len(matching_orders) != 1:
            continue

        order = matching_orders[0]
        transaction_amount = round(float(transaction.get("amount") or 0), 2)
        order_amount = round(float(order.get("charged_amount") or 0), 2)
        if transaction_amount != order_amount:
            amount_mismatches += 1
            continue
        repairs.append((transaction, order))

    print(f"Eligible single-order transactions: {len(repairs)}")
    print(f"Multi-order batches left unchanged: {multi_order_matches}")
    print(f"User mismatches skipped: {user_mismatches}")
    print(f"Amount mismatches skipped: {amount_mismatches}")

    if not apply:
        print("Dry run only; no records were changed. Use --apply to migrate.")
        return 0

    now = datetime.now(timezone.utc)
    operations = []
    for transaction, order in repairs:
        old_reference = transaction["reference"]
        new_order_id = order["order_id"]
        backup_id = f"{MIGRATION_ID}:{transaction['_id']}"
        backups_col.update_one(
            {"_id": backup_id},
            {
                "$setOnInsert": {
                    "migration_id": MIGRATION_ID,
                    "transaction_id": transaction["_id"],
                    "old_reference": old_reference,
                    "old_order_id": transaction.get("order_id"),
                    "old_batch_id": transaction.get("batch_id"),
                    "old_order_ids": transaction.get("order_ids"),
                    "new_reference": new_order_id,
                    "backed_up_at": now,
                }
            },
            upsert=True,
        )
        operations.append(
            UpdateOne(
                {
                    "_id": transaction["_id"],
                    "user_id": transaction["user_id"],
                    "type": "purchase",
                    "reference": old_reference,
                },
                {
                    "$set": {
                        "reference": new_order_id,
                        "order_id": new_order_id,
                        "batch_id": old_reference,
                        "order_ids": [new_order_id],
                        "reference_migrated_at": now,
                        "reference_migration_id": MIGRATION_ID,
                    }
                },
            )
        )

    result = transactions_col.bulk_write(operations, ordered=False) if operations else None
    matched = result.matched_count if result else 0
    modified = result.modified_count if result else 0
    print(f"Matched during guarded update: {matched}")
    print(f"Modified transactions: {modified}")
    print(f"Rollback records stored in migration_backups: {len(repairs)}")
    return 0 if matched == len(repairs) else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Apply the migration")
    args = parser.parse_args()
    raise SystemExit(main(args.apply))
