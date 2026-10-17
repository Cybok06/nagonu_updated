"""
Create MongoDB indexes for dashboard and admin hot paths.

Run:
    python api_test/create_runtime_indexes.py
"""

from __future__ import annotations

from pymongo import ASCENDING, DESCENDING, IndexModel

from db import db, campus_db


MAIN_INDEX_PLAN = {
    "orders": [
        IndexModel([("created_at", DESCENDING)], name="created_at_desc"),
        IndexModel([("status", ASCENDING), ("created_at", DESCENDING)], name="status_created_at_desc"),
        IndexModel(
            [("paid_from", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING)],
            name="paid_from_status_created_at_desc",
        ),
        IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)], name="user_id_created_at_desc"),
        IndexModel([("order_id", ASCENDING)], name="order_id_asc"),
        IndexModel([("total_amount", DESCENDING), ("created_at", DESCENDING)], name="total_amount_created_at_desc"),
        IndexModel([("items.phone", ASCENDING)], name="items_phone_asc"),
        IndexModel([("items.serviceName", ASCENDING)], name="items_service_name_asc"),
        IndexModel([("items.provider", ASCENDING), ("created_at", DESCENDING)], name="items_provider_created_at_desc"),
        IndexModel([("paystack_reference", ASCENDING)], name="paystack_reference_asc", sparse=True),
    ],
    "users": [
        IndexModel([("role", ASCENDING), ("status", ASCENDING)], name="role_status_asc"),
        IndexModel([("phone", ASCENDING)], name="phone_asc", sparse=True),
        IndexModel([("email", ASCENDING)], name="email_asc", sparse=True),
    ],
    "balance_logs": [
        IndexModel([("action", ASCENDING), ("created_at", DESCENDING)], name="action_created_at_desc"),
        IndexModel([("created_at", DESCENDING)], name="created_at_desc"),
    ],
    "balances": [
        IndexModel([("user_id", ASCENDING)], name="user_id_asc"),
    ],
    "afa_registrations": [
        IndexModel([("status", ASCENDING), ("created_at", DESCENDING)], name="status_created_at_desc"),
        IndexModel([("created_at", DESCENDING)], name="created_at_desc"),
    ],
    "store_withdraw_requests": [
        IndexModel([("status", ASCENDING), ("created_at", DESCENDING)], name="status_created_at_desc"),
        IndexModel([("reference", ASCENDING)], name="reference_asc", sparse=True),
        IndexModel([("store_slug", ASCENDING), ("created_at", DESCENDING)], name="store_slug_created_at_desc", sparse=True),
    ],
    "store_accounts": [
        IndexModel([("store_slug", ASCENDING)], name="store_slug_asc", sparse=True),
    ],
}

CAMPUS_INDEX_PLAN = {
    "orders": [
        IndexModel([("created_at", DESCENDING)], name="created_at_desc"),
        IndexModel([("status", ASCENDING), ("created_at", DESCENDING)], name="status_created_at_desc"),
        IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)], name="user_id_created_at_desc"),
        IndexModel([("order_id", ASCENDING)], name="order_id_asc"),
        IndexModel([("total_amount", DESCENDING), ("created_at", DESCENDING)], name="total_amount_created_at_desc"),
        IndexModel([("items.phone", ASCENDING)], name="items_phone_asc"),
        IndexModel([("items.serviceName", ASCENDING)], name="items_service_name_asc"),
        IndexModel([("items.provider", ASCENDING), ("created_at", DESCENDING)], name="items_provider_created_at_desc"),
    ],
}


def ensure_indexes(database, label: str, plan: dict) -> None:
    print(f"\n=== {label.upper()} DATABASE ===")
    for collection_name, models in plan.items():
        collection = database[collection_name]
        print(f"\n[{label}.{collection_name}] ensuring {len(models)} indexes")
        try:
            created = collection.create_indexes(models)
        except Exception as exc:
            print(f"  ! failed: {exc}")
            continue

        for name in created:
            print(f"  - {name}")


def main() -> None:
    print("Starting index creation...")
    ensure_indexes(db, "main", MAIN_INDEX_PLAN)
    ensure_indexes(campus_db, "campus", CAMPUS_INDEX_PLAN)
    print("\nDone.")


if __name__ == "__main__":
    main()
