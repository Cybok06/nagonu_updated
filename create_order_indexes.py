from pymongo import ASCENDING, DESCENDING

from db import db, campus_db


MAIN_DB_NAME = "nagobu"
CAMPUS_DB_NAME = "campus_data"


def ensure_orders_indexes(database, database_name: str) -> None:
    orders_col = database["orders"]

    index_specs = [
        ([("created_at", DESCENDING)], "created_at_desc"),
        ([("status", ASCENDING)], "status_asc"),
        ([("status", ASCENDING), ("created_at", DESCENDING)], "status_created_at"),
        ([("user_id", ASCENDING)], "user_id_asc"),
        ([("order_id", ASCENDING)], "order_id_asc"),
        ([("paid_from", ASCENDING)], "paid_from_asc"),
        ([("total_amount", ASCENDING)], "total_amount_asc"),
        ([("items.serviceName", ASCENDING)], "items_service_name"),
        ([("items.phone", ASCENDING)], "items_phone"),
        ([("items.provider", ASCENDING), ("items.line_status", ASCENDING)], "items_provider_line_status"),
    ]

    print(f"\nCreating indexes for {database_name}.orders")
    for keys, name in index_specs:
        created_name = orders_col.create_index(keys, name=name, background=True)
        print(f"  OK  {created_name}")


def main() -> None:
    ensure_orders_indexes(db, MAIN_DB_NAME)
    ensure_orders_indexes(campus_db, CAMPUS_DB_NAME)
    print("\nDone.")


if __name__ == "__main__":
    main()
