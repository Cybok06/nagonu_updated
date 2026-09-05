from __future__ import annotations

import random
from datetime import datetime

from pymongo import ASCENDING

from db import db


users_col = db["users"]
agent_codes_col = db["agent_codes"]


def _new_agent_code(existing_codes: set[str]) -> str:
    for _ in range(100000):
        code = str(random.randint(10000, 99999))
        if code not in existing_codes:
            existing_codes.add(code)
            return code
    raise RuntimeError("Could not find an unused 5-digit agent code")


def generate_agent_codes() -> dict[str, int]:
    agent_codes_col.create_index([("agent_code", ASCENDING)], unique=True)
    agent_codes_col.create_index([("user_id", ASCENDING)], unique=True)
    agent_codes_col.create_index([("status", ASCENDING)])

    now = datetime.utcnow()
    existing_codes = {
        str(doc.get("agent_code"))
        for doc in agent_codes_col.find({"agent_code": {"$exists": True, "$ne": ""}}, {"agent_code": 1})
    }

    users = list(
        users_col.find(
            {
                "role": "customer",
                "$or": [{"deleted": {"$exists": False}}, {"deleted": False}],
            },
            {"_id": 1},
        )
    )

    created = 0
    updated = 0
    skipped = 0

    for user in users:
        user_id = user["_id"]
        existing = agent_codes_col.find_one({"user_id": user_id})
        if existing:
            update = {}
            if not existing.get("id"):
                update["id"] = str(existing["_id"])
            if not existing.get("status"):
                update["status"] = "active"
            if update:
                update["updated_at"] = now
                agent_codes_col.update_one({"_id": existing["_id"]}, {"$set": update})
                updated += 1
            else:
                skipped += 1
            continue

        code = _new_agent_code(existing_codes)
        result = agent_codes_col.insert_one(
            {
                "user_id": user_id,
                "agent_code": code,
                "status": "active",
                "created_at": now,
                "updated_at": now,
            }
        )
        agent_codes_col.update_one({"_id": result.inserted_id}, {"$set": {"id": str(result.inserted_id)}})
        created += 1

    return {
        "users_seen": len(users),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "total_codes": agent_codes_col.count_documents({}),
    }


if __name__ == "__main__":
    result = generate_agent_codes()
    print("Agent code generation complete")
    for key, value in result.items():
        print(f"{key}: {value}")
