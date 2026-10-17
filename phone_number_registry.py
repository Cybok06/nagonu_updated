from __future__ import annotations

import re
import threading
import time
from typing import Any, Dict, List, Mapping

from pymongo import ASCENDING, UpdateOne

from db import db


phone_numbers_col = db["phone_numbers"]
_index_lock = threading.Lock()
_index_ready = False


def normalize_registry_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if re.fullmatch(r"233\d{9}", digits):
        return "0" + digits[3:]
    if re.fullmatch(r"\d{9}", digits):
        return "0" + digits
    if re.fullmatch(r"0\d{9}", digits):
        return digits
    return ""


def order_phone_numbers(order: Mapping[str, Any]) -> List[str]:
    candidates: List[Any] = [order.get("buyer_phone"), order.get("dial_phone")]
    ussd = order.get("ussd")
    if isinstance(ussd, Mapping):
        candidates.append(ussd.get("dial_phone"))
    for item in order.get("items") or []:
        if isinstance(item, Mapping):
            candidates.append(item.get("phone"))
    normalized = [normalize_registry_phone(value) for value in candidates]
    return list(dict.fromkeys(phone for phone in normalized if phone))


def order_agent_id(order: Mapping[str, Any]) -> str:
    return str(
        order.get("agent_user_id")
        or order.get("user_id")
        or order.get("store_owner_id")
        or ""
    ).strip()


def _ensure_unique_index() -> None:
    global _index_ready
    if _index_ready:
        return
    with _index_lock:
        if _index_ready:
            return
        phone_numbers_col.create_index(
            [("phone_number", ASCENDING)],
            name="uq_phone_numbers_phone_number",
            unique=True,
            background=True,
        )
        _index_ready = True


def register_order_phone_numbers(order: Mapping[str, Any]) -> Dict[str, int]:
    """Add missing order recipients atomically without changing existing owners."""
    phones = order_phone_numbers(order)
    if not phones:
        return {"phones": 0, "inserted": 0}
    _ensure_unique_index()
    agent_id = order_agent_id(order)
    operations = [
        UpdateOne(
            {"phone_number": phone},
            {"$setOnInsert": {"phone_number": phone, "agent_id": agent_id}},
            upsert=True,
        )
        for phone in phones
    ]
    result = phone_numbers_col.bulk_write(operations, ordered=False)
    return {"phones": len(phones), "inserted": int(result.upserted_count)}


def register_phone_number(phone_number: Any, agent_id: Any) -> bool:
    """Register one phone atomically; return True only when newly added."""
    normalized = normalize_registry_phone(phone_number)
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized:
        raise ValueError("invalid_phone_number")
    if not normalized_agent_id:
        raise ValueError("missing_agent_id")
    _ensure_unique_index()
    result = phone_numbers_col.update_one(
        {"phone_number": normalized},
        {"$setOnInsert": {"phone_number": normalized, "agent_id": normalized_agent_id}},
        upsert=True,
    )
    return bool(result.upserted_id)


def register_order_phone_numbers_async(order: Mapping[str, Any]) -> None:
    """Update the registry outside checkout latency, retrying transient failures."""
    snapshot = {
        "user_id": order.get("user_id"),
        "agent_user_id": order.get("agent_user_id"),
        "store_owner_id": order.get("store_owner_id"),
        "buyer_phone": order.get("buyer_phone"),
        "dial_phone": order.get("dial_phone"),
        "ussd": order.get("ussd"),
        "items": order.get("items") or [],
    }

    def worker() -> None:
        for attempt in range(3):
            try:
                register_order_phone_numbers(snapshot)
                return
            except Exception as exc:
                if attempt == 2:
                    print(f"Phone registry update failed: {exc}")
                    return
                time.sleep(0.25 * (2 ** attempt))

    threading.Thread(target=worker, daemon=True, name="nagonu-phone-registry").start()
