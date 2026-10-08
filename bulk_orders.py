"""Single-order persistence for Nagonu dashboard bulk checkout."""
from copy import deepcopy
from datetime import datetime
from decimal import Decimal


def money(value):
    return float(Decimal(str(value or 0)).quantize(Decimal("0.01")))


def split_documents(order):
    items = order.get("items") or []
    if len(items) <= 1:
        return [deepcopy(order)]
    documents = []
    for position, item in enumerate(items, 1):
        document = deepcopy(order)
        document.pop("_id", None)
        skipped = str(item.get("line_status", "")).startswith("skipped")
        charge = 0.0 if skipped else money(item.get("amount"))
        document.update(order_id=f"{order['order_id']}-{position}",
                        batch_order_id=order['order_id'], batch_position=position,
                        batch_size=len(items), items=[deepcopy(item)],
                        total_amount=money(item.get("amount")), charged_amount=charge,
                        profit_amount_total=0.0 if skipped else money(item.get("profit_amount")),
                        status="skipped" if skipped else item.get("line_status") or "pending")
        document['items'][0]['wallet_debit_amount'] = charge
        documents.append(document)
    if money(sum(d['charged_amount'] for d in documents)) != money(order.get('charged_amount')):
        raise ValueError("Bulk item charges do not match the wallet payment.")
    return documents


def persist_bulk(database, order, transaction=None):
    """Commit all children, one wallet debit, and one payment together."""
    documents = split_documents(order)
    amount = money(order.get("charged_amount"))
    with database.client.start_session() as session:
        def commit(active_session):
            if amount:
                result = database.balances.update_one(
                    {"user_id": order['user_id'], "amount": {"$gte": amount}},
                    {"$inc": {"amount": -amount}, "$set": {"updated_at": datetime.utcnow()}},
                    session=active_session)
                if not result.matched_count:
                    raise ValueError("Insufficient wallet balance for this bulk order.")
            database.orders.insert_many(deepcopy(documents), session=active_session)
            if transaction:
                database.transactions.insert_one(deepcopy(transaction), session=active_session)
            return documents
        return session.with_transaction(commit)


def load_batch(collection, batch_id, user_id=None):
    query = {"batch_order_id": batch_id}
    if user_id is not None:
        query['user_id'] = user_id
    children = list(collection.find(query).sort('batch_position', 1))
    if not children:
        return None
    order = deepcopy(children[0])
    order.update(order_id=batch_id, items=[i for child in children for i in child.get('items', [])],
                 total_amount=money(sum(c.get('total_amount', 0) for c in children)),
                 charged_amount=money(sum(c.get('charged_amount', 0) for c in children)),
                 profit_amount_total=money(sum(c.get('profit_amount_total', 0) for c in children)),
                 order_ids=[c['order_id'] for c in children])
    statuses = [c.get('status') for c in children if c.get('status') != 'skipped']
    order['status'] = statuses[0] if statuses and len(set(statuses)) == 1 else 'processing' if statuses else 'skipped'
    return order
