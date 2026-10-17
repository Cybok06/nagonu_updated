"""Atomic base-price refunds for main dashboard and store orders."""
from datetime import datetime
from decimal import Decimal, InvalidOperation

from bson import ObjectId
from db import db


def base_price(order, item):
    store_order = bool(order.get('store_slug') or (order.get('debug') or {}).get('store_checkout'))
    value = item.get('base_amount') if store_order else item.get('wallet_debit_amount')
    if not store_order and value is None:
        value = order.get('charged_amount') if len(order.get('items') or []) == 1 else None
        if value is None:
            value = item.get('amount')
    if value is None and store_order:
        if item.get('store_profit_amount') is not None:
            try:
                value = Decimal(str(item['amount'])) - Decimal(str(item['store_profit_amount']))
            except (InvalidOperation, KeyError, TypeError, ValueError):
                pass
    try:
        amount = Decimal(str(value)).quantize(Decimal('0.01'))
        if amount.is_finite() and amount > 0:
            return float(amount)
    except (InvalidOperation, ValueError, TypeError):
        pass
    raise ValueError('Original base price is unavailable. Refund was not credited.')


def refund_owner(order, session=None):
    owner = order.get('user_id')
    if order.get('store_slug') or (order.get('debug') or {}).get('store_checkout'):
        store = db['stores'].find_one({'slug': order.get('store_slug')}, session=session) or {}
        owner = order.get('store_owner_id') or store.get('owner_id')
    if ObjectId.is_valid(str(owner)):
        owner = ObjectId(str(owner))
    user = db['users'].find_one({'_id': owner}, session=session) if owner else None
    if not user:
        raise ValueError('Original wallet owner is unavailable. Refund was not credited.')
    return owner, user


def refund_preview(order_id, indices=None):
    order = db['orders'].find_one({'_id': order_id})
    if not order:
        raise ValueError('Order not found.')
    _, user = refund_owner(order)
    items = order.get('items') or []
    selected = sorted(set(indices)) if indices is not None else range(len(items))
    total = Decimal('0')
    for index in selected:
        if index < 0 or index >= len(items):
            raise ValueError('Order item not found.')
        item = items[index]
        if (item.get('refunded_at') or item.get('line_status') == 'refunded'
                or order.get('refunded_at') or order.get('status') == 'refunded'
                or str(item.get('line_status') or '').startswith('skipped') or item.get('api_status') == 'skipped'):
            continue
        reference = f"{order.get('order_id') or order['_id']}:LINE:{index}:REFUND"
        if not db['transactions'].find_one({'type': 'refund', 'reference': reference}):
            total += Decimal(str(base_price(order, item)))
    name = (user.get('full_name') or ' '.join(filter(None, [user.get('first_name'), user.get('last_name')]))
            or user.get('username') or user.get('name') or user.get('email') or str(user['_id']))
    return {'amount': float(total), 'recipient': name}


def refund_order(order_id, indices=None, *, reason='manual', actor_admin_id=None):
    """Credit selected items and save their status in one MongoDB transaction."""
    def commit(session):
        orders, ledger = db['orders'], db['transactions']
        order = orders.find_one({'_id': order_id}, session=session)
        if not order:
            raise ValueError('Order not found.')
        items = order.get('items') or []
        selected = sorted(set(indices)) if indices is not None else list(range(len(items)))
        if not selected:
            raise ValueError('Order has no refundable items.')
        if any(not isinstance(i, int) or i < 0 or i >= len(items) for i in selected):
            raise ValueError('Order item not found.')
        owner, _ = refund_owner(order, session)
        now, credited, changed = datetime.utcnow(), Decimal('0'), 0
        fields = {'updated_at': now}
        for index in selected:
            item = items[index]
            if str(item.get('line_status') or '').startswith('skipped') or item.get('api_status') == 'skipped':
                if indices is not None:
                    raise ValueError('Skipped items were not charged and cannot be refunded.')
                continue
            reference = f"{order.get('order_id') or order['_id']}:LINE:{index}:REFUND"
            existing = ledger.find_one({'type': 'refund', 'reference': reference}, session=session)
            if item.get('refunded_at') or item.get('line_status') == 'refunded' or order.get('refunded_at') or order.get('status') == 'refunded':
                continue
            amount = existing['amount'] if existing else base_price(order, item)
            if not existing:
                ledger.insert_one({'_id': reference, 'reference': reference, 'type': 'refund',
                    'user_id': owner, 'amount': amount, 'order_id': order.get('order_id'),
                    'status': 'success', 'gateway': 'Wallet', 'currency': 'GHS',
                    'created_at': now, 'verified_at': now,
                    'meta': {'note': f'{reason.capitalize()} base-price refund',
                             'order_db_id': order['_id'], 'line_index': index,
                             'refund_basis': 'original_store_base_amount' if order.get('store_slug') or (order.get('debug') or {}).get('store_checkout') else 'original_wallet_charge', 'store_slug': order.get('store_slug'),
                             'actor_admin_id': actor_admin_id}}, session=session)
                credited += Decimal(str(amount))
            item.update(line_status='refunded', refunded_at=now, refund_amount=amount, refunded_by=actor_admin_id)
            for key in ('line_status', 'refunded_at', 'refund_amount', 'refunded_by'):
                fields[f'items.{index}.{key}'] = item[key]
            changed += 1
        if credited:
            db['balances'].update_one({'user_id': owner},
                {'$inc': {'amount': float(credited)}, '$set': {'updated_at': now},
                 '$setOnInsert': {'currency': 'GHS', 'created_at': now}}, upsert=True, session=session)
        if changed:
            active = [i for i in items if not str(i.get('line_status') or '').startswith('skipped') and i.get('api_status') != 'skipped']
            if active and all(i.get('line_status') == 'refunded' for i in active):
                fields.update(status='refunded', refunded_at=now)
            orders.update_one({'_id': order['_id']}, {'$set': fields}, session=session)
        return changed, float(credited)

    with db.client.start_session() as session:
        return session.with_transaction(commit)
