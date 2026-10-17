"""Refund Campus wallet deductions and provider costs in the Campus database."""
from datetime import datetime
from decimal import Decimal, InvalidOperation

from bson import ObjectId
from db import campus_db


def _amount(value):
    try:
        value = Decimal(str(value))
        return float(value.quantize(Decimal('0.01'))) if value.is_finite() and value > 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _base_cost(order, item, index, session):
    # The original wallet debit is authoritative even after service prices change.
    # Checkout reserves with one-based cart positions, retained after splitting.
    position = order.get('batch_position') if order.get('batch_id') else index + 1
    if not order.get('batch_id') and order.get('order_id'):
        zero_based = campus_db['provider_transactions'].find_one({
            'provider': 'provider_wallet', 'direction': 'DEBIT',
            'order_id': order['order_id'], 'line_index': 0,
        }, session=session)
        if zero_based:
            position = index
    debit = campus_db['provider_transactions'].find_one({
        'provider': 'provider_wallet', 'direction': 'DEBIT',
        'order_id': order.get('order_id'), 'line_index': position,
    }, session=session) if order.get('order_id') else None
    if not debit and order.get('batch_id'):
        debit = campus_db['provider_transactions'].find_one({
            'provider': 'provider_wallet', 'direction': 'DEBIT',
            'order_id': order['batch_id'], 'line_index': position,
        }, session=session)
    if debit and _amount(debit.get('amount')):
        return _amount(debit['amount']), 'original_campus_debit'
    for key in ('campus_base_amount', 'provider_base_amount', 'base_amount'):
        if _amount(item.get(key)):
            return _amount(item[key]), key

    service_id = item.get('serviceId') or item.get('service_id')
    service = None
    if service_id:
        ids = [service_id]
        if ObjectId.is_valid(str(service_id)):
            ids.append(ObjectId(str(service_id)))
        service = campus_db['services'].find_one({'_id': {'$in': ids}}, session=session)
    if not service and item.get('serviceName'):
        candidates = list(campus_db['services'].find({'name': item['serviceName']}, session=session))
        service = candidates[0] if len(candidates) == 1 else None
    if not service:
        raise ValueError('Campus service base price could not be identified.')
    from routes.admin_campus_services import _parse_volume_to_mb
    value = item.get('value_obj') or item.get('value')
    if isinstance(value, dict):
        volume = _parse_volume_to_mb(value.get('volume'))
    else:
        volume = _parse_volume_to_mb(value)
    key = 'store_offers' if order.get('store_slug') and service.get('store_offers') else 'offers'
    matches = []
    for offer in service.get(key) or []:
        offer_value = offer.get('value')
        offer_volume = _parse_volume_to_mb(offer_value.get('volume')) if isinstance(offer_value, dict) else _parse_volume_to_mb(offer_value)
        if (volume is not None and volume == offer_volume) or (volume is None and value and value == offer_value):
            amount = _amount(offer.get('amount'))
            if amount:
                matches.append(amount)
    if len(set(matches)) != 1:
        raise ValueError('Campus offer base price is missing or ambiguous; refund was not credited.')
    return matches[0], 'campus_service_offer'


def _wallet_debit(order, item, session):
    """Resolve the payer and the original line charge, never today's offer price."""
    owner = order.get('wallet_user_id') or order.get('user_id')
    if owner and ObjectId.is_valid(str(owner)):
        owner = ObjectId(str(owner))
    purchase = campus_db['transactions'].find_one({
        'user_id': owner, 'reference': order.get('batch_id') or order.get('order_id'),
        'type': 'purchase', 'status': 'success', 'gateway': 'Wallet',
    }, session=session) if owner else None
    # Store Paystack payments have no customer-wallet debit to reverse.
    if not order.get('wallet_user_id') and order.get('paid_from') != 'wallet' and not purchase:
        if order.get('store_slug') or order.get('paid_from'):
            return None, 0.0, 'no_wallet_debit'
        # Historical direct Campus checkout always deducted the user wallet.
    if not owner:
        raise ValueError('Campus order has no identifiable wallet payer.')
    value = item.get('wallet_debit_amount')
    basis = 'original_wallet_debit_snapshot'
    if value is None and len(order.get('items') or []) == 1:
        value = order.get('wallet_debit_amount', order.get('charged_amount'))
        basis = 'original_order_charge'
    if value is None:
        value = item.get('amount')
        basis = 'original_line_charge'
    amount = _amount(value)
    if amount is None:
        raise ValueError('Original Campus wallet deduction is unavailable; refund was not credited.')
    if purchase and amount > (_amount(purchase.get('amount')) or 0):
        raise ValueError('Campus line charge exceeds the recorded wallet deduction.')
    return owner, amount, basis


def credit_campus_line_refund(order, index, *, reason, actor_admin_id=None):
    """Commit both balances, both histories, and the line marker together."""
    reference = f"CAMPUS:{order['_id']}:LINE:{index}:REFUND"
    # Concurrent refunds must share one account, including its first creation.
    campus_db['provider_accounts'].create_index('provider', unique=True)

    def commit(session):
        orders = campus_db['orders']
        ledger = campus_db['provider_transactions']
        current = orders.find_one({'_id': order['_id']}, session=session)
        if not current or index >= len(current.get('items') or []):
            raise ValueError('Campus order item no longer exists.')
        item = current['items'][index]
        existing = ledger.find_one({'_id': reference}, session=session)
        if existing:
            return existing['amount']
        if item.get('refunded_at') or item.get('line_status') == 'refunded':
            return item.get('refund_amount') or 0.0
        if str(item.get('line_status') or '').startswith('skipped'):
            raise ValueError('Skipped order items cannot be refunded.')
        amount, basis = _base_cost(current, item, index, session)
        wallet_owner, wallet_amount, wallet_basis = _wallet_debit(current, item, session)
        now = datetime.utcnow()
        if wallet_amount:
            wallet_reference = reference + ':WALLET'
            campus_db['transactions'].insert_one({
                '_id': wallet_reference, 'reference': wallet_reference,
                'user_id': wallet_owner, 'amount': wallet_amount,
                'order_id': current.get('order_id'), 'type': 'refund',
                'status': 'success', 'gateway': 'Wallet', 'currency': 'GHS',
                'created_at': now, 'verified_at': now,
                'meta': {'note': f'Campus wallet refund ({reason})',
                         'order_db_id': current['_id'], 'line_index': index,
                         'batch_id': current.get('batch_id'),
                         'refund_basis': wallet_basis, 'actor_admin_id': actor_admin_id},
            }, session=session)
            campus_db['balances'].update_one({'user_id': wallet_owner}, {
                '$inc': {'amount': wallet_amount}, '$set': {'updated_at': now},
            }, upsert=True, session=session)
        ledger.insert_one({
            '_id': reference, 'provider': 'provider_wallet', 'amount': amount,
            'direction': 'CREDIT', 'reason': 'REFUNDED', 'status': 'success',
            'reference': reference, 'dedupe_key': reference,
            'order_id': current.get('order_id'), 'line_index': index, 'created_at': now,
            'meta': {'note': f'Campus order refund ({reason})', 'actor_admin_id': actor_admin_id,
                     'order_db_id': current['_id'], 'service_name': item.get('serviceName'),
                     'value': item.get('value'), 'phone': item.get('phone'),
                     'service_provider': item.get('provider'), 'refund_basis': basis,
                     'customer_paid_amount': item.get('amount')},
        }, session=session)
        campus_db['provider_accounts'].update_one({'provider': 'provider_wallet'}, {
            '$inc': {'balance': amount}, '$set': {'updated_at': now},
            '$setOnInsert': {'created_at': now},
        }, upsert=True, session=session)
        fields = {
            f'items.{index}.line_status': 'refunded', f'items.{index}.refunded_at': now,
            f'items.{index}.refund_amount': amount, f'items.{index}.refunded_by': actor_admin_id,
            f'items.{index}.refund_basis': basis, f'items.{index}.refund_destination': 'campus_balance',
            f'items.{index}.wallet_refund_amount': wallet_amount,
            f'items.{index}.wallet_refund_user_id': wallet_owner,
            f'items.{index}.wallet_refund_basis': wallet_basis,
            f'items.{index}.wallet_refunded_at': now if wallet_amount else None,
            f'items.{index}.refund_required': False, 'updated_at': now,
        }
        statuses = ['refunded' if position == index else candidate.get('line_status')
                    for position, candidate in enumerate(current['items'])
                    if not str(candidate.get('line_status') or '').startswith('skipped')]
        if statuses and all(status == 'refunded' for status in statuses):
            fields.update(status='refunded', refunded_at=now)
        orders.update_one({'_id': current['_id']}, {'$set': fields}, session=session)
        return amount

    with campus_db.client.start_session() as session:
        return session.with_transaction(commit)
