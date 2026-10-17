"""Complaint refunds using purchase snapshots, committed with wallet history."""
import ast
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from bson import ObjectId
from db import db


def _money(value):
    try:
        amount = Decimal(str(value)).quantize(Decimal('0.01'))
        if amount.is_finite() and amount > 0:
            return float(amount)
    except (InvalidOperation, ValueError, TypeError):
        pass
    return None


def _offer(value):
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            pass
    if isinstance(value, dict):
        value = value.get('volume', value.get('value'))
    text = str(value or '').strip().lower().replace(' ', '')
    match = re.fullmatch(r'(\d+(?:\.\d+)?)(gb|mb)?', text)
    if match:
        return float(match[1]) * (1000 if match[2] == 'gb' else 1)
    return text


def _selected_lines(complaint, order):
    items = order.get('items') or []
    if complaint.get('order_line_index') is not None:
        index = complaint['order_line_index']
        if not isinstance(index, int) or index < 0 or index >= len(items):
            raise ValueError('Complaint order item no longer exists.')
        return [index]
    selections = complaint.get('cart_snapshot') or [{
        'serviceName': complaint.get('service_name'), 'value': complaint.get('offer')}]
    selected = []
    for selection in selections:
        service = selection.get('serviceName') or selection.get('service_name')
        value = selection.get('value_obj') or selection.get('value') or selection.get('offer')
        phone = selection.get('phone')
        matches = [i for i, item in enumerate(items) if i not in selected
                   and service and str(item.get('serviceName') or '').strip().lower() == str(service).strip().lower()
                   and value is not None and _offer(item.get('value_obj') or item.get('value')) == _offer(value)
                   and (not phone or re.sub(r'\D', '', str(item.get('phone') or ''))[-9:] == re.sub(r'\D', '', str(phone))[-9:])]
        if len(matches) != 1:
            raise ValueError('Complaint service/offer does not identify one purchased item. Refund was not credited.')
        selected.append(matches[0])
    return selected


def refund_complaint(complaint_id, actor):
    """Atomically credit the original payer and mark the complaint and items."""
    def commit(session):
        complaints, orders, ledger = db['complaints'], db['orders'], db['transactions']
        complaint = complaints.find_one({'_id': complaint_id}, session=session)
        if not complaint:
            raise ValueError('Complaint not found.')
        if complaint.get('refund_completed_at'):
            return complaint['refund_amount'], False
        ref = complaint.get('order_ref') or {}
        scope = {'store_slug': complaint['store_slug']} if complaint.get('store_slug') else {'user_id': complaint.get('user_id')}
        identifiers = []
        if complaint.get('paystack_reference'):
            identifiers = [{'paystack_reference': complaint['paystack_reference']}]
        else:
            if ref.get('_id'):
                oid = ref['_id']
                identifiers.append({'_id': ObjectId(str(oid))})
            for key in ('order_id', 'order_no'):
                if ref.get(key):
                    identifiers.append({key: ref[key]})
            if not identifiers and complaint.get('order_number_provided'):
                identifiers = [{key: complaint['order_number_provided']} for key in ('order_id', 'order_no')]
        matches = list(orders.find({**scope, '$or': identifiers}, session=session)) if identifiers else []
        if len(matches) != 1:
            raise ValueError('A unique original order could not be found. Refund was not credited.')
        order = matches[0]
        indices = _selected_lines(complaint, order)
        owner = order.get('user_id')
        store_order = bool(order.get('store_slug'))
        if store_order and order.get('paid_from') in ('paystack_inline', 'admin_complaint'):
            store = db['stores'].find_one({'slug': order['store_slug']}, session=session) or {}
            owner = order.get('store_owner_id') or store.get('owner_id')
        if not owner:
            raise ValueError('The original wallet owner could not be identified.')
        if ObjectId.is_valid(str(owner)):
            owner = ObjectId(str(owner))
        now, total, credited = datetime.utcnow(), 0.0, 0.0
        fields = {'updated_at': now}
        for index in indices:
            item = order['items'][index]
            reference = f"{order.get('order_id') or order['_id']}:LINE:{index}:REFUND"
            existing = ledger.find_one({'type': 'refund', 'reference': reference}, session=session)
            if existing or item.get('refunded_at') or item.get('line_status') == 'refunded' or order.get('refunded_at') or order.get('status') == 'refunded':
                total += (existing or {}).get('amount') or item.get('refund_amount') or 0
                continue
            if str(item.get('line_status') or '').startswith('skipped'):
                raise ValueError('Skipped items were not charged and cannot be refunded.')
            amount = _money(item.get('amount'))
            basis = 'original_charged_amount'
            if store_order:
                amount = _money(item.get('base_amount'))
                basis = 'original_store_base_amount'
                if amount is None and item.get('store_profit_amount') is not None:
                    amount = _money(Decimal(str(item.get('amount') or 0)) - Decimal(str(item['store_profit_amount'])))
            if amount is None:
                raise ValueError('Original purchase price is unavailable. Refund was not credited.')
            ledger.insert_one({'_id': reference, 'reference': reference, 'type': 'refund',
                'user_id': owner, 'amount': amount, 'order_id': order.get('order_id'),
                'status': 'success', 'gateway': 'Wallet', 'currency': 'GHS',
                'created_at': now, 'verified_at': now,
                'meta': {'note': 'Complaint refund', 'complaint_id': complaint_id,
                         'order_db_id': order['_id'], 'line_index': index,
                         'refund_basis': basis, 'actor_admin_id': actor.get('user_id'),
                         'store_slug': order.get('store_slug')}}, session=session)
            credited += amount
            total += amount
            fields.update({f'items.{index}.line_status': 'refunded',
                           f'items.{index}.refunded_at': now,
                           f'items.{index}.refund_amount': amount,
                           f'items.{index}.refunded_by': actor.get('user_id')})
        if credited:
            db['balances'].update_one({'user_id': owner}, {'$inc': {'amount': round(credited, 2)},
                '$set': {'updated_at': now}}, upsert=True, session=session)
        if all(i in indices or item.get('refunded_at') or item.get('line_status') == 'refunded'
               or str(item.get('line_status') or '').startswith('skipped')
               for i, item in enumerate(order['items'])):
            fields.update(status='refunded', refunded_at=now)
        orders.update_one({'_id': order['_id']}, {'$set': fields}, session=session)
        complaints.update_one({'_id': complaint_id}, {'$set': {
            'status': 'refund', 'updated_at': now, 'updated_by': actor,
            'refund_completed_at': now, 'refund_amount': round(total, 2),
            'refund_wallet_user_id': owner, 'refund_order_id': order['_id'],
            'refund_line_indices': indices}}, session=session)
        return round(credited, 2), True

    with db.client.start_session() as session:
        return session.with_transaction(commit)
