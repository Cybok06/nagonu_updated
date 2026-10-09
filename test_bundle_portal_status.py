"""Inspect Bundle Portal status for a recent DB order.

python test_bundle_portal_status.py
python test_bundle_portal_status.py --order-id NAN123 --source main
python test_bundle_portal_status.py --source all --limit 5
python test_bundle_portal_status.py --order-id NAN123 --apply

Default is read-only. --apply replays saved authenticated webhook settlements;
this never submits purchases, charges wallets, or fabricates a delivered status.
"""
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path


def run_diagnostics(sources, events, inspect, order_id=None, limit=1, apply=False):
    from bson import ObjectId
    from bundle_portal import PROVIDERS
    query = {"items": {"$elemMatch": {"provider": {"$in": list(PROVIDERS)}}}}
    if order_id:
        choices = [{"order_id": order_id}, {"batch_order_id": order_id}]
        if ObjectId.is_valid(order_id):
            choices.append({"_id": ObjectId(order_id)})
        query["$or"] = choices
    recent = []
    for source, collection in sources:
        for order in collection.find(query).sort([("created_at", -1), ("_id", -1)]).limit(limit):
            recent.append((source, collection, order))
    recent.sort(key=lambda row: (row[2].get("created_at") or datetime.min).replace(tzinfo=None), reverse=True)
    results = []
    for source, collection, order in recent[:limit]:
        result = inspect(collection, order, events_col=events, apply=apply)
        result["source"] = source
        results.append(result)
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--order-id', help='Order ID, batch checkout ID, or Mongo ObjectId')
    parser.add_argument('--source', choices=['main', 'campus', 'all'], default='all')
    parser.add_argument('--limit', type=int, default=1, help='Number of recent orders (1–100)')
    parser.add_argument('--apply', action='store_true', help='Apply matching saved webhook settlements to selected orders')
    parser.add_argument('--output', type=Path, help='Also save the JSON report to this file')
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= 100:
        parser.error('--limit must be between 1 and 100')
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).with_name('.env'))
    # Import production logic without starting application background jobs.
    os.environ['ORDER_STATUS_START_SCHEDULERS'] = '0'
    try:
        from pymongo import timeout
        from db import db, campus_db
        from order_status import inspect_bundle_portal_order
        sources = [('main', db['orders']), ('campus', campus_db['orders'])]
        if args.source != 'all':
            sources = [source for source in sources if source[0] == args.source]
        with timeout(20):
            results = run_diagnostics(sources, db['bundleportal_events'], inspect_bundle_portal_order,
                                      args.order_id, args.limit, args.apply)
        report = {'ok': bool(results), 'mode': 'apply' if args.apply else 'read_only',
                  'status_source': 'saved_authenticated_webhook',
                  'webhook_secret_configured': bool(os.getenv('BUNDLE_PORTAL_WEBHOOK_SECRET', '').strip()),
                  'orders': results}
        if not results:
            report['message'] = 'No matching Bundle Portal orders found.'
        if args.apply and any(result['summary']['updated_lines'] or result['summary'].get('updated_orders') for result in results):
            from bundle_portal_orders import invalidate_orders
            invalidate_orders()
    except Exception as exc:
        # Avoid exposing connection strings or credentials in exception messages.
        report = {'ok': False, 'error_type': type(exc).__name__,
                  'message': 'Diagnostic failed. Check database connectivity and environment configuration.'}
        if type(exc).__name__ == 'ConfigurationError' and 'resolution lifetime expired' in str(exc).lower():
            report['reason'] = 'database_dns_timeout'
            report['message'] = 'MongoDB Atlas DNS lookup timed out before an order could be read.'
    output = json.dumps(report, indent=2, default=str, ensure_ascii=False)
    print(output)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + '\n', encoding='utf-8')
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    sys.exit(main())
