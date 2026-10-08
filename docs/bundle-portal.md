# Bundle Portal service integration

Both **MTN NORMAL** and **MTN EXPRESS** have these independent provider choices in Admin Services:

| Label | Stored provider | Bundle Portal network |
| --- | --- | --- |
| Bundle Portal MTN | `bundleportal_mtn` | `mtn` |
| Bundle Portal MTN2 | `bundleportal_mtn2` | `mtn_2` |
| Bundle Portal MTN3 | `bundleportal_mtn3` | `mtn_3` |
| Bundle Portal AT iShare | `bundleportal_ishare` | `airteltigo` (alias `ishare`) |
| Bundle Portal Telecel | `bundleportal_telecel` | `telecel` |

Telecel (including legacy Vodafone service names) supports **Bundle Portal Telecel** in each Admin Services provider selector. Turn the service ON/API before switching. Customer and store checkout use the wallet-funded `get_bundles` / `place_order` catalogue with `network: telecel`, and the existing key, webhook secret, retry, and refund workflows. No additional environment variables are needed. The separate preloaded `share_telecel` API is not used.

AT iShare uses the wallet-funded `get_bundles` / `place_order` catalogue, not the separate preloaded share-balance API. It is selectable only on the AT iShare service, uses the existing credentials and webhook, and works for customer and store orders. The AT Bigtime selector shows a disabled Bundle Portal option: the supplied documentation does not specify a Bigtime network or catalogue. Confirm its route with Bundle Portal before enabling it; do not substitute the iShare catalogue for Bigtime. CodeCraft remains selectable for existing AT services.

Switching a service affects new customer-dashboard and store orders. Existing orders retain their original route and reference. The service must be ON/API. Default and Store selling prices remain the prices configured in this app; the integration does not replace them with supplier prices. Bundle sizes must exist in the selected route's catalogue.

## Configuration before enabling

1. Set `BUNDLE_PORTAL_KEY=bp_live_...` in the server environment (or local `.env`).
2. Deploy the callback endpoint at `https://YOUR_APP_HOST/webhooks/bundleportal`.
3. Register that exact URL with Bundle Portal using a server-side POST to `https://api.bundleportal.com/v2`, header `x-api-key: YOUR_KEY`, and JSON:

   ```json
   {"action":"set_webhook","webhook_url":"https://YOUR_APP_HOST/webhooks/bundleportal"}
   ```

4. Save the returned, one-time `data.webhook_secret` as `BUNDLE_PORTAL_WEBHOOK_SECRET` in the server environment and restart the app. Do not register again unnecessarily: registration rotates the secret. Use `get_webhook` to inspect the existing registration.
5. Run `python api_test/bundleportal_runtime.py` to inspect the supported live catalogues without purchasing anything. Admins can also GET `/admin/services/bundleportal/catalog?network=mtn_2` while logged in.
6. In Admin Services, choose a Bundle Portal provider for each MTN service and enable API mode. Selecting a Bundle Portal route is blocked until both environment secrets exist. That check cannot verify that the remote webhook URL was registered correctly.

Never expose either secret in frontend code. Fund the provider wallet before accepting live orders. No live purchase is part of the automated tests.

## Delivery and error handling

- Each line gets a unique `BP_...` reference. The shared background worker checks the route's catalogue and recipient eligibility, then submits using that reference.
- `processing` and `cached` remain processing. A signed `order.completed` callback marks the line delivered and recomputes the parent order status. These lines are excluded from timed auto-delivery and are never polled.
- Callback signatures use HMAC-SHA256 over the raw body. Route and recipient must match the stored line. Duplicate callbacks do not reapply terminal changes; authenticated events are retained in `bundleportal_events`.
- A rejection or confirmed failed/cancelled/refunded provider event marks the line failed with `refund_required`. Use the existing **Refunded** action in Admin Orders to credit the local wallet. A provider-wallet refund is not a customer-wallet refund. Store refunds continue to exclude customer markup under the existing refund workflow.
- Timeouts, 5xx, 409, 429 and pending recipient approval stay processing with `review_required`. The **Retry Bundle Portal** action reuses the original reference and original route; uncertain purchase retries bypass preflight so an existing in-flight order can be returned idempotently. It never switches providers or generates a second reference.
- The app uses its existing background-thread checkout dispatcher. A server restart can interrupt queued work; admins can retry queued lines. A line stranded in `submitting` must be reconciled against the Bundle Portal dashboard before further action.
- Bundle Portal v2 rejects `check_status` with HTTP 410 (`polling_disabled`). Failed webhook deliveries are retried after 30 seconds, 2 minutes, 10 minutes, 30 minutes, and 2 hours. Monitor the public endpoint and its secret configuration. If all delivery attempts were missed, obtain the result from the provider dashboard and use the existing admin status controls; no polling endpoint can recover it.
- Authenticated callbacks arriving before a local order exists are saved and acknowledged with HTTP 202. The three-minute reconciler searches both app databases, without relying on a Campus reference prefix. It checks all five routes, including delivered/failed lines so later provider refunds are not missed, and repairs stale parent statuses. Local completed/refunded lines remain protected. Provider settlement times prevent older receipts replacing newer settlements. Legacy merchant references in `provider_order_id` are supported.
- The inbox is durable before order updates. Order updates and cache invalidation run in a background worker so the endpoint can acknowledge promptly. Persisted events are replayed after interrupted processing. The initial database lookup and inbox write still require a responsive database.

## Verification

Install `requirements-test.txt` in addition to the application's dependencies, then run:

```text
python -m unittest discover -s tests -p test_bundle_portal.py -v
```

Tests use an in-memory database and mocked provider/payment calls. They exercise all 12 combinations of two MTN services, three routes, and customer/store checkout, plus AT iShare on both checkout paths, admin selection, wrong-product rejection, signed callbacks, failure handling, and idempotency. Live credentials, public webhook registration, and a separately authorized live order are still required for end-to-end provider validation.

Contract reference: supplied Bundle Portal v2 documentation, also published at https://bundleportal.com/api-docs.
