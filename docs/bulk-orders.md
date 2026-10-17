# Dashboard and store multi-item checkout

Every new multi-item checkout saves one independent `orders` document per item.
Each order has its own NAN ID, one item, status, charge, base price and profit
snapshots, and unchanged provider references. IDs do not use numbered suffixes.
No parent order containing the cart is saved. Customer Orders displays the actual
order IDs without item-number labels.

`checkout_id` and `receipt_position` link independent orders to the shared payment
receipt. They are receipt metadata, not an additional saved order. The receipt
loads the individual records and shows every item with its own order ID and the
checkout total. Store checkout opens this receipt; checker result links remain
available on the corresponding receipt items. Existing legacy receipts remain
readable, and existing purchase records are not migrated.

Dashboard checkout commits the independent orders, one wallet debit and one
purchase transaction atomically. Store checkout commits the independent orders
without debiting the store owner's wallet. Skipped duplicates retain zero charge
and profit. Any transaction failure rolls back the orders and wallet debit.
Provider jobs target the individual order ID with item index zero.

Checkout responses include `order_ids` and each item's `order_id`. The top-level
`order_id` identifies the shared receipt. Dashboard request-key retries and store
payment-reference retries return the same saved order IDs without submitting or
charging them again. Individual order IDs remain independently trackable.

`create_order_indexes.py` includes the checkout receipt lookup index alongside
the legacy lookup index; run it during deployment if indexes need updating.

Validation: `python -m unittest discover -s tests -p "test_bulk_orders.py"`
plus store tracking, customer order display and admin refund regression tests.
