# Nagonu dashboard bulk orders

New multi-item dashboard wallet purchases save one `orders` document per item.
Each child has a unique ID (`<batch>-1`, `<batch>-2`, ...), one item, its own
status, charge and profit snapshots, and the unchanged provider request reference.
`batch_order_id`, `batch_position` and `batch_size` preserve the shared purchase.
Both Admin Orders and Customer Orders therefore read the same independent orders.
Skipped duplicate items are preserved with zero wallet charge and zero profit.

A MongoDB transaction saves all children, one wallet debit and one purchase
transaction together. MongoDB must support transactions (the configured Atlas
cluster does). Insufficient balance or any write failure aborts the whole batch.
Provider dispatch runs after commit, with each job targeting its child order.
The purchase transaction's metadata lists all child IDs. The checkout response
retains its batch ID and shared invoice URL and adds `order_ids`.

The batch invoice aggregates the saved children. Repeated requests carrying the
same `client_request_id` return the saved batch and do not debit or submit again.
This preserves existing sequential retry handling; requests without that key
continue to use the existing duplicate-line checks.

Existing purchases are not migrated: their customer/admin line views continue
showing individual items. Campus Data and the separate public-store checkout are
unchanged. Run `python create_order_indexes.py` when deploying to add the batch
lookup index alongside the existing order indexes.

Validation: `python -m unittest discover -s tests -q` (in Nagonu).
