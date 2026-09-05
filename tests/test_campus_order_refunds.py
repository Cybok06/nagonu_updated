import admin_orders


def test_campus_line_refund_uses_base_amount():
    amount, basis = admin_orders._line_refundable_amount(
        {"order_id": "CAMP123"},
        {"amount": 12.0, "base_amount": 9.5},
        "campus",
    )
    assert amount == 9.5
    assert basis == "campus_line_base_amount"


def test_campus_refunds_use_campus_wallet_collections():
    balances, transactions = admin_orders._refund_collections("campus")
    assert balances is admin_orders.campus_balances_col
    assert transactions is admin_orders.campus_transactions_col


def test_main_refunds_keep_using_main_wallet_collections():
    balances, transactions = admin_orders._refund_collections("main")
    assert balances is admin_orders.balances_col
    assert transactions is admin_orders.transactions_col


class _InsertResult:
    inserted_id = "credit-id"


class _ProviderTransactions:
    def __init__(self):
        self.docs = [{
            "provider": "provider_wallet", "direction": "DEBIT",
            "reason": "ORDER_RESERVE", "order_id": "CAMP100001",
            "line_index": 1, "amount": 7.25, "dedupe_key": "original-debit",
        }]

    @staticmethod
    def _matches(doc, query):
        return all(doc.get(key) == value for key, value in query.items())

    def find(self, query, projection=None):
        return [doc for doc in self.docs if self._matches(doc, query)]

    def find_one(self, query, projection=None):
        return next((doc for doc in self.docs if self._matches(doc, query)), None)

    def insert_one(self, doc):
        self.docs.append(dict(doc, _id="credit-id"))
        return _InsertResult()

    def delete_one(self, query):
        self.docs = [doc for doc in self.docs if not self._matches(doc, query)]


class _ProviderAccounts:
    def __init__(self):
        self.balance = 10.0
        self.calls = 0

    def update_one(self, query, update, upsert=False):
        self.calls += 1
        self.balance += update["$inc"]["balance"]


def test_campus_provider_refund_restores_exact_recorded_debit_once(monkeypatch):
    txns = _ProviderTransactions()
    account = _ProviderAccounts()
    monkeypatch.setattr(admin_orders, "campus_provider_transactions_col", txns)
    monkeypatch.setattr(admin_orders, "campus_provider_accounts_col", account)
    order = {"_id": "order-db-id", "order_id": "CAMP100001", "items": [{"amount": 99.0}]}

    amount, error = admin_orders._credit_campus_provider_refund(order, item_index=0)
    second_amount, second_error = admin_orders._credit_campus_provider_refund(order, item_index=0)

    assert error is None and second_error is None
    assert amount == second_amount == 7.25
    assert account.balance == 17.25
    assert account.calls == 1
    credit = next(doc for doc in txns.docs if doc.get("reason") == "ORDER_REFUND")
    assert credit["amount"] == 7.25
    assert credit["direction"] == "CREDIT"


def test_mtn_normal_provider_refund_uses_saved_base_when_legacy_debit_is_missing(monkeypatch):
    txns = _ProviderTransactions()
    txns.docs = []
    account = _ProviderAccounts()
    monkeypatch.setattr(admin_orders, "campus_provider_transactions_col", txns)
    monkeypatch.setattr(admin_orders, "campus_provider_accounts_col", account)
    order = {
        "_id": "legacy-order-db-id",
        "order_id": "CAMP-LEGACY-MTN-1",
        "items": [{"serviceName": "MTN Normal", "amount": 8.0, "base_amount": 6.5}],
    }

    amount, error = admin_orders._credit_campus_provider_refund(order, item_index=0)

    assert error is None
    assert amount == 6.5
    assert account.balance == 16.5
    credit = next(doc for doc in txns.docs if doc.get("reason") == "ORDER_REFUND")
    assert credit["meta"]["refund_basis"] == "mtn_normal_base_amount_fallback"


def test_delivered_campus_order_can_transition_to_refunded():
    assert admin_orders._can_transition("delivered", "refunded") is True


class _OrderCollection:
    def __init__(self, order):
        self.order = order
        self.status_updates = []

    def find_one(self, query):
        return self.order

    def update_one(self, query, update, **kwargs):
        self.status_updates.append(update)
        return type("Result", (), {"modified_count": 1})()


class _TrackingBalance:
    def __init__(self):
        self.amount = 0.0

    def update_one(self, query, update, **kwargs):
        self.amount += update["$inc"]["amount"]


class _FailingRefundTransactions:
    def insert_one(self, doc):
        raise RuntimeError("ledger unavailable")


def test_mtn_normal_campus_order_is_not_marked_refunded_when_wallet_credit_fails(monkeypatch):
    order = {
        "_id": "campus-order-db-id",
        "order_id": "CAMP-MTN-NORMAL-1",
        "user_id": "campus-user-id",
        "status": "delivered",
        "items": [{
            "serviceName": "MTN Normal",
            "amount": 8.0,
            "base_amount": 6.5,
            "line_status": "delivered",
        }],
    }
    orders = _OrderCollection(order)
    balance = _TrackingBalance()
    monkeypatch.setattr(admin_orders, "campus_balances_col", balance)
    monkeypatch.setattr(admin_orders, "campus_transactions_col", _FailingRefundTransactions())
    monkeypatch.setattr(admin_orders, "_credit_campus_provider_refund", lambda *args, **kwargs: (6.0, None))

    updated, errors = admin_orders._apply_status_change(
        [order["_id"]],
        "refunded",
        orders_collection=orders,
        source="campus",
    )

    assert updated == 0
    assert errors and "refund ledger err" in errors[0]
    assert balance.amount == 0.0
    assert orders.status_updates == []
