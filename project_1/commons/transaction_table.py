class TransactionTable:
    def __init__(self, transaction_id, timestamp, state):
        self.id = transaction_id
        self.timestamp = timestamp
        self.state = state
        self.locked_items = list()
        self.blocked_tasks = list()
        self.blocked_by_transactions = list()

    def add_blocked_tasks(self, transaction):
        self.blocked_tasks.append(transaction)

    def get_blocked_tasks(self):
        return self.blocked_tasks

    def get_transaction_state(self):
        return self.state

    def set_transaction_state(self, state):
        self.state = state

    def add_locked_items(self, item):
        if item not in self.locked_items:
            self.locked_items.append(item)

    def remove_locked_items(self, item):
        self.locked_items.remove(item)

    def get_locked_items(self):
        return self.locked_items

    def get_timestamp(self):
        return self.timestamp

    def add_blocked_by_transaction(self, transaction_id):
        self.blocked_by_transactions.append(transaction_id)

    def get_blocked_by_transaction(self):
        return self.blocked_by_transactions

    def remove_blocked_by_transaction(self, transaction_id):
        self.blocked_by_transactions.remove(transaction_id)
