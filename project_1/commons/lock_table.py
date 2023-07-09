class LockTable:
    def __init__(self, item_value, lock_state):
        self.item_value = item_value
        self.lock_state = lock_state
        self.lock_holders = list()
        self.blocked_transactions = list()

    def add_lock_holders(self, transaction_id):
        self.lock_holders.append(transaction_id)
        self.lock_holders.sort()

    def remove_lock_holders(self, transaction_id):
        self.lock_holders.remove(transaction_id)

    def get_lock_holders(self):
        return self.lock_holders

    def get_lock_state(self):
        return self.lock_state

    def set_lock_state(self, state):
        self.lock_state = state
