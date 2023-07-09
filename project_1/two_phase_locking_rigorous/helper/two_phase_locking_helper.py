import re

from commons.generic_constants import GenericConstants
from commons.lock_table import LockTable
from commons.transaction_table import TransactionTable


class TwoPhaseLockingHelper:
    def __init__(self):
        self.lock_table_objs = None
        self.timestamp = None
        self.transactions_map = None

    def execute_transaction(self, transaction):
        transaction_number = self.get_transaction_number(transaction)
        transaction_type = self.check_transaction_type(transaction)
        if transaction_type != GenericConstants.BEGIN:
            if self.check_transaction_state(transaction, transaction_number):
                if self.transactions_map[transaction_number].get_transaction_state() == GenericConstants.ABORT:
                    return f'T{transaction_number} is already aborted.'
                return self.abort_transaction(transaction_number)
        return self.execute_action(transaction, transaction_number, transaction_type)

    @staticmethod
    def get_transaction_number(transaction):
        for ch in transaction:
            if ch.isdigit():
                return int(ch)

    @staticmethod
    def check_transaction_type(transaction):
        if transaction[0] == GenericConstants.BEGIN_TRANSACTION_CH:
            return GenericConstants.BEGIN

        elif transaction[0] == GenericConstants.READ_TRANSACTION_CH:
            return GenericConstants.READ

        elif transaction[0] == GenericConstants.WRITE_TRANSACTION_CH:
            return GenericConstants.WRITE

        elif transaction[0] == GenericConstants.END_TRANSACTION_CH:
            return GenericConstants.END

    def check_transaction_state(self, transaction, transaction_id):
        if self.transactions_map:
            if self.transactions_map[transaction_id].get_transaction_state() == GenericConstants.BLOCKED:
                self.transactions_map[transaction_id].add_blocked_tasks(transaction)

            elif self.transactions_map[transaction_id].get_transaction_state() == GenericConstants.ABORT:
                return True
        return False

    def execute_action(self, transaction, transaction_id, transaction_type):
        if transaction_type == GenericConstants.BEGIN:
            return self.begin_transaction(transaction_id)

        elif transaction_type == GenericConstants.READ:
            return self.read_transaction_task(transaction, transaction_id, transaction_type)

        elif transaction_type == GenericConstants.WRITE:
            return self.write_transaction_task(transaction, transaction_id, transaction_type)

        elif transaction_type == GenericConstants.END:
            return self.end_transaction(transaction, transaction_id)

    def begin_transaction(self, transaction_id):
        t = TransactionTable(transaction_id=transaction_id, timestamp=self.timestamp, state=GenericConstants.ACTIVE)
        self.transactions_map[transaction_id] = t
        self.timestamp += 1
        return f'T{transaction_id} begins. Id={transaction_id}. TS={self.timestamp - 1}. state=active'

    def read_transaction_task(self, transaction, transaction_id, transaction_type):
        item = self.get_transaction_task_item(transaction)
        if self.transactions_map[transaction_id].get_transaction_state() == GenericConstants.ACTIVE:
            return self.read_lock(item, transaction_id, transaction_type, transaction)

    def write_transaction_task(self, transaction, transaction_id, transaction_type):
        item = self.get_transaction_task_item(transaction)
        if self.transactions_map[transaction_id].get_transaction_state() == GenericConstants.ACTIVE:
            return self.write_lock(item, transaction_id, transaction_type, transaction)

    def end_transaction(self, transaction, transaction_id):
        transaction_state = self.transactions_map[transaction_id].get_transaction_state()
        if transaction_state == GenericConstants.ABORT:
            return f'T{transaction_id} is already aborted.'
        elif transaction_state == GenericConstants.BLOCKED:
            return f'Committing T{transaction_id} is added to operation list.'

        locked_items = self.transactions_map[transaction_id].get_locked_items()
        for item in locked_items:
            self.unlock_item(item, transaction_id)
        all_blocked_operations = []
        for t_id, t_obj in self.transactions_map.items():
            if transaction_id in t_obj.get_blocked_by_transaction():
                t_obj.set_transaction_state(GenericConstants.ACTIVE)
                t_obj.remove_blocked_by_transaction(transaction_id)
                all_blocked_operations.extend(t_obj.get_blocked_tasks())

        for t in all_blocked_operations:
            self.execute_transaction(t)

        return f'T{transaction_id} is committed.'

    @staticmethod
    def get_transaction_task_item(transaction):
        t = re.split(r"[^A-Za-z0-9]", transaction)
        t = [i for i in t if i]
        return t[1]

    def read_lock(self, item, transaction_id, transaction_type, transaction):
        if not self.check_transaction_item_lock_table(item):
            _l = LockTable(item_value=item, lock_state=transaction_type)
            _l.add_lock_holders(transaction_id)
            self.transactions_map[transaction_id].add_locked_items(item)
            self.lock_table_objs.append(_l)
            return f'{item} is read locked by T{transaction_id}'
        else:
            lock_obj, lock_status = self.get_item_lock_status(item)
            if lock_status == GenericConstants.READ and transaction_id not in lock_obj.get_lock_holders():
                lock_obj.add_lock_holders(transaction_id)
                self.transactions_map[transaction_id].add_locked_items(item)
                return f'{item} is read locked by T{transaction_id}'

            elif lock_status == GenericConstants.WRITE and transaction_id not in lock_obj.get_lock_holders():
                return self.wait_die(item, transaction_id, transaction)

    def write_lock(self, item, transaction_id, transaction_type, transaction):
        _l, lock_status = (None, None)
        if self.check_transaction_item_lock_table(item):
            _l, lock_status = self.get_item_lock_status(item)

        obj_item_value = _l.item_value if _l else None
        if obj_item_value == item and lock_status == GenericConstants.WRITE and transaction_id not in _l.get_lock_holders():
            return self.wait_die(item, transaction_id, transaction)

        elif lock_status == GenericConstants.READ and transaction_id in _l.get_lock_holders() and len(
                _l.get_lock_holders()) == 1:
            _l.set_lock_state(transaction_type)
            return f'read lock on {item} by T{transaction_id} is upgraded to {transaction_type} lock'

        elif lock_status == GenericConstants.READ and transaction_id in _l.get_lock_holders() and len(
                _l.get_lock_holders()) > 1:
            return self.wait_die(item, transaction_id, transaction, flag=True)

    def check_transaction_item_lock_table(self, item):
        for _l in self.lock_table_objs:
            if _l.item_value == item:
                return True
        return False

    def get_item_lock_status(self, item):
        for _l in self.lock_table_objs:
            if _l.item_value == item:
                return _l, _l.get_lock_state()
        return

    def wait_die(self, item, transaction_id, transaction, flag=False):
        curr_transaction_timestamp = self.transactions_map[transaction_id].get_timestamp()
        lock_obj, lock_state = self.get_item_lock_status(item)
        lock_holder_timestamp = self.transactions_map[lock_obj.get_lock_holders()[-1]].get_timestamp()
        if curr_transaction_timestamp < lock_holder_timestamp:
            # lock_obj.add_blocked_transaction(transaction_id)
            self.transactions_map[transaction_id].set_transaction_state(GenericConstants.BLOCKED)
            self.transactions_map[transaction_id]. \
                add_blocked_by_transaction(self.transactions_map[lock_obj.get_lock_holders()[0]].id)
            self.transactions_map[transaction_id].add_blocked_tasks(transaction)
            if flag:
                self.execute_transaction(transaction)
            return f'T{transaction_id} is blocked/waiting due to wait die.'
        else:
            self.transactions_map[transaction_id].set_transaction_state(GenericConstants.ABORT)
            self.abort_transaction(transaction_id)
            return f'T{transaction_id} is aborted due to wait-die.'

    def abort_transaction(self, transaction_id):
        locked_items = self.transactions_map[transaction_id].get_locked_items()
        for item in locked_items:
            self.transactions_map[transaction_id].remove_locked_items(item)
            self.unlock_item(item, transaction_id)
        all_blocked_operations = []

        for t_id, t_obj in self.transactions_map.items():
            if transaction_id in t_obj.get_blocked_by_transaction():
                t_obj.set_transaction_state(GenericConstants.ACTIVE)
                t_obj.remove_blocked_by_transaction(transaction_id)
                all_blocked_operations.extend(t_obj.get_blocked_tasks())
        for t in all_blocked_operations:
            self.execute_transaction(t)

    def unlock_item(self, item, transaction_id):
        lock_obj, lock_state = self.get_item_lock_status(item)
        lock_obj.remove_lock_holders(transaction_id)
        if len(lock_obj.get_lock_holders()) > 0:
            lock_obj.set_lock_state(GenericConstants.READ)
        else:
            self.lock_table_objs.remove(lock_obj)
            del lock_obj
