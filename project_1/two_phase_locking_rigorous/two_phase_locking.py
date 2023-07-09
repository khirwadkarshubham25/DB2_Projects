import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from commons.generic_constants import GenericConstants
from two_phase_locking_rigorous.helper.two_phase_locking_helper import TwoPhaseLockingHelper


class TwoPhaseLocking(TwoPhaseLockingHelper):
    def __init__(self, *input_list):
        super().__init__()
        self.input_file_name = f'{GenericConstants.INPUT_FILE_DIR}{input_list[0]}'
        self.out_file_name = f'{GenericConstants.OUTPUT_FILE_DIR}{input_list[1]}'
        self.transactions_map = {}
        self.lock_table_objs = []
        self.timestamp = 1

    def two_phase_locking_rigorous(self):
        self.clean_output_file(self.out_file_name)
        input_data = self.read_input_file(self.input_file_name)
        for transaction in input_data:
            o = self.execute_transaction(transaction)
            self.write_output_file(transaction, o, self.out_file_name)

    @staticmethod
    def clean_output_file(output_file_name):
        open(output_file_name, 'w').close()

    @staticmethod
    def read_input_file(input_file_name):
        with open(input_file_name, 'r') as f:
            data = f.read().splitlines()

        return data

    @staticmethod
    def write_output_file(transaction, o, output_file_name):
        with open(output_file_name, 'a') as f:
            f.write(f'{transaction} {o}\n')


if __name__ == '__main__':
    cmd_args = sys.argv
    obj = TwoPhaseLocking(*cmd_args[1:])
    obj.two_phase_locking_rigorous()
