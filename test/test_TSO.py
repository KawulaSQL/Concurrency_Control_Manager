import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
from models.CCManagerEnums import OperationType
from ConcurrencyControlManager import ConcurrencyControlManager,Operation

# TSO normal condition
def main():
    ccm = ConcurrencyControlManager("TSO")

    # Start Transaction 1 and Transaction 2
    transaction_id_1 = ccm.begin_transaction()
    time.sleep(1)
    transaction_id_2 = ccm.begin_transaction()
    
    time.sleep(1)

    resource_a = "users_1"
    resource_b = "users_2"
    resource_c = "users_3"

    def normal():
        opList = [Operation(transaction_id_1, OperationType.W, resource_a), 
                  Operation(transaction_id_2, OperationType.R, resource_a), 
                  Operation(transaction_id_1, OperationType.W, resource_b),
                  Operation(transaction_id_2, OperationType.W, resource_c)]
        for i in range(len(opList)):
            ccm.validate_object(opList[i])
            print()

    def aborted():
        opList = [Operation(transaction_id_1, OperationType.W, resource_a), 
                  Operation(transaction_id_1, OperationType.R, resource_c),
                  Operation(transaction_id_2, OperationType.W, resource_b),
                  Operation(transaction_id_2, OperationType.W, resource_a)]
        for i in range(len(opList)):
            ccm.validate_object(opList[i])
            print()

    print("Normal Condition")
    normal()
    time.sleep(1)
    print("Aborted Condition")
    aborted()
    ccm.end_transaction(transaction_id_1)
    ccm.end_transaction(transaction_id_2)

if __name__ == "__main__":
    main()
