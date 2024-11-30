import threading
import time
from models.Resource import Resource
from models.CCManagerEnums import OperationType, OperationStatus
from ConcurrencyControlManager import ConcurrencyControlManager

def main():
    # Initialize the ConcurrencyControlManager
    ccm = ConcurrencyControlManager()

    # Create Resources (assuming Resource class is defined somewhere)
    resource_A = Resource("A")
    resource_B = Resource("B")
    resource_C = Resource("C")
    resource_D = Resource("D")

    # Transaction 1 - Operation list for transaction 1
    operation_1_tx1 = Operation(OperationType.READ, "tx1", resource_A)
    operation_2_tx1 = Operation(OperationType.WRITE, "tx1", resource_B)
    operation_3_tx1 = Operation(OperationType.READ, "tx1", resource_C)

    # Transaction 2 - Operation list for transaction 2
    operation_1_tx2 = Operation(OperationType.WRITE, "tx2", resource_C)
    operation_2_tx2 = Operation(OperationType.READ, "tx2", resource_D)
    operation_3_tx2 = Operation(OperationType.READ, "tx2", resource_A)

    # List of operations for Transaction 1
    operations_tx1 = [operation_1_tx1, operation_2_tx1, operation_3_tx1]
    # List of operations for Transaction 2
    operations_tx2 = [operation_1_tx2, operation_2_tx2, operation_3_tx2]

    # Start Transaction 1 and Transaction 2
    transaction_id_1 = ccm.begin_transaction(operations_tx1)
    transaction_id_2 = ccm.begin_transaction(operations_tx2)

    # Start the concurrency control manager in a separate thread
    thread = threading.Thread(target=ccm.run)
    thread.start()

    # Simulate the process for a while
    time.sleep(5)  # Let it process the transactions

    # Stop the concurrency control manager gracefully
    ccm.stop()

    # Wait for the thread to finish
    thread.join()

if __name__ == "__main__":
    main()
