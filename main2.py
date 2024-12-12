from models.Operation import Operation
from models.CCManagerEnums import OperationType, ResponseType
from ConcurrencyControlManager import ConcurrencyControlManager


class QueryProcessor:
    def __init__(self, cc_manager):
        """
        Initialize the QueryProcessor with a reference to the CCManager.
        """
        self.cc_manager = ConcurrencyControlManager()

    def process_transaction(self, transaction_id, operations):
        """
        Process a transaction and its operations.
        
        Parameters:
        - transaction_id: ID of the transaction being processed.
        - operations: List of Operation objects for this transaction.
        """
        for operation in operations:
            print(f"\nProcessing operation on resource {operation.getOperationResource()}...")

            response = self.cc_manager.validate_object(operation)

            if response == ResponseType.ALLOWED:
                # Execute the operation
                print(f"Operation on {operation.getOperationResource()} allowed. Executing...")
                self.log_object(operation)
            elif response == ResponseType.WAITING:
                # Wait for resource to be available
                print(f"Operation on {operation.getOperationResource()} is waiting for resource availability.")
                return False  # Wait and retry later
            elif response == ResponseType.ABORT:
                # Abort and rollback the transaction
                print(f"Operation on {operation.getOperationResource()} aborted. Rolling back transaction.")
                self.cc_manager.rollback(transaction_id)
                self.end_transaction(transaction_id)
                return True  # Transaction has been aborted and removed

        # If all operations have been processed, commit the transaction
        self.end_transaction(transaction_id)
        return True

    def log_object(self, operation):
        """
        Call CCManager to log the operation execution.
        """
        self.cc_manager.log_object(operation)
        print(f"Logged operation on resource {operation.getOperationResource()} through CCManager.")

    def end_transaction(self, transaction_id):
        """
        End the transaction using CCManager.
        """
        self.cc_manager.end_transaction(transaction_id)
        print(f"Transaction {transaction_id} ended.")


def transaction_simulation(cc_manager):
    """
    Simulate the query processor workflow for processing transactions.
    """
    query_processor = QueryProcessor(cc_manager)

    transactions = [
        {
            "transaction_id": cc_manager.begin_transaction(),
            "operations": [
                Operation(transactionID=1, typeOp=OperationType.R, res="Resource1"),
                Operation(transactionID=1, typeOp=OperationType.W, res="Resource2"),
            ],
        },
        {
            "transaction_id": cc_manager.begin_transaction(),
            "operations": [
                Operation(transactionID=2, typeOp=OperationType.W, res="Resource3"),
                Operation(transactionID=2, typeOp=OperationType.R, res="Resource4"),
            ],
        },
    ]

    for transaction in transactions:
        transaction_id = transaction["transaction_id"]
        operations = transaction["operations"]

        print(f"\n--- Processing Transaction {transaction_id} ---")
        is_finished = query_processor.process_transaction(transaction_id, operations)

        if not is_finished:
            # Retry waiting transactions
            waiting_transactions = cc_manager.get_waiting_transactions()
            print(f"Waiting transactions: {waiting_transactions}")
            for waiting_transaction in waiting_transactions:
                query_processor.process_transaction(waiting_transaction.transaction_id, waiting_transaction.operations)


if __name__ == "__main__":
    # Initialize CCManager instance
    cc_manager = ConcurrencyControlManager()

    # Start the simulation
    transaction_simulation(cc_manager)
