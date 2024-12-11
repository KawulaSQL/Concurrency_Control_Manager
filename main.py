from models.Schedule import Schedule
from models.Transaction import Transaction
from models.Operation import Operation
from models.Response import Response
from models.CCManagerEnums import OperationType, ResponseType, TransactionStatus
from ConcurrencyControlManager import ConcurrencyControlManager

class QueryProcessor:
    def __init__(self):
        """Initialize the Query Processor with a schedule instance."""
        self.schedule = Schedule()
        self.ccm = ConcurrencyControlManager()  # Assuming Concurrency Control Manager is initialized elsewhere

    def execute(self):
        """Simulate processing queries."""
        while True:
            query = input("Enter query (or 'exit' to quit): ")
            if query.lower() == 'exit':
                break

            # Start a transaction for the query
            transaction_id = self.ccm.begin_transaction()
            print(f"Transaction {transaction_id} started.")

            # Parse query to create operations (simplified example)
            operations = self.parse_query(query, transaction_id)

            for operation in operations:
                # Validate operation
                response = self.ccm.validate_object(operation)

                if response.responseType == ResponseType.ALLOWED:
                    # Log operation
                    self.ccm.log_object(response.operation)
                    print(f"Operation {response.operation.getOperationID()} logged.")

                elif response.responseType == ResponseType.WAIT:
                    print(f"Operation {operation.getOperationID()} is waiting.")
                    continue

                elif response.responseType == ResponseType.ABORT:
                    print(f"Transaction {transaction_id} aborted.")
                    self.ccm.end_transaction(transaction_id)
                    return

            # End transaction if all operations are successful
            self.ccm.end_transaction(transaction_id)
            print(f"Transaction {transaction_id} ended successfully.")

    def parse_query(self, query: str, transaction_id: int):
        """Parse a query string into operations (simplified for demonstration)."""
        operations = []
        for part in query.split(','):
            resource_name, op_type = part.strip().split()
            print(resource_name)
            print(op_type)
            operation = Operation(
                transactionID=transaction_id,
                typeOp=OperationType[op_type.upper()],
                res=resource_name
            )
            operations.append(operation)
        return operations

if __name__ == "__main__":
    qp = QueryProcessor()
    qp.execute()
