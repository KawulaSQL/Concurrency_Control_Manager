from ControllerMethod import ControllerMethod
from CCManagerEnums import Action, ResponseType, OperationType, TransactionStatus
from Response import Response
from datetime import datetime
from Schedule import Schedule,Operation,Resource
class TimestampOrdering(ControllerMethod):
    def __init__(self):
        self.schedule = Schedule()
        print("TimestampOrdering initialized, schedule created.")

    def validate_object(self, operation: Operation) -> Response: 
        """
        Validating timestamp on object/resource
        """
        print(f"Validating operation to get resource: {operation.getOperationResource()}")
        
        # Creating/validating the Resource object of the requested resource name
        operationResource = self.schedule.get_or_create_resource(operation.getOperationResource)
        print(f"Operation resource: {operationResource.getName()}")

        transaction = self.schedule.getTransactionByID(operation.getOpTransactionID)
        print(f"Transaction retrieved: {transaction.txID}, Timestamp: {transaction.getTimestamp()}")

        transaction.addOperation(operation)

        if operation.getOperationType() == OperationType.R:
            if transaction.getTimestamp() < operationResource.getWTS():
                print("Transaction timestamp is earlier than resource write timestamp. Transaction aborted.")
                transaction.setTransactionStatus(TransactionStatus.ABORTED)
                return Response(ResponseType.ABORT, operation)

            if transaction.getTimestamp() > operationResource.getRTS():
                print("Transaction timestamp is after resource read timestamp. Transaction allowed.")
                return Response(ResponseType.ALLOWED, operation)
        elif operation.getOperationType() == OperationType.W:
            print("Operation type is WRITE.")
            if transaction.getTimestamp() < operationResource.getWTS() or transaction.getTimestamp() < operationResource.getRTS():
                print("Transaction timestamp is earlier than resource timestamps. Transaction aborted.")
                transaction.setTransactionStatus(TransactionStatus.ABORTED)
                return Response(ResponseType.ABORT, operation)
            print("Transaction timestamp is after resource read/writw timestamp. Transaction allowed.")
            return Response(ResponseType.ALLOWED, operation)

    def log_object(self, operation: Operation): 
        """
        Give timestamp to object.resource
        """
        print(f"Logging operation for resource: {operation.getOperationResource()}")
        
        transaction = self.schedule.getTransactionByID(operation.getOpTransactionID)
        operationResource = self.schedule.get_or_create_resource(operation.getOperationResource)

        if operation.getOperationType() == OperationType.R:
            print(f"Setting resource read timestamp for {operationResource.getName()} to the timestamp of Transaction-{transaction.getTransactionID()}.")
            operationResource.setRTS(max(operationResource.getRTS(), transaction.getTimestamp()))
        elif operation.getOperationType() == OperationType.W:
            print(f"Setting resource write timestamp for {operationResource.getName()} to the timestamp of Transaction-{transaction.getTransactionID()}.")
            operationResource.setWTS(transaction.getTimestamp())

    def end_transaction(self, transaction_id: int):
        """
        Implementation
        """
        print(f"Ending transaction {transaction_id}.")
        
        transaction = self.schedule.getTransactionByID(transaction_id)

        if transaction.getTransactionStatus() == TransactionStatus.ABORTED:
            print(f"Transaction {transaction_id} aborted, adding to waiting list.")
            self.schedule.addWaitingTransaction(transaction)

        self.schedule.removeTransaction(transaction)
        transaction.setTransactionStatus(TransactionStatus.TERMINATED)
        print(f"Transaction {transaction_id} removed and terminated.")