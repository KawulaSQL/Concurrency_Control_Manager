from models.ControllerMethod import ControllerMethod
from models.Resource import Resource
from models.CCManagerEnums import ResponseType, OperationType, TransactionStatus
from models.Response import Response
from models.Operation import Operation
from models.Schedule import Schedule
from datetime import datetime
from models.Schedule import Schedule

YELLOW = '\033[33m'
RESET = '\033[0m'

class TimestampOrdering(ControllerMethod):
    def __init__(self):
        self.schedule = Schedule()
        print(YELLOW + "TimestampOrdering initialized, schedule created." + RESET)

    def validate_object(self, operation: Operation) -> Response: 
        """
        Validating timestamp on object/resource
        """
        print(f"{YELLOW}Validating operation to get resource: {operation.getOperationResource()}{RESET}")
        # Creating/validating the Resource object of the requested resource name
        operationResource = self.schedule.get_or_create_resource(operation.getOperationResource())
        print(f"{YELLOW}Operation resource: {operationResource.getName()}{RESET}")

        #Checking whether operation is aborted before
        transaction = self.schedule.getWaitingTransaction(operation.getOpTransactionID())
        if (transaction):
            transaction.setTransactionStatus(TransactionStatus.ACTIVE)
            transaction.setTimestamp(datetime.now())
            print(f"{YELLOW}Transaction reactivated: {transaction.getTransactionID()}, Timestamp: {transaction.getTimestamp()}{RESET}")
            self.schedule.removeTransaction(transaction)
        else:
            transaction = self.schedule.getTransactionByID(operation.getOpTransactionID())
            print(f"{YELLOW}Transaction retrieved: {transaction.getTransactionID()}, Timestamp: {transaction.getTimestamp()}{RESET}")
        transaction.addOperation(operation)

        if operation.getOperationType() == OperationType.R:
            if transaction.getTimestamp() < operationResource.getWTS():
                print(YELLOW + "Transaction timestamp is earlier than resource write timestamp. Transaction aborted." + RESET)
                transaction.setTransactionStatus(TransactionStatus.ABORTED)
                return Response(ResponseType.ABORT, operation)
            else:
                print(YELLOW + "Transaction timestamp is after resource read timestamp. Transaction allowed." + RESET)
                self.log_object(operation)
                return Response(ResponseType.ALLOWED, operation)
        elif operation.getOperationType() == OperationType.W:
            if transaction.getTimestamp() < operationResource.getWTS() or transaction.getTimestamp() < operationResource.getRTS():
                print(YELLOW + "Transaction timestamp is earlier than resource timestamps. Transaction aborted." + RESET)
                transaction.setTransactionStatus(TransactionStatus.ABORTED)
                return Response(ResponseType.ABORT, operation)
            print(YELLOW + "Transaction timestamp is after resource read/write timestamp. Transaction allowed." + RESET)
            self.log_object(operation)
            return Response(ResponseType.ALLOWED, operation)

    def log_object(self, operation: Operation): 
        """
        Give timestamp to object.resource
        """
        print(f"{YELLOW}Logging operation for resource: {operation.getOperationResource()}{RESET}")
        
        transaction = self.schedule.getTransactionByID(operation.getOpTransactionID())
        operationResource = self.schedule.get_or_create_resource(operation.getOperationResource())

        if operation.getOperationType() == OperationType.R:
            print(f"{YELLOW}Setting resource read timestamp for {operationResource.getName()} to the timestamp of Transaction-{transaction.getTransactionID()}.{RESET}")
            operationResource.setRTS(max(operationResource.getRTS(), transaction.getTimestamp()))
        elif operation.getOperationType() == OperationType.W:
            print(f"{YELLOW}Setting resource write timestamp for {operationResource.getName()} to the timestamp of Transaction-{transaction.getTransactionID()}.{RESET}")
            operationResource.setWTS(transaction.getTimestamp())

    def end_transaction(self, transaction_id: int):
        """
        Implementation
        """
        print(f"{YELLOW}Ending transaction {transaction_id}.{RESET}")
        
        transaction = self.schedule.getTransactionByID(transaction_id)

        if transaction.getTransactionStatus() == TransactionStatus.ABORTED:
            print(f"{YELLOW}Transaction {transaction_id} aborted, adding to waiting list.{RESET}")
            self.schedule.addWaitingTransaction(transaction)

        self.schedule.removeTransaction(transaction)
        transaction.setTransactionStatus(TransactionStatus.TERMINATED)
        print(f"{YELLOW}Transaction {transaction_id} removed and terminated.{RESET}")