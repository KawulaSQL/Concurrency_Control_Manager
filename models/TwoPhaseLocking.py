from collections import defaultdict
from abc import ABC
from models.Schedule import Schedule
from models.Resource import Resource
from models.Response import Response
from models.Operation import Operation
from models.ControllerMethod import ControllerMethod
from models.Transaction import Transaction
from models.CCManagerEnums import OperationType, ResponseType, TransactionStatus

class TwoPhaseLocking(ControllerMethod, ABC):
    def __init__(self):
        self.schedule = Schedule()

    def log_object(self, operation: Operation): 
        """
        locking object/resource
        """
        # self.transaction_history.append({
        #     "transaction": transaction_id,
        #     "table": resource.name,
        #     "operation": "LOG",
        #     "status": "Logged"
        # })

        print(f"Logging operation for resource: {operation.getOperationResource()}")

        transaction = self.schedule.getTransactionByID(operation.getOpTransactionID())
        operationResource = self.schedule.get_or_create_resource(operation.getOperationResource())

        # Give Lock to the object/resource
        if operation.getOperationType() == OperationType.R:
            print(f"Setting resource read lock for {operationResource.getName()} to the transaction-{transaction.getTransactionID()}.")
            operationResource.addLockHolder(transaction.getTransactionID(), "S", "Locked")
        elif operation.getOperationType() == OperationType.W:
            print(f"Setting resource write lock for {operationResource.getName()} to the transaction-{transaction.getTransactionID()}.")
            operationResource.addLockHolder(transaction.getTransactionID(), "X", "Locked")

    def validate_object(self, operation: Operation) -> Response: #rough implementation
        """
        Checking lock on object/resource
        """
        # if action == Action.READ:
        #     success = self.shared_lock(transaction_id, resource.name)
        # elif action == Action.WRITE:
        #     success = self.exclusive_lock(transaction_id, resource.name)
        # else:
        #     success = False
        # return Response(success=success, message="Validation successful" if success else "Validation failed")

        print(f"Validating operation to get resource: {operation.getOperationResource()}")

        # Creating/validating the Resource object of the requested resource name
        operationResource = self.schedule.get_or_create_resource(operation.getOperationResource())
        print(f"Operation resource: {operationResource.getName()}")

        transaction = self.schedule.getTransactionByID(operation.getOpTransactionID())
        print(f"Transaction retrieved: {transaction.getTransactionID()}, Timestamp: {transaction.getTimestamp()}")

        transaction.addOperation(operation)

        # Use the lock type to determine if the transaction can proceed
        if operation.getOperationType() == OperationType.R:
            if not self.shared_lock(transaction.getTransactionID(), operationResource.getName()):
                print("Transaction cannot acquire shared lock. Transaction aborted.")
                transaction.setTransactionStatus(TransactionStatus.ABORTED)
                return Response(ResponseType.ABORT, operation)
        elif operation.getOperationType() == OperationType.W:
            if not self.exclusive_lock(transaction.getTransactionID(), operationResource.getName()):
                print("Transaction cannot acquire exclusive lock. Transaction aborted.")
                transaction.setTransactionStatus(TransactionStatus.ABORTED)
                return Response(ResponseType.ABORT, operation)
        return Response(ResponseType.ALLOWED, operation)

    def end_transaction(self, transaction_id: int):
        """
        Implementation
        """
        print(f"Ending transaction {transaction_id}.")

        transaction = self.schedule.getTransactionByID(transaction_id)
        
        # Release all locks held by the transaction but check if the transaction is still active first or if it has been aborted
        if transaction.getTransactionStatus() == TransactionStatus.ACTIVE:
            self.release_locks(transaction)
            transaction.setTransactionStatus(TransactionStatus.COMMITTED)
        else:
            self.abort(transaction_id)

        
    def shared_lock(self, transaction: int, table: str) -> bool:
        """Acquire a shared lock."""
        if table in self.exclusive_lock_table:
            if self.exclusive_lock_table[table] == transaction:
                return True  # Already holds an exclusive lock
            return False
        if transaction in self.shared_lock_table[table]:
            return True  # Already holds a shared lock
        self.shared_lock_table[table].append(transaction)
        self.log_transaction(transaction, table, "SL", "Success")
        return True

    def exclusive_lock(self, transaction: int, table: str) -> bool:
        """Acquire an exclusive lock."""
        if table in self.shared_lock_table:
            if transaction in self.shared_lock_table[table] and len(self.shared_lock_table[table]) == 1:
                self.shared_lock_table[table].remove(transaction)
                self.exclusive_lock_table[table] = transaction
                self.log_transaction(transaction, table, "UPL", "Success")
                return True
            return False
        if table in self.exclusive_lock_table:
            return self.exclusive_lock_table[table] == transaction
        self.exclusive_lock_table[table] = transaction
        self.log_transaction(transaction, table, "XL", "Success")
        return True

    def release_locks(self, transaction: Transaction):
        # """Release all locks held by a transaction."""
        # for resource in transaction.getSharedLockList():
        #     if resource in self.schedule.getResourceList():
        #         resource.deleteLockHolder(transaction, S)
        # for resource in transaction.getExclusiveLockList():
        #     if resource in self.schedule.getResourceList():
        #         resource.deleteLockHolder(transaction, X)
        pass
        # for table, holder in list(self.exclusive_lock_table.items()):
        #     if holder == transaction:
        #         del self.exclusive_lock_table[table]
        #         self.log_transaction(transaction, table, "UL", "Success")
        # for table, holders in list(self.shared_lock_table.items()):
        #     if transaction in holders:
        #         holders.remove(transaction)
        #         self.log_transaction(transaction, table, "UL", "Success")
        #         if not holders:
        #             del self.shared_lock_table[table]

    def wait_die(self, current: dict):
        """ IMPLEMENT THE LOGIC IN VALIDATE_OBJECT FUNCTION
        Deadlock prevention using the wait-die scheme."""
        # transaction = current["transaction"]
        # table = current["table"]
        # if (
        #     table in self.exclusive_lock_table and
        #     self.timestamp.index(transaction) < self.timestamp.index(self.exclusive_lock_table[table])
        # ) or (
        #     table in self.shared_lock_table and
        #     all(self.timestamp.index(transaction) < self.timestamp.index(t) for t in self.shared_lock_table[table])
        # ):
        #     self.queue.append(current)
        #     self.log_transaction(transaction, table, current["operation"], "Queue")
        # else:
        #     self.abort(current)

    def wound_wait(self, current: dict):
        """Deadlock prevention using the wound-wait scheme."""
        

    # def commit(self, transaction_id: int):
    #     """Commit a transaction and release its locks."""
        # self.release_locks(transaction)
        # self.log_transaction(transaction, "-", "Commit", "Success")

    # def abort(self, transaction_id: int):
    #     """Abort a transaction."""
        # transaction = current["transaction"]
        # transaction_id = transaction.getTransactionID
        # self.schedule.setOperationQueue([op for op in self.schedule.getOperationQueue if op.getOpTransactionID != transaction_id])
        # self.schedule.setOperationWaitingList([op for op in self.schedule.getOperationWaitingList if op.getOpTransactionID != transaction_id])
        # self.release_locks(transaction)

    # def abort(self, current: dict):
    #     """Abort a transaction."""
    #     transaction = current["transaction"]
    #     self.sequence = [op for op in self.sequence if op["transaction"] != transaction]
    #     self.result = [op for op in self.result if op["transaction"] != transaction]
    #     self.release_locks(transaction)
    #     self.log_transaction(transaction, current.get("table", "-"), "Abort", "Abort")
