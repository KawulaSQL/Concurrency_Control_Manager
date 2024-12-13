from collections import defaultdict
from typing import Optional, Dict, List
from models.ControllerMethod import ControllerMethod
from models.Schedule import Schedule
from models.Operation import Operation
from models.Response import Response
from models.CCManagerEnums import OperationType, ResponseType, TransactionStatus

class TwoPhaseLockingv3(ControllerMethod):
    def __init__(self, deadlock_prevention: str = "WAIT-DIE"):
        self.sequence = []
        self.schedule = Schedule()
        self.transactionIDSet = []
        self.exclusive_lock_table = {}
        self.shared_lock_table = defaultdict(list)
        self.transaction_history = []
        self.result = []
        self.wait_sequence = [] #semua sequence yang masuk ke wait 
        self.deadlock_strategy = deadlock_prevention
        self.holder = None
    def validate_object(self, operation: Operation) -> Response:
        """
        Validating Two Phase Locking with deadlock prevention
        """
        print(f"Validating operation to get resource: {operation.getOperationResource()}")
        
        operationResource = self.schedule.get_or_create_resource(operation.getOperationResource())
        transaction = self.schedule.getTransactionByID(operation.getOpTransactionID())
        transactionWaitingList = self.schedule.getTransactionWaitingList()
        
        if not transaction:
            return Response(ResponseType.ABORT, operation)
        
        if transaction.getTransactionID() in transactionWaitingList:
            if self.schedule.checkWaitingTransactionBlocker(transaction.getTransactionID()):
                transaction.setTransactionStatus(TransactionStatus.ACTIVE)
            else:
                return Response(ResponseType.ABORT, operation)
        print(f"Operation resource: {operationResource.getName()}")
        print(f"Transaction retrieved ID: {transaction.getTransactionID()}, Status: {transaction.getTransactionStatus()}")

        transaction.addOperation(operation)

        if operation.getOperationType() == OperationType.R:
            return self._handle_read_operation(transaction, operationResource, operation)
        elif operation.getOperationType() == OperationType.W:
            return self._handle_write_operation(transaction, operationResource, operation)
        else:  # COMMIT
            return self._handle_commit_operation(transaction, operation)

    def _handle_read_operation(self, transaction, resource, operation) -> Response:
        if self.shared_lock(transaction, resource):
            print(f"Resource {resource.getName()} has shared lock granted on transaction {transaction.getTransactionID()}")
            return Response(ResponseType.ALLOWED, operation)
        
        holder_id = self.exclusive_lock_table.get(resource.getName()) # Jika ada exclusive lock
        if holder_id:
            return self._apply_deadlock_prevention(
                requesting_tx=transaction,
                holding_tx_id=holder_id,
                operation=operation
            )
        return Response(ResponseType.ALLOWED, operation)

    def _handle_write_operation(self, transaction, resource, operation) -> Response:
        if self.exclusive_lock(transaction, resource):
            print(f"Resource {resource.getName()} has exclusive lock granted")
            return Response(ResponseType.ALLOWED, operation)
        
        holder_id = None
        if resource.getName() in self.exclusive_lock_table:
            holder_id = self.exclusive_lock_table[resource.getName()]
        elif resource.getName() in self.shared_lock_table:
            shared_holders = self.shared_lock_table[resource.getName()]
            if shared_holders:
                holder_id = max(shared_holders)
        
        if holder_id:
            return self._apply_deadlock_prevention(
                requesting_tx=transaction,
                holding_tx_id=holder_id,
                operation=operation
            )
        return Response(ResponseType.ALLOWED, operation)

    def _handle_commit_operation(self, transaction, operation) -> Response:
        transaction.setTransactionStatus(TransactionStatus.COMMITTED)
        self.release_locks(transaction)
        return Response(ResponseType.ALLOWED, operation)

    def _apply_deadlock_prevention(self, requesting_tx, holding_tx_id: int, operation: Operation) -> Response:
        if self.deadlock_strategy == "WAIT-DIE":
            return self._wait_die_strategy(requesting_tx, holding_tx_id, operation)
        else:
            return self._wound_wait_strategy(requesting_tx, holding_tx_id, operation)

    def _wait_die_strategy(self, requesting_tx, holding_tx_id: int, operation: Operation) -> Response:
        if holding_tx_id > requesting_tx.getTransactionID():
            # Yang older wait
            print(f"Wait-Die: T{requesting_tx.getTransactionID()} (older) waiting for T{holding_tx_id} (younger)")
            requesting_tx.setTransactionStatus(TransactionStatus.WAITING)
            self.wait_sequence.append(operation)
            
            return Response(ResponseType.WAITING, operation)
        else:
            # Yang younger abort
            print(f"Wait-Die: T{requesting_tx.getTransactionID()} (younger) aborted")
            requesting_tx.setTransactionStatus(TransactionStatus.ABORTED)
            self.release_locks(requesting_tx)
            self.holder = holding_tx_id
            return Response(ResponseType.ABORT, operation)

    def _wound_wait_strategy(self, requesting_tx, holding_tx_id: int, operation: Operation) -> Response:
        if holding_tx_id > requesting_tx.getTransactionID():
            # Yang older wounding
            print(f"Wound-Wait: T{requesting_tx.getTransactionID()} (older) wounding T{holding_tx_id} (younger)")
            holding_tx = self.schedule.getTransactionByID(holding_tx_id)
            if holding_tx:
                holding_tx.setTransactionStatus(TransactionStatus.ABORTED)
                self.release_locks(holding_tx)
            return Response(ResponseType.ALLOWED, operation)
        else:
            # Yang younger wait
            print(f"Wound-Wait: T{requesting_tx.getTransactionID()} (younger) waiting for T{holding_tx_id} (older)")
            requesting_tx.setTransactionStatus(TransactionStatus.WAITING)
            self.wait_sequence.append(operation)
            self.holder = holding_tx_id
            return Response(ResponseType.WAITING, operation)

    def shared_lock(self, transaction, resource) -> bool:
        resource_name = resource.getName()
        
        if transaction.getTransactionID() in self.shared_lock_table[resource_name]:
            return True
            
        if resource_name in self.exclusive_lock_table: # Cek apakah ada exclusive lock
            return self.exclusive_lock_table[resource_name] == transaction.getTransactionID()
            
        self.shared_lock_table[resource_name].append(transaction.getTransactionID())
        return True

    def exclusive_lock(self, transaction, resource) -> bool:
        resource_name = resource.getName()
        
        if resource_name in self.exclusive_lock_table: # Cek apakah ada exclusive lock
            return self.exclusive_lock_table[resource_name] == transaction.getTransactionID()
            
        if resource_name in self.shared_lock_table: # Cek apakah ada shared lock
            shared_holders = self.shared_lock_table[resource_name]
            if len(shared_holders) == 1 and shared_holders[0] == transaction.getTransactionID():
                # Upgrade lock
                del self.shared_lock_table[resource_name]
                self.exclusive_lock_table[resource_name] = transaction.getTransactionID()
                return True
            return False
            
        self.exclusive_lock_table[resource_name] = transaction.getTransactionID()
        return True

    def release_locks(self, transaction):
        transaction_id = transaction.getTransactionID()
        
        for resource_name in list(self.exclusive_lock_table.keys()):
            if self.exclusive_lock_table[resource_name] == transaction_id:
                del self.exclusive_lock_table[resource_name]
                
        for resource_name in list(self.shared_lock_table.keys()):
            if transaction_id in self.shared_lock_table[resource_name]:
                self.shared_lock_table[resource_name].remove(transaction_id)
                if not self.shared_lock_table[resource_name]:
                    del self.shared_lock_table[resource_name]

    def log_object(self, operation: Operation):
        print(f"Logging operation for resource: {operation.getOperationResource()}")
        print(f"Shared Lock Table: {dict(self.shared_lock_table)}")
        print(f"Exclusive Lock Table: {self.exclusive_lock_table}")
        print(f"Waiting Transactions: {self.schedule.getTransactionWaitingList()}")

    def end_transaction(self, transaction_id: int):
        print(f"Ending transaction {transaction_id}")
        
        transaction = self.schedule.getTransactionByID(transaction_id)
        if not transaction:
            return

        if transaction.getTransactionStatus() == TransactionStatus.ABORTED:
            print(f"Transaction {transaction_id} aborted, adding to waiting list")
            self.schedule.addWaitingTransaction(transaction,self.holder)

        self.schedule.removeTransaction(transaction)
        self.release_locks(transaction)
        transaction.setTransactionStatus(TransactionStatus.TERMINATED)
        print(f"Transaction {transaction_id} removed and terminated")

        print(f"Transaction List: {self.schedule.getTransactionList().keys()}")