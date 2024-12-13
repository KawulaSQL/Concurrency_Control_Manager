from collections import defaultdict
from typing import Optional, Dict, List
from models.ControllerMethod import ControllerMethod
from models.Schedule import Schedule
from models.Operation import Operation
from models.Response import Response
from models.CCManagerEnums import OperationType, ResponseType, TransactionStatus, LockType, LockStatus

YELLOW = '\033[33m'
RESET = '\033[0m'

class TwoPhaseLocking(ControllerMethod):
    def __init__(self, deadlock_prevention: str = "WAIT-DIE"):
        self.sequence = []
        self.schedule = Schedule()
        self.wait_sequence = []
        self.deadlock_strategy = deadlock_prevention
        self.holder = None

    def validate_object(self, operation: Operation) -> Response:
        print(f"{YELLOW}Validating operation to get resource: {operation.getOperationResource()}{RESET}")
        
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

        print(f"{YELLOW}Operation resource: {operationResource.getName()}{RESET}")
        print(f"{YELLOW}Transaction retrieved ID: {transaction.getTransactionID()}, Status: {transaction.getTransactionStatus()}{RESET}")

        transaction.addOperation(operation)
        
        if operation.getOperationType() == OperationType.R:
            return self._handle_read_operation(transaction, operationResource, operation)
        elif operation.getOperationType() == OperationType.W:
            return self._handle_write_operation(transaction, operationResource, operation)
        
        return Response(ResponseType.ABORT, operation)

    def _handle_read_operation(self, transaction, resource, operation) -> Response:
        lock_holders = resource.getLockHolderList()
        transaction_id = transaction.getTransactionID()
        
        for holder_id, lock_type, lock_status in lock_holders:
            if lock_type == LockType.X and holder_id != transaction_id and lock_status == LockStatus.HOLDING:
                return self._apply_deadlock_prevention(
                    requesting_tx=transaction,
                    holding_tx_id=holder_id,
                    operation=operation
                )
        return Response(ResponseType.ALLOWED, operation)

    def _handle_write_operation(self, transaction, resource, operation) -> Response:
        lock_holders = resource.getLockHolderList()
        transaction_id = transaction.getTransactionID()
        
        for holder_id, lock_type, lock_status in lock_holders:
            if holder_id != transaction_id and lock_status == LockStatus.HOLDING:
                if lock_type == LockType.X or (lock_type == LockType.S and len(lock_holders) > 1) or (lock_type == LockType.S and holder_id != transaction_id):
                    return self._apply_deadlock_prevention(
                        requesting_tx=transaction,
                        holding_tx_id=holder_id,
                        operation=operation
                    )
        return Response(ResponseType.ALLOWED, operation)

    def log_object(self, operation: Operation):
        print(f"{YELLOW}Logging operation for resource: {operation.getOperationResource()}{RESET}")
        
        transaction = self.schedule.getTransactionByID(operation.getOpTransactionID())
        resource = self.schedule.get_or_create_resource(operation.getOperationResource())
        
        if operation.getOperationType() == OperationType.R:
            self._grant_shared_lock(transaction, resource)
        elif operation.getOperationType() == OperationType.W:
            self._grant_exclusive_lock(transaction, resource)
        
        print(f"{YELLOW}Lock holders for {resource.getName()}: {resource.getLockHolderList()}{RESET}")
        print(f"{YELLOW}Waiting Transactions: {self.schedule.getTransactionWaitingList()}{RESET}")

    def _grant_shared_lock(self, transaction, resource):
        transaction_id = transaction.getTransactionID()
        lock_holders = resource.getLockHolderList()
        
        for i, (holder_id, lock_type, lock_status) in enumerate(lock_holders):
            if holder_id == transaction_id:
                if lock_type == LockType.X:
                    return
                elif lock_type == LockType.S:
                    return
        
        lock_holders.append((transaction_id, LockType.S, LockStatus.HOLDING))
        print(f"{YELLOW}Resource {resource.getName()} has shared lock granted on transaction {transaction_id}{RESET}")

    def _grant_exclusive_lock(self, transaction, resource):
        transaction_id = transaction.getTransactionID()
        lock_holders = resource.getLockHolderList()
        
        for i, (holder_id, lock_type, lock_status) in enumerate(lock_holders):
            if holder_id == transaction_id:
                if lock_type == LockType.X:
                    return
                elif lock_type == LockType.S and len(lock_holders) == 1 and transaction_id == holder_id:
                    lock_holders[i] = (transaction_id, LockType.X, LockStatus.HOLDING)
                    print(f"{YELLOW}Resource {resource.getName()} lock upgraded to exclusive for transaction {transaction_id}{RESET}")
                    return
        
        lock_holders.append((transaction_id, LockType.X, LockStatus.HOLDING))
        print(f"{YELLOW}Resource {resource.getName()} has exclusive lock granted{RESET}")

    def release_locks(self, transaction):
        transaction_id = transaction.getTransactionID()
        
        for resource_name in self.schedule.getResourceList():
            resource = self.schedule.getResourceList().get(resource_name)
            if resource:
                lock_holders = resource.getLockHolderList()
                lock_holders[:] = [(h_id, l_type, l_status) for h_id, l_type, l_status in lock_holders 
                                 if h_id != transaction_id]

    def _apply_deadlock_prevention(self, requesting_tx, holding_tx_id: int, operation: Operation) -> Response:
        if self.deadlock_strategy == "WAIT-DIE":
            return self._wait_die_strategy(requesting_tx, holding_tx_id, operation)
        else:
            return self._wound_wait_strategy(requesting_tx, holding_tx_id, operation)

    def _wait_die_strategy(self, requesting_tx, holding_tx_id: int, operation: Operation) -> Response:
        if holding_tx_id > requesting_tx.getTransactionID():
            print(f"{YELLOW}Wait-Die: T{requesting_tx.getTransactionID()} (older) waiting for T{holding_tx_id} (younger){RESET}")
            requesting_tx.setTransactionStatus(TransactionStatus.WAITING)
            self.wait_sequence.append(operation)
            return Response(ResponseType.WAITING, operation)
        else:
            print(f"{YELLOW}Wait-Die: T{requesting_tx.getTransactionID()} (younger) aborted{RESET}")
            requesting_tx.setTransactionStatus(TransactionStatus.ABORTED)
            self.release_locks(requesting_tx)
            self.holder = holding_tx_id
            return Response(ResponseType.ABORT, operation)

    def _wound_wait_strategy(self, requesting_tx, holding_tx_id: int, operation: Operation) -> Response:
        if holding_tx_id > requesting_tx.getTransactionID():
            print(f"{YELLOW}Wound-Wait: T{requesting_tx.getTransactionID()} (older) wounding T{holding_tx_id} (younger){RESET}")
            holding_tx = self.schedule.getTransactionByID(holding_tx_id)
            if holding_tx:
                holding_tx.setTransactionStatus(TransactionStatus.ABORTED)
                self.release_locks(holding_tx)
            return Response(ResponseType.ALLOWED, operation)
        else:
            print(f"{YELLOW}Wound-Wait: T{requesting_tx.getTransactionID()} (younger) waiting for T{holding_tx_id} (older){RESET}")
            requesting_tx.setTransactionStatus(TransactionStatus.WAITING)
            self.wait_sequence.append(operation)
            self.holder = holding_tx_id
            return Response(ResponseType.WAITING, operation)

    def end_transaction(self, transaction_id: int):
        print(f"{YELLOW}Ending transaction {transaction_id}{RESET}")
        
        transaction = self.schedule.getTransactionByID(transaction_id)
        if not transaction:
            return

        if transaction.getTransactionStatus() == TransactionStatus.ABORTED:
            print(f"{YELLOW}Transaction {transaction_id} aborted, adding to waiting list{RESET}")
            self.schedule.addWaitingTransaction(transaction, self.holder)

        self.schedule.removeTransaction(transaction)
        self.release_locks(transaction)
        transaction.setTransactionStatus(TransactionStatus.TERMINATED)
        print(f"{YELLOW}Transaction {transaction_id} removed and terminated{RESET}")
        print(f"{YELLOW}Transaction List: {self.schedule.getTransactionList().keys()}{RESET}")