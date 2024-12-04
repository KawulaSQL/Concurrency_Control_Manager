from models.TwoPhaseLocking import TwoPhaseLocking
from models.TimeStampOrdering import TimestampOrdering
from models.ControllerMethod import ControllerMethod
from models.Schedule import Schedule
from models.Transaction import Transaction
from models.Operation import Operation
from models.Response import Response
from models.CCManagerEnums import ResponseType, OperationStatus, TransactionStatus
import time

class ConcurrencyControlManager:
    """Manages concurrency control for transactions."""

    def __init__(self, controller="2PL"):
        """
        Initialize the ConcurrencyControlManager.

        :param controller: A string to specify the concurrency control method ("2PL" or "MVCC").
                            Default is "2PL".
        """
        if controller == "TSO":
            self.controller = TimestampOrdering()
        else:
            self.controller = TwoPhaseLocking()  # Default to TwoPhaseLocking if not "MVCC"
        
        self.schedule = Schedule(opQueue=[], resList=[], txList=[])
        self.running = True

    def begin_transaction(self, queries: list[Operation]) -> int:
        """
        Start a new transaction. This will give the new Query (queries) transaction id 
        and construct a new Transaction object. When query processor send the transactional or non-transactional queries to be processed, 
        this procedure will be called to change the schedule state.

        :param queries: list[Operation] are the class of queries transactional or non transactional that is sent from QueryProcessor
        :return: The ID of the new transaction.
        """
        new_transaction = Transaction(queries) #Initialize new transaction
        self.schedule.addTransaction(new_transaction) #Adding the new transaction to transaction list
        for operation in new_transaction.operationList: #Enqueue all operation in queue list
            self.schedule.enqueue(operation)
            for operation_resource in operation.getOperationResource: #For each operation, add the resource needed to schedule resource
                self.schedule.addResource(operation_resource)
        return new_transaction.getTransactionID()

    def notify_executed_query(self, executedQuery: Operation):
        """
        When query processor send the log of executed queries, this procedure will be called to change the schedule state.

        :param executedQuery: Operation are the class of executed query that is sent from QueryProcessor
        """
        transaction_id = executedQuery.getOpTransactionID()
        transaction = next((tx for tx in self.schedule.getTransactionList if tx.getTransactionID == transaction_id), None)
        #Update Operation Status, if Operation failed, abort transaction, request recovery manager to rollback
        if executedQuery.getOperationStatus() == OperationStatus.NE:
            transaction.setTransactionStatus(TransactionStatus.FAILED)
            self.request_rollback(transaction_id)
        #Update Transaction Status, if all Operations executed: mark as Partially Committed
        if all(op.getOperationStatus() == OperationStatus.E for op in transaction.getOperationList()):
            transaction.setTransactionStatus(TransactionStatus.PARTIALLYCOMMITTED)


    def notify_transaction_state(self, transaction_id: int, action: str):
        """
        Recovery manager call this function to change the transaction state after action commit or abort.
        This procedure will change the transaction status and call the end_transaction procedure.

        :param transaction_id: id of transaction
        :param action: string ("Committed", "Aborted")
        """
        transaction = next((tx for tx in self.schedule.getTransactionList if tx.getTransactionID == transaction_id), None) #Acquiring the transaction
        if not transaction:
            print(f"Transaction with ID {transaction_id} not found.")
            return
        if action=="Committed":
            transaction.setTransactionStatus(TransactionStatus.COMMITTED)
            print(f"Transaction with ID {transaction_id} is committed.")
        elif action=="Aborted":
            transaction.setTransactionStatus(TransactionStatus.ABORTED)
            print(f"Transaction with ID {transaction_id} is aborted.")
        self.end_transaction(transaction)
        

    def end_transaction(self, transaction: Transaction):
        """
        Flushes objects belonging to a particular transaction after it has successfully committed/aborted. 
        Also terminates the transaction.

        :param transaction_id: The ID of the transaction to end.
        """
        transaction.setTransactionStatus(TransactionStatus.TERMINATED) #Choose between below or this
        print(f"Transaction with ID {transaction.getTransactionID} is terminated.")
        self.schedule.setTransactionList([tx for tx in self.transactionList if tx.txID != transaction.getTransactionID]) #Choose between above or this

    def request_rollback(self, transaction: int):
        """
        This will notify the Failure Recovery Manager regarding the Transaction to be rolled back, 
        calling function from failure recovery manager.

        :param transaction: int the transaction id to be rolled back.
        """

    def send_response_to_processor(self, response: Response):
        """
        This will notify the Processor regarding the Query that can run/wait,
        calling function from failure recovery manager.
        """


    def run(self):
        while self.running:
            while self.schedule.getOperationWaitingList():
                current_waiting_operation = self.schedule.dequeueWaiting()
                response = self.controller.process_query(current_waiting_operation)
                self.send_response_to_processor(response)
                if response.responseType==ResponseType.ABORT:    
                    self.request_rollback(current_waiting_operation.getOpTransactionID)

            if not self.schedule.getOperationQueue(): 
                time.sleep(1)
                continue 
            else:
                current_operation = self.schedule.dequeue()
                self.controller.process_query(current_operation)
                response = self.controller.process_query(current_operation)
                self.send_response_to_processor(response)
                if response.responseType==ResponseType.ABORT:    
                    self.request_rollback(current_operation.getOpTransactionID)
                    
    def stop(self):
        """Stop the Concurrency Control Manager's run loop."""
        self.running = False

    # def __repr__(self):
    #     """
    #     Represent the current state of the concurrency control manager.

    #     :return: A string representation of the manager's state.
    #     """
    #     return f"ConcurrencyControlManager(Controller: {self.controller}, Schedule: {self.schedule}, History: {self.history})"
