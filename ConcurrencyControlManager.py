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

    def begin_transaction() -> int:
        """
        Start a new transaction. This will give the new Query (queries) transaction id 
        and construct a new Transaction object. When query processor send the transactional or non-transactional queries to be processed, 
        this procedure will be called to change the schedule state.
        """
        new_transaction = Transaction() #Initialize new transaction
        self.schedule.addTransaction(new_transaction) #Adding the new transaction to transaction list
        return new_transaction.getTransactionID()

    def log_object(self, object: Operation):
        """
        Query processor call this function after getting validating operation can be run
        """


    def validate_object(self, object: Operation) -> Response:
        """
        Query processor call this function to get the information whether the operation can be run or not.
        """

    def end_transaction(self, transaction_id: int):
        """
        Flushes objects belonging to a particular transaction after it has successfully committed/aborted.
        e.g. unlocking resources
        Also terminates the transaction.

        :param transaction_id: The ID of the transaction to end.
        """
        #Unlock
        transaction.setTransactionStatus(TransactionStatus.TERMINATED) #Choose between below or this
        print(f"Transaction with ID {transaction.getTransactionID} is terminated.")
        self.schedule.setTransactionList([tx for tx in self.transactionList if tx.txID != transaction.getTransactionID]) #Choose between above or this
