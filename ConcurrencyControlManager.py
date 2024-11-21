from controller.TwoPhaseLocking import TwoPhaseLocking
from controller.MVCC import MVCC
from controller.ControllerMethod import ControllerMethod
from model.Schedule import Schedule
from model.Transaction import Transaction
from model.Operation import Operation
from model.Response import Response
from model.Action import Action
from model.Resource import Resource

class ConcurrencyControlManager:
    """Manages concurrency control for transactions."""

    def __init__(self, controller=TwoPhaseLocking):
        """
        Initialize the ConcurrencyControlManager.

        :param controller: An instance of ControllerMethod to handle locking.
        :param schedule: An instance of Schedule to manage transactions.
        """
        self.controller = controller()  
        self.schedule = Schedule.get_instance() 

    def refreshConcurrencyController(query: Query):
        """
        When query processor send the transactional or non-transactional queries to be processed, 
        this procedure will be called to change the schedule state.

        :param query: Query are the class of queries transactional or non transactional that is sent from QueryProcessor
        """

    def refreshConcurrencyController(executedQuery: ExecutedQuery):
        """
        When query processor send the log of executed queries, this procedure will be called to change the schedule state.

        :param query: ExecutedQuery are the class of executed query that is sent from QueryProcessor
        """

    def begin_transaction(self, query: Query) -> int:
        """
        Start a new transaction. This will give the new Query (queries) transaction id 
        and construct a new Transaction object

        :param query: Query are the class of queries transactional or non transactional that is sent from QueryProcessor
        :return: The ID of the new transaction.
        """

    def end_transaction(self, transaction_id: int):
        """
        Flushes objects belonging to a particular transaction after it has successfully committed/aborted. 
        Also terminates the transaction.

        :param transaction_id: The ID of the transaction to end.
        """

    def request_rollback(self, transaction: Transaction):
        """
        This will notify the Failure Recovery Manager regarding the Transaction to be rolled back.

        :param transaction: Transaction to be rolled back.
        """

    def __repr__(self):
        """
        Represent the current state of the concurrency control manager.

        :return: A string representation of the manager's state.
        """
        return f"ConcurrencyControlManager(Controller: {self.controller}, Schedule: {self.schedule}, History: {self.history})"
