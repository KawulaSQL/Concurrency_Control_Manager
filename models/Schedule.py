from models.Resource import Resource
from models.Transaction import Transaction
from models.Operation import Operation
from models.CCManagerEnums import LockType,TransactionStatus
from datetime import datetime, time
class Schedule:
    _instance = None

    def __new__(cls):
        """
        Ensure only one instance of Schedule exists.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.resourceList = {}  # Dictionary: {resource_name: Resource object}
            cls._instance.transactionList = {}  # Dictionary: {txID: Transaction object} for fast lookup by txID
            cls._instance.transactionWaitingList = {}  # Dictionary: {txID: tuple(Transaction object, id_transaction int, TransactionWaitingStatus Enum))} for waiting transactions
        return cls._instance

    def __init__(self):
        """
        Initialize the Schedule if it's the first time being created.
        """
        pass

    def get_or_create_resource(self, resource_name: str) -> Resource:
        """
        Check if a resource with the given name exists in resourceList.
        If it exists, return the Resource object.
        If not, create a new Resource, add it to resourceList, and return it.

        :param resource_name: The name of the resource to find or create.
        :return: The existing or newly created Resource object.
        """
        if resource_name in self.resourceList:
            return self.resourceList[resource_name]
        
        start_of_day = datetime.combine(datetime.now().date(), time.min)
        new_resource = Resource(name=resource_name, rts=start_of_day, wts=start_of_day)
        self.resourceList[resource_name] = new_resource
        return new_resource

    def getTransactionList(self):
        return self.transactionList
    def getResourceList(self):
        return self.resourceList
    def getTransactionWaitingList(self):
        return self.transactionWaitingList
    
    def setTransactionList(self, txList: {}):
        self.transactionList = txList
    def setResourceList(self, resList: {}):
        self.resourceList = resList
    def setTransactionWaitingList(self, transactionWaitingList: {}):
        self.transactionWaitingList = transactionWaitingList 

    def addTransaction(self, tx: Transaction):
        """Add a transaction to the transactionList by txID."""
        self.transactionList[tx.txID] = tx

    def removeTransaction(self, tx: Transaction):
        """Remove a transaction from the transactionList by txID."""
        if tx.txID in self.transactionList:
            del self.transactionList[tx.txID]
            print(f"Transaction {tx.txID} removed from transaction waiting list")

    def addWaitingTransaction(self, tx: Transaction, id_transaction_blocker: int = None):
        """Add a transaction to the transactionWaitingList by txID."""
        self.transactionWaitingList[tx.txID] = (tx, id_transaction_blocker)

    def removeWaitingTransaction(self, tx: Transaction):
        """Remove a transaction from the transactionWaitingList by txID."""
        if tx.txID in self.transactionWaitingList:
            del self.transactionWaitingList[tx.txID]
    
    def getTransactionByID(self, txID: int):
        """Retrieve a transaction by txID."""
        return self.transactionList.get(txID, None)

    def getWaitingTransaction(self, txID: int):
        """Retrieve a waiting transaction by txID."""
        entry = self.transactionWaitingList.get(txID)
        if entry:
            return entry[0]  # Return the Transaction object
        return None

    def checkWaitingTransactionBlocker(self, txID: int) -> bool:
        """Retrieve the id_transaction_blocker for a waiting transaction by txID."""
        entry = self.transactionWaitingList.get(txID)
        if entry:
            blockerTransaction = self.getTransactionByID(entry[1])
            return blockerTransaction.getTransactionStatus() == TransactionStatus.COMMITTED
        return False
