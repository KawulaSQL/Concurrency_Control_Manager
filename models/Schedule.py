from models.Operation import Operation
from models.Resource import Resource
from models.Transaction import Transaction
from models.CCManagerEnums import LockType
class Schedule:
    _instance = None

    def __new__(cls):
        """
        Ensure only one instance of Schedule exists.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # Dictionary: {txID: Transaction object} for fast lookup by txID
            cls._instance.resourceList = {}  # Dictionary: {resource_name: Resource object}
            cls._instance.transactionList = {}  # Dictionary: {txID: Transaction object}
            cls._instance.transactionWaitingList = {}  # Dictionary: {txID: Transaction object} for waiting transactions
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
        
        new_resource = Resource(name=resource_name)
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

    def addWaitingTransaction(self, tx: Transaction):
        """Add a transaction to the transactionWaitingList by txID."""
        self.transactionWaitingList[tx.txID] = tx

    def removeWaitingTransaction(self, tx: Transaction):
        """Remove a transaction from the transactionWaitingList by txID."""
        if tx.txID in self.transactionWaitingList:
            del self.transactionWaitingList[tx.txID]
    
    def getTransactionByID(self, txID: int):
        """Retrieve a transaction by txID."""
        return self.transactionList.get(txID, None)

    def getWaitingTransaction(self, txID: int):
        """Retrieve a waiting transaction by txID."""
        return self.transactionWaitingList.get(txID, None)

    def addResource(self, res: Resource):
        self.resourceList[res.name] = res