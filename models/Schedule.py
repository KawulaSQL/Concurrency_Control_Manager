from Operation import Operation
from Resource import Resource
from Transaction import Transaction
from CCManagerEnums import LockType
class Schedule:
    transactionList: list[Transaction] # daftar transaksi
    resourceList: set[Resource] # set of resource, behave like lock table
    transactionWaitingList: list[Transaction]
    # operationQueue: list[Operation] # daftar antrean dari operasi yang mau dijalankan
    # __lockHolderList:list[{Transaction,LockType}] = None #daftar pemegang lock dan transactionnya
    # Class variable to store the single instance of Schedule
    _instance = None

    def __new__(cls, opQueue: list[Operation], resList: list[Resource], txList: list[Transaction]):
        """Ensure only one instance of Schedule exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.operationQueue = opQueue
            cls._instance.resourceList = resList
            cls._instance.transactionList = txList
            cls._instance.operationWaitingList = []
        return cls._instance

    def __init__(self, opQueue: list[Operation], resList: list[Resource], txList: list[Transaction]):
        """Initialize the Schedule if it's the first time being created."""
        # The __init__ method will only run when the instance is created for the first time.
        pass

    # Method getter
    def getTransactionList(self):
        return self.transactionList
    def getResourceList(self):
        return self.resourceList
    def getOperationQueue(self):
        return self.operationQueue
    def getOperationWaitingList(self):
        return self.operationWaitingList
    
    # Method setter
    def setTransactionList(self, txList: list[Transaction]):
        self.transactionList = txList
    def setResourceList(self, resList: list[Resource]):
        self.resourceList = resList
    def setOperationQueue(self, opQueue: list[Operation]):
        self.operationQueue = opQueue 
    def setOperationWaitingList(self, opWaitingList: list[Operation]):
        self.operationQueue = opWaitingList 

    # Method tambahan
    def addTransaction(self, tx: Transaction):
        self.transactionList.append(tx)
    def addResource(self, res: Resource):
        self.resourceList.append(res)
    def enqueueWaiting(self, op: Operation):
        self.operationWaitingList.append(op)
    def dequeueWaiting(self) -> Operation:
        return self.operationWaitingList.pop(0)
    def enqueue(self, op: Operation):
        self.operationQueue.append(op)
    def dequeue(self) -> Operation:
        return self.operationList.pop(0)
    
    def setLockHolderList(self,lhList:list[{Transaction,LockType}]): #set lock holder list
        self.__lockHolderList=lhList

    #Method Add dan Delete
    def addLockHolder(self,transaction:Transaction,locktype:LockType): #tambah lock holder baru
        self.__lockHolderList.append({transaction,locktype})
    def deleteLockHolder(self,transaction:Transaction,locktype:LockType = None):
        '''
            hapus berdasarkan transaction dan locktype
            jika locktype gak ada, hapus berdasarkan transaction aja
        '''
        if locktype: #hapus semua lock holder berdasarkan transaction dan locktype
            self.__lockHolderList = [
                lock for lock in self.__lockHolderList 
                if not (lock['Transaction'] == transaction and lock['LockType'] == locktype)
            ]
        else: #hapus semua lock holder berdasarkan transaction
            self.__lockHolderList = [
                lock for lock in self.__lockHolderList 
                if lock['Transaction'] != transaction
            ]