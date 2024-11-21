from models.Operation import Operation
from models.Resource import Resource
from models.Transaction import Transaction

class Schedule:
    transactionList: list[Transaction] # daftar transaksi
    resourceList: list[Resource] # daftar resource
    operationQueue: list[Operation] # daftar antrean dari operasi yang mau dijalankan
    operationWaitingList: list[Operation] # daftar operasi yang dihentikan sementara

    def __init__(self, opQueue: list[Operation], resList: list[Resource], txList: list[Transaction]):
        self.operationQueue = opQueue
        self.resourceList = resList
        self.transactionList = txList
        self.operationWaitingList = []

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