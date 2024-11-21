from models.Operation import Operation
from models.Resource import Resource
from models.Transaction import Transaction

class Schedule:
    transactionList: list[Transaction] #daftar transaksi
    resourceList: list[Resource] #daftar resource
    operationQueue: list[Operation] #daftar operasi

    def __init__(self, operationList: list, dataItemList: list, transactionList: list):
        self.operationList = operationList
        self.dataItemList = dataItemList
        self.transactionList = transactionList
    #Method Getter
    def getTransactionList(self):
        return self.transactionList
    def getResourceList(self):
        return self.resourceList
    def getOperationQueue(self):
        return self.operationQueue
    
    #Method setter
    def setTransactionList(self,txList:list[Transaction]):
        self.transactionList = txList
    def setResourceList(self,resList:list[Resource]):
        self.resourceList = resList
    def setOperationQueue(self,opQueue:list[Operation]):
        self.operationQueue = opQueue 

    #method tambahan
    def addTransaction(self,tx:Transaction):
        self.transactionList.append(tx)
    def addResource(self,res:Resource):
        self.resourceList.append(res)
    def enqueueOperation(self,op:Operation):
        self.operationQueue.append(op)
    def dequeueOperation(self) -> Operation:
        return self.operationList.pop(0)