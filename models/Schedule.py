from models.Operation import Operation
from models.Resource import Resource
from models.Transaction import Transaction

class Schedule:
    transactionList: list[Transaction] #daftar transaksi
    resourceList: list[Resource] #daftar resource
    operationQueue: list[Operation] #daftar antrean dari operasi
    operationSequence: list[Operation]

    def __init__(self, operationList: list, resourceList: list, transactionList: list):
        self.operationList = operationList
        self.resourceList = resourceList
        self.transactionList = transactionList

    def dequeue(self) -> Operation:
        return self.operationList.pop(0)
    
    def enqueue(self, op: Operation):
        self.operationQueue.append(op)