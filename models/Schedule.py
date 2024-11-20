from models.Operation import Operation
from models.Resource import Resource
from models.Transaction import Transaction

class Schedule:
    transactionList: list[Transaction] #daftar transaksi
    resourceList: list[Resource] #daftar resource
    operationList: list[Operation] #daftar operasi

    def __init__(self, operationList: list, dataItemList: list, transactionList: list):
        self.operationList = operationList
        self.dataItemList = dataItemList
        self.transactionList = transactionList

    def dequeue(self) -> Operation:
        return self.operationList.pop(0)