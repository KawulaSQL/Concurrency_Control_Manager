from models.Operation import Operation
from models.CCManagerEnums import TransactionStatus
from datetime import datetime
from typing import List 

class Transaction:
   TransactionCount: int = 0 
   txID: int = None 
   txStatus: TransactionStatus = None 
   operationList: List[Operation] = [] 
   txTimestamp: datetime = None 

   def __init__(self):
      """
      Constructor for the Transaction class.
      Initializes txID, txTimestamp, and operationList.
      """
      Transaction.TransactionCount += 1
      self.txID = Transaction.TransactionCount
      self.txTimestamp = self.txTimestamp or datetime.now()  # Set to current time if not provided
      self.operationList = self.operationList or [] 

   def __eq__(self, other):
      """
      Check equality based on transaction ID.
      """
      if isinstance(other, Transaction):
            return self.txID == other.getTransactionID
      return False

   def __hash__(self):
      """
      Generate hash based on transaction ID.
      """
      return hash(self.txID)

   #METHOD GETTER
   def getTransactionID(self):
      return self.txID
   def getTransactionStatus(self):
      return self.txStatus
   def getOperationList(self):
      return self.operationList
   def getTimestamp(self):
      return self.txTimestamp

   #METHOD SETTER
   def setTransactionID(self,ID:int):
      self.txID = ID
   def setTransactionStatus(self, status:TransactionStatus):
      self.txStatus = status
   def setOperationList(self, ol: {}):
      self.operationList = ol
   def setTimestamp(self,newTS:datetime):
      self.txTimestamp = newTS

   def addOperation(self,op:Operation):
      if op not in self.operationList:
         op.setOperationID(len(self.operationList) + 1)
         self.operationList.append(op)

   def printOperationList(self):
      for op in self.operationList:
         print(op.getOperationType(),op.getOpTransactionID(),f"({op.getOperationResource()})")