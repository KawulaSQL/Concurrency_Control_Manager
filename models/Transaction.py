from Operation import Operation
from CCManagerEnums import TransactionStatus
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
            return self.tx_id == other.tx_id
      return False

   def __hash__(self):
      """
      Generate hash based on transaction ID.
      """
      return hash(self.tx_id)

   #METHOD GETTER
   def getTransactionID(self):
      return self.txID
   def getTransactionStatus(self):
      return self.txStatus
   def getOperationList(self):
      return self.operationList
   def getSharedLockList(self):
      return self.sharedLockList
   def getExclusiveLockList(self):
      return self.exclusiveLockList
   def getTimestamp(self):
      return self.txTimestamp

   #METHOD SETTER
   def setTransactionID(self,ID:int):
      self.txID = ID
   def setTransactionStatus(self, status:TransactionStatus):
      self.txStatus = status
   def setOperationList(self, ol: list[Operation] = None):
      self.operationList = ol
   def setSharedLockList(self, sl: list[Resource] = None):
      self.sharedLockList = sl
   def setExclusiveLockList(self, xl: list[Resource] = None):
      self.exclusiveLockList = xl
   def setTimestamp(self,newTS:datetime):
      self.txTimestamp = newTS

   def addOperation(self,op:Operation):
      if op not in self.operationList:
         op.setOperationID(len(self.operationList) + 1)
         self.operationList.append(op)

   def addSharedLock(self,sl:Resource): #tambah resource shared lockk ke list
      self.sharedLockList.append(sl)

   def addExclusiveLock(self,sl:Resource): #tambah resource exclusive lock ke list
      self.exclusiveLockList.append(sl)

   def markStartTS(self): #update start timestamp
      self.startTS = datetime.now()

   def markFinishTS(self): #update finish timestamp
      self.finishTS = datetime.now()

   def markValidationTS(self): #update validation timestamp
      self.valTS = datetime.now()

   def abort(self): #abort transaction
      self.startTS = datetime.max
      self.finishTS = datetime.max
      self.valTS = datetime.max
   #  self.readSet=[]
   #  self.writeSet=[]
