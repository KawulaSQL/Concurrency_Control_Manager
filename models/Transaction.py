from models.Operation import Operation
from models.Resource import Resource
from models.CCManagerEnums import TransactionStatus
from datetime import datetime
class Transaction:
      txID: int = None #id transaction
      txStatus: TransactionStatus = None #status transaction
      operationList: list[Operation] = None #daftar operasi
      sharedLockList: list[Resource] = None #daftar shared lock
      exclusiveLockList: list[Resource] = None #daftar exclusive lock
      startTS:datetime = None #start Timestamp
      valTS:datetime = None #validation Timestamp
      finishTS:datetime = None #finish Timestamp

      def __init__(self,txid: int, sl: list[Resource] = None, xl : list[Resource] = None, ol: list[Operation] = None):
         #THIS SHOULD BE ADJUSTED TO begin_transaction function, jdi nanti dia ngirimnya apa
         self.txID = txid
         self.exclusiveLockList = xl
         self.sharedLockList = sl
         self.startTS = datetime.max
         self.finishTS = datetime.max
         self.valTS = datetime.max
         self.operationList = ol

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
      def getStartTS(self):
         return self.startTS
      def getFinishTS(self):
         return self.finishTS
      def getValidationTS(self):
         return self.valTS

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
      def setStartTS(self,newTS:datetime):
         self.startTS = newTS
      def setFinishTS(self,newTS:datetime):
         self.finishTS = newTS
      def setValidationTS(self,newTS:datetime):
         self.valTS = newTS

      def addOperation(self,op:Operation): #tambah operation ke operation list
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
