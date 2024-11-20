from models.Operation import Operation
from models.Resource import Resource
from datetime import datetime
class Transaction:
    txID: str = None #id transaction
    sharedLockList: list[str] = None #daftar shared lock
    exclusiveLockList: list[str] = None #daftar exclusive lock

    readSet:list[Resource] = None #daftar operasi read di transaction
    writeSet:list[Resource] =None #daftar operasi write di transaction

    startTS:datetime = None #start Timestamp
    valTS:datetime = None #validation Timestamp
    finishTS:datetime = None #finish Timestamp
    def __init__(self,txid: str, sl: list[str], xl : list[str]):
        self.txID = txid
        self.exclusiveLockList = xl
        self.sharedLockList = sl
        self.readSet = []
        self.writeSet = []
        self.startTS = datetime.max
        self.finishTS = datetime.max
        self.valTS = datetime.max

    def addToSet(self, operation: Operation): #tambahkan operation ke set
        if operation.getOperationType() == "R":
          self.addToReadSet(operation.getOperationResource())
        elif operation.getOperationType() == "W":
          self.addToWriteSet(operation.getOperationResource())

    def addToWriteSet(self,resource:Resource): #masukkan resource write ke write set
       if(resource not in self.writeSet):
          self.writeSet.append(resource)

    def addToReadSet(self,resource:Resource): #masukkan resource read ke read set
       if(resource not in self.readSet):
          self.readSet.append(resource)

    def getReadSet(self): #get read set
       return self.readSet
    
    def getWriteSet(self): #get write set
       return self.writeSet
    
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
       self.readSet=[]
       self.writeSet=[]
