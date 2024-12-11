from Resource import Resource
from CCManagerEnums import OperationType,OperationStatus
#kelas untuk menentukan jenis operasi yang ada di dalam suatu transaction
class Operation:
    transactionID: int #Operation Transaction ID
    operationID: int  # Operation ID, automatically assigned when added to a transaction
    __opType:OperationType #Operation Type: R/W/C/A
    __opResource:str #Resource of Operation / Object Row Name

    def __init__(self, transactionID: int, typeOp:OperationType, res:str):
        self.transactionID = transactionID
        self.operationID = None
        self.__opType=typeOp
        self.__opResource=res

    def __eq__(self, other):
        if isinstance(other, Operation):
            return self.transactionID == other.transactionID and self.operationID == other.operationID
        return False

    def __hash__(self):
        return hash((self.transactionID, self.operationID))

    def getOpTransactionID(self):
        return self.transactionID

    def getOperationID(self):
        return self.operationID

    def getOperationResource(self): #dapatkan resource dari operasi
        return self.__opResource

    def getOperationType(self): #dapatkan tipe operasi
        return self.__opType
    
    def setOpTransactionID(self, txID: int):
        self.transactionID = txID

    def setOperationID(self, opID: int):
        self.operationID = opID

    def setOperationResource(self,res:str): #set resource dari operasi
        self.__opResource = res 

    def setOperationType(self,typeOp:OperationType): #set tipe operasi
        self.__opType = typeOp

    def printOperation(self):
        print(self.getOperationType(),self.getOpTransactionID(),f"{self.getOperationResource()}")