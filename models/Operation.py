from Resource import Resource
from CCManagerEnums import OperationType,OperationStatus
#kelas untuk menentukan jenis operasi yang ada di dalam suatu transaction
class Operation:
    '''
        CATATAN:
        TIPE OPERASI DAN STATUS OPERASI YANG VALID LIHAT DI SOURCE CODE models/CCManagerEnums
    '''
    transactionID: int
    __opType:OperationType #tipe operasi
    __opStatus:OperationStatus #status dari operasi
    __opTx:str #transaksi dari operasi
    __opResource:list[Resource] #resource dari operasi

    def __init__(self, typeOp:OperationType, tx:str, res:Resource, transactionID: int): #konstruktor Operation
        self.__opType=typeOp
        self.__opTx=tx
        self.__opResource=res
        self.__opStatus = OperationStatus.NE
        self.transactionID = transactionID

    def getOpTransactionID(self):
        return self.transactionID

    def getOperationType(self): #dapatkan tipe operasi
        return self.__opType
    
    def getOperationTransaction(self): #dapatkan transaksi dari operasi
        return self.__opTx
    
    def getOperationResource(self): #dapatkan resource dari operasi
        return self.__opResource
    
    def getOperationStatus(self): #dapatkan status dari operasi
        return self.__opStatus
    
    def setOperationType(self,typeOp:OperationType): #set tipe operasi
        self.__opType = typeOp

    def setOpTransactionID(self, txID: int):
        self.transactionID = txID

    def setOperationTransaction(self,tx:str): #set transaction operasi
        self.__opTx = tx
    
    def setOperationResource(self,res:Resource): #set resource dari operasi
        self.__opResource = res

    def setOperationStatus(self,status:OperationStatus): #set status dari operasi
        self.__opStatus = status
    