from models.Resource import *

#kelas untuk menentukan jenis operasi yang ada di dalam suatu transaction
class Operation:
    '''
        CATATAN:
        DAFTAR TIPE OPERASI YANG VALID:
        R = READ
        W = WRITE
        C = COMMIT
        A = ABORT
        SL = SHARED LOCK
        XL = EXCLUSIVE LOCK
        UL = UNLOCK
    '''
    __opType:str #tipe operasi

    __opTx:str #transaksi dari operasi
    __opResource:Resource #resource dari operasi

    def __init__(self, typeOp:str, tx:str, res:Resource): #konstruktor Operation
        self.__opType=typeOp
        self.__opTx=tx
        self.__opResource=res

    def getOperationType(self): #dapatkan operation type
        return self.__opType
    
    def getOperationTransaction(self): #dapatkan transaksi dari operasi
        return self.__opTx
    
    def getOperationResource(self): #dapatkan resource dari operasi
        return self.__opResource
    
    def setOperationType(self,typeOp:str):
        self.__opType = typeOp

    def setOperationTransaction(self,tx:str):
        self.__opTx = tx
    
    def setOperationResource(self,res:Resource):
        self.__opResource = res
    