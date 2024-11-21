from datetime import datetime
from models.Transaction import Transaction
from models.CCManagerEnums import LockType
class Resource:
    #atribut resource
    __name:str = None #nama variabel dari resource
    __rts:datetime = None #read timestamp
    __wts:datetime = None # write timestamp
    versions:list[int] = None #versi dari resource (MVCC)
    __lockHolderList:list[{Transaction,LockType}] = None #daftar pemegang lock dan transactionnya
    def __init__(self, name:str, rts:datetime, wts:datetime): #konstruktor untuk resource
        self.__rts = rts
        self.__wts = wts
        self.__name=name
        self.versions = []
        self.__lockHolderList = []
    #METHOD GETTER
    def getRTS(self): #method untuk mendapatkan nilai read timestamp
        return self.__rts
    def getWTS(self): #method untuk mendapatkan nilai write timestamp
        return self.__wts
    def getName(self): #method untuk mendapatkan nama dari resource
        return self.__name
    def getVersion(self): #method untuk mendapatkan versi dari resource
        return self.__version
    def getLockHolderList(self): #method untuk mendapatkan list dari lockHolder
        return self.__lockHolderList
    def getLockHolderIdx(self,idx): #method untuk mendapatkan lockholder ke-(idx-1)
        return self.__lockHolderList[idx]
    
    #METHOD SETTER
    def setRTS(self,rts:datetime): # set read timestamp
        self.__rts=rts
    def setWTS(self,wts:datetime): #set write timestamp
        self.__wts=wts
    def setName(self,name:str): #set nama variabel resource
        self.__name = name
    def setVersion(self,version:int): #set versi resource
        self.__version=version
    def setLockHolderList(self,lhList:list[{Transaction,LockType}]): #set lock holder list
        self.__lockHolderList=lhList

    #Method Add dan Delete
    def addLockHolder(self,transaction:Transaction,locktype:LockType): #tambah lock holder baru
        self.__lockHolderList.append({transaction,locktype})
    def deleteLockHolder(self,transaction:Transaction,locktype:LockType = None):
        '''
            hapus berdasarkan transaction dan locktype
            jika locktype gak ada, hapus berdasarkan transaction aja
        '''
        if locktype: #hapus semua lock holder berdasarkan transaction dan locktype
            self.__lockHolderList = [
                lock for lock in self.__lockHolderList 
                if not (lock['Transaction'] == transaction and lock['LockType'] == locktype)
            ]
        else: #hapus semua lock holder berdasarkan transaction
            self.__lockHolderList = [
                lock for lock in self.__lockHolderList 
                if lock['Transaction'] != transaction
            ]
    def add_version(self, value: str, wts: datetime):
        """Tambahkan versi baru ke resource."""
        self.versions.append({
            "value": value,
            "wts": wts,
            "rts": datetime.min  # RTS diinisialisasi ke datetime.min
        })