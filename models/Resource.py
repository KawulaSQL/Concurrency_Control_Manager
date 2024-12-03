from datetime import datetime
class Resource:
    #atribut resource
    __name:str = None #nama variabel dari resource
    __rts:datetime = None #read timestamp
    __wts:datetime = None # write timestamp
    versions:list[int] = None #versi dari resource (MVCC)
    __data:str = None #data

    def __init__(self, name: str, rts: datetime = None, wts: datetime = None):
        """
        Constructor for the Resource class.
        
        :param name: Name of the resource.
        :param rts: Read timestamp (default is current time).
        :param wts: Write timestamp (default is current time).
        """
        self.__name = name
        self.__rts = rts if rts is not None else datetime.now() 
        self.__wts = wts if wts is not None else datetime.now()  
        self.versions = []  
        self.__lockHolderList = []  

    def getData(self): #method untuk mendapatkan nilai data dari resource
        return self.__data
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
    
    def add_version(self, value: str, wts: datetime):
        """Tambahkan versi baru ke resource."""
        self.versions.append({
            "value": value,
            "wts": wts,
            "rts": datetime.min  # RTS diinisialisasi ke datetime.min
        })