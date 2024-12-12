from datetime import datetime
# from Transaction import Transaction
from models.CCManagerEnums import LockType, LockStatus

class Resource:
    def __init__(self, name: str, rts: datetime = None, wts: datetime = None):
        """
        Constructor for the Resource class.
        
        :param name: Name of the resource.
        :param rts: Read timestamp (default is current time).
        :param wts: Write timestamp (default is current time).
        """
        self.__name: str = name
        self.__rts: datetime = rts if rts is not None else datetime.now()
        self.__wts: datetime = wts if wts is not None else datetime.now()
        self.__lockHolderList = [] #tuple of (transaction_id, LockType, LockStatus)
        print(f"Resource created with RTS: {self.__rts}, WTS: {self.__wts}")

    def __eq__(self, other):
        """
        Check equality based on resource name.
        """
        if isinstance(other, Resource):
            return self.__name == other.__name
        return False

    def __hash__(self):
        """
        Generate hash based on resource name.
        """
        return hash(self.__name)

    def __repr__(self):
        """
        Represent the Resource for debugging purposes.
        """
        return f"Resource(name={self.__name})"


    def getRTS(self): #method untuk mendapatkan nilai read timestamp
        return self.__rts
    def getWTS(self): #method untuk mendapatkan nilai write timestamp
        return self.__wts
    def getName(self): #method untuk mendapatkan nama dari resource
        return self.__name
    def getLockHolderList(self): #method untuk mendapatkan list dari lockHolder
        return self.__lockHolderList
    # def getLockHolderIdx(self,idx): #method untuk mendapatkan lockholder ke-(idx-1)
    #     return self.__lockHolderList[idx]
    
    def setRTS(self,rts:datetime): # set read timestamp
        self.__rts=rts
    def setWTS(self,wts:datetime): #set write timestamp
        self.__wts=wts
    def setName(self,name:str): #set nama variabel resource
        self.__name = name