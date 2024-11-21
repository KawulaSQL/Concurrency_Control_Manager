from datetime import datetime

class Resource:
    #atribut resource
    __data:str = None #isi data dari resource itu sendiri
    __rts:datetime = None #read timestamp
    __wts:datetime = None # write timestamp

    def __init__(self, data:str, rts:datetime, wts:datetime): #konstruktor untuk resource
        self.__data = data
        self.__rts = rts
        self.__wts = wts
        self.versions = [] #versi dari resource

    def getData(self): #method untuk mendapatkan nilai data dari resource
        return self.__data
    
    def getRTS(self): #method untuk mendapatkan nilai read timestamp
        return self.__rts
    
    def getWTS(self): #method untuk mendapatkan nilai write timestamp
        return self.__wts
    
    def setData(self,data:str): # set nilai data dari resource
        self.__data=data

    def setRTS(self,rts:datetime): # set read timestamp
        self.__rts=rts

    def setWTS(self,wts:datetime): #set write timestamp
        self.__wts=wts

    def add_version(self, value: str, wts: datetime):
        """Tambahkan versi baru ke resource."""
        self.versions.append({
            "value": value,
            "wts": wts,
            "rts": datetime.min  # RTS diinisialisasi ke datetime.min
        })