from ConcurrencyControlManager import ConcurrencyControlManager,Operation
from models.CCManagerEnums import OperationType
def main():
    ccm = ConcurrencyControlManager(controller="2PL")

    #TEST 1: R1(X); W2(Y); W1(Y); R1(X); W1(X); C1; C2;
    ccm.begin_transaction() 
    ccm.begin_transaction()
    print("Banyak transaksi di CCM: ",len(ccm.schedule.getTransactionList())) #Banyak transaksi di CCM: 2

    operation1 = Operation(1,OperationType.R,"X")
    print(ccm.validate_object(operation1)) #seharusnya shared lock untuk resource X granted di transaction 1
    ccm.log_object(operation1) #daftar yang benar: shared lock: X :[1], exclusive lock: kosong
    operation2 = Operation(2,OperationType.W,"Y")
    print(ccm.validate_object(operation2)) #seharusnya exclusive lock untuk resource Y granted di transaction 2
    ccm.log_object(operation2) #daftar yang benar: shared lock: X :[1], exclusive lock: Y : 2
    operation3 = Operation(1,OperationType.W,"Y")
    print(ccm.validate_object(operation3)) #seharusnya exclusive lock untuk resource Y GAGAL granted di transaction 1
    ccm.log_object(operation3) #daftar yang benar: shared lock: X :[1], exclusive lock: Y : 2

main()