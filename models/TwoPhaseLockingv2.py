from abc import ABC
from collections import defaultdict
from models.ControllerMethod import ControllerMethod
from models.Schedule import Schedule,Transaction,Operation,Resource
from models.CCManagerEnums import ResponseType, OperationType, TransactionStatus
from models.Response import Response
class TwoPhaseLockingv2(ControllerMethod):
    def __init__(self):
        self.sequence = []
        self.timestamp = []
        self.schedule = Schedule()
        self.transactionIDSet = []
        self.exclusive_lock_table = defaultdict()
        self.shared_lock_table = defaultdict(list)
        self.transaction_history = []
        self.result = []
        self.wait_sequence = [] #semua sequence yang masuk ke wait
     # Parse input sequence
        # try:
        #     input_sequence = input_sequence.rstrip(";").split(";")
        #     for entry in input_sequence:
        #         entry = entry.strip()
        #         if entry[0] in {'R', 'W'}:
        #             #buat transaction baru jika belum pernah dibuat
        #             newTx = None
        #             if(int(entry[1]) not in self.transactionIDSet):
        #                 newTx = Transaction()
        #                 self.schedule.addTransaction(newTx)
        #                 self.transactionIDSet.append(int(entry[1]))
        #                 # txList = self.schedule.getTransactionList()
        #                 # # for transaction in txList:
        #                 # #     print(transaction.getTransactionID())
        #             newOp = Operation(int(entry[1]),entry[0],entry[3])
        #             tx = self.schedule.getTransactionByID(int(entry[1]))

                
        #             tx.addOperation(newOp)
                    
        #             self.sequence.append({
        #                 "operation": newOp,
        #                 "transaction": tx,
        #                 "resource": entry[3]
        #             })
        #             if int(entry[1]) not in self.timestamp:
        #                 self.timestamp.append(int(entry[1]))
        #         elif entry[0] == 'C':
        #             newOp = Operation(int(entry[1]),entry[0],"")
        #             tx = self.schedule.getTransactionByID(int(entry[1]))
        #             tx.addOperation(newOp)
        #             self.sequence.append({"operation": newOp, "transaction": tx})
        #         else:
        #             raise ValueError("Invalid operation detected.")
                
        #     # Ensure each transaction has a commit
        #     if len([x for x in self.sequence if x["operation"].getOperationType() == 'C']) != len(set(self.timestamp)):
        #         raise ValueError("Each transaction must have a commit operation.")
            # print(self.sequence)

            # transactionList = self.schedule.getTransactionList()

            # # for txID  in transactionList:
            # #     print("transaction id: ", txID)
            # #     transactionList[txID].printOperationList()
                
                
        # except ValueError as e:
        #     raise ValueError(f"Invalid input sequence: {e}")
        
    def log_transaction(self, transaction, table, operation, status):
        """Log transaction operations."""
        self.transaction_history.append({
            "transaction": transaction, "table": table, "operation": operation, "status": status
        })
    def result_string(self) -> str:
        """Generate a result string from the operations."""
        return ";".join(
            f"{op['operation']}{op['transaction']}({op['table']})" if "table" in op else f"{op['operation']}{op['transaction']}"
            for op in self.result
        )
    def history_string(self) -> str:
        """Generate a string representation of transaction history."""
        return "\n".join(
            f"{entry['operation']} {entry['transaction']} {entry['table']} ({entry['status']})"
            for entry in self.transaction_history
        )
    def shared_lock(self, transaction: Transaction, resource: str) -> bool:
        """Acquire a shared lock."""
        if resource in self.exclusive_lock_table: #jika resource sudah dilock sama exclusive lock
            if self.exclusive_lock_table[resource] == transaction.getTransactionID(): #jika exclusive lock melock resource di transaksi yang sama
                return True  #lock granted
            return False #lock gagal granted, masuk ke skema deadlock prevention
        if transaction.getTransactionID() in self.shared_lock_table[resource]:#jika resource sudah dilock sama shared lock dimanapun posisinya, keynya juga granted
            return True 
        
        #grant shared lock baru ke resource
        self.shared_lock_table[resource].append(transaction.getTransactionID())
        self.log_transaction(transaction.getTransactionID(), resource, "SL", "Success")
        return True
    def release_locks(self, transaction:Transaction):
        #release semua resource dari transaction 

        #release shared lock semua resource yang dilock oleh transaction ke-transactionID
        for resource in self.shared_lock_table:
            if (transaction.getTransactionID() in self.shared_lock_table[resource]):
                self.shared_lock_table[resource].remove(transaction.getTransactionID())

        #release exclusive lock semua resource yang dilock oleh transaction ke-transactionID
        for resource in self.exclusive_lock_table:
            if(transaction.getTransactionID() == self.exclusive_lock_table[resource]):
                self.exclusive_lock_table.pop()

    def exclusive_lock(self, transaction: Transaction, resource: str) -> bool:
        """Acquire an exclusive lock."""
        if resource in self.shared_lock_table: #jika resource ada di shared_lock_table
            if transaction.getTransactionID() in self.shared_lock_table[resource] and len(self.shared_lock_table[resource]) == 1:
                #jika resource udah pernah di shared lock dengan transaction yang sama dan transaction itu satu satunya yang pernah shared lock
                #upgrade key dari shared lock ke exclusive lock
                self.shared_lock_table[resource].remove(transaction.getTransactionID())
                self.exclusive_lock_table[resource] = transaction.getTransactionID()
                self.log_transaction(transaction.getTransactionID(), resource, "UPL", "Success")
                return True
            return False
        if resource in self.exclusive_lock_table: # jika resource sudah pernah dilock oleh exclusive lock
            return self.exclusive_lock_table[resource] == transaction.getTransactionID() 
            #granted jika di exclusive lock oleh transaksi yang sama
            #grant gagal jika transaksi yang exclusive lock beda
        #grant exclusive lock baru ke resource
        self.exclusive_lock_table[resource] = transaction.getTransactionID()
        self.log_transaction(transaction.getTransactionID(), resource, "XL", "Success")
        return True


    def waitDie(self, transaction: Transaction, resource: str, lock_type: str) -> bool:
        holder_id = self.exclusive_lock_table.get(resource) or (self.shared_lock_table[resource][0] if self.shared_lock_table[resource] else None)
        if holder_id is None:
            return False

        holder_tx = self.schedule.getTransactionByID(holder_id)
        if self.timestamp[transaction.getTransactionID() - 1] < self.timestamp[holder_tx.getTransactionID() - 1]:
            self.log_transaction(transaction.getTransactionID(), resource, lock_type, "Wait")
            return False
        else:
            self.log_transaction(transaction.getTransactionID(), resource, lock_type, "Abort")
            self.abort_transaction(transaction)
            return False

    def abort_transaction(self, transaction: Transaction):
        transaction_id = transaction.getTransactionID()

        for resource, holder_id in self.exclusive_lock_table.items():
            if holder_id == transaction_id:
                del self.exclusive_lock_table[resource]

        for resource, holders in self.shared_lock_table.items():
            if transaction_id in holders:
                holders.remove(transaction_id)

        self.schedule.removeTransaction(transaction)
    
    def testSL(self):
        current = self.sequence.pop(0)
        if(current["operation"].getOperationType() == "R" and (self.shared_lock(current["transaction"],current["resource"]))):
            self.result.append(current)

    def testXL(self):
        current = self.sequence.pop(0)
        if(current["operation"].getOperationType() == "W" and (self.exclusive_lock(current["transaction"],current["resource"]))):
            self.result.append(current)
    def validate_object(self, operation: Operation) -> Response: 
        """
        Validating Two Phase Locking With Wait-Die scheme
        """
        print(f"Validating operation to get resource: {operation.getOperationResource()}")
        operationResource = self.schedule.get_or_create_resource(operation.getOperationResource())
        print(f"Operation resource: {operationResource.getName()}")

        transaction = self.schedule.getTransactionByID(operation.getOpTransactionID())

        print(f"Transaction retrieved ID: {transaction.getTransactionID()}")

        transaction.addOperation(operation)
        # self.sequence.append({
        #     "operation": operation,
        #     "transaction": transaction,
        #     "resource": operationResource
        # })
        
        if (operation.getOperationType() == OperationType.R): # read: minta shared lock
            if(self.shared_lock(transaction,operationResource)): #shared lock granted
                print(f"Resource {operationResource.getName()} has shared lock granted on transaction {transaction.getTransactionID()}. Transaction Allowed")
                return Response(ResponseType.ALLOWED, operation)
            else: #tidak granted, wait die
                #syarat aktivasi protokol wait die untuk read: resource dilock pake exclusive lock di transaction berbeda
                print(f"Shared lock grant for resource {operationResource} failed.")
                lockHolderID = self.exclusive_lock_table[operationResource]
                lockRequesterID = transaction.getTransactionID()

                if(lockHolderID > lockRequesterID): #lock holder idnya lebih besar (transaksi lebih muda melock resource tersebut dan lock direquest oleh transaksi yang lebih tua)
                    #aktifkan protokol WAIT
                    print(f"Older transaction {lockRequesterID} requested shared lock from younger transaction {lockHolderID}. Transaction {lockRequesterID} is now waiting.")
                    transaction.setTransactionStatus(TransactionStatus.WAITING)
                    self.wait_sequence.append(operation)
                    return Response(ResponseType.WAITING, operation)
                else: #lock holder idnya lebih kecil (transaksi lebih tua memegang lock dan lock requester lebih muda dari lock holder)
                    #aktifkan protokol DIE
                    print(f"Younger transaction {lockRequesterID} requested shared lock from older transaction {lockHolderID}. Transaction {lockRequesterID} is aborted.")
                    transaction.setTranscationStatus(TransactionStatus.ABORTED)
                    #unlock semua resource yang dipegang oleh transaction setelah abort
                    self.release_locks(transaction)
                    return Response(ResponseType.ABORT, operation)

        elif (operation.getOperationType() == OperationType.W): #write: minta exclusive lock
            if(self.exclusive_lock(transaction,operationResource)): #exclusive lock berhasil granted
                print(f"Resource {operationResource} has exclusive lock granted. Transaction allowed")
                return Response(ResponseType.ALLOWED, operation)
            else: #grant exclusive lock gagal
                #syarat aktivasi protokol wait die untuk write: resource dilock pake exclusive lock di transaction berbeda
                #atau shared lock
                print(f"Exclusive lock grant for resource {operationResource} failed.")
                lockHolderID = self.exclusive_lock_table[operationResource] if self.exclusive_lock_table[operationResource] else self.shared_lock_table[operationResource][len(self.shared_lock_table[operationResource]) - 1]
                #asumsi lockholderid adalah transaksi terakhir yang melakukan grant shared lock ke resource tersebut
                lockRequesterID = transaction.getTransactionID()

                if(lockHolderID > lockRequesterID): #lock holder idnya lebih besar (transaksi lebih muda melock resource tersebut dan lock direquest oleh transaksi yang lebih tua)
                    #aktifkan protokol WAIT
                    print(f"Older transaction {lockRequesterID} requested shared lock from younger transaction {lockHolderID}. Transaction {lockRequesterID} is now waiting.")
                    transaction.setTransactionStatus(TransactionStatus.WAITING)
                    return Response(ResponseType.WAITING, operation)
                else: #lock holder idnya lebih kecil (transaksi lebih tua memegang lock dan lock requester lebih muda dari lock holder)
                    #aktifkan protokol DIE
                    print(f"Younger transaction {lockRequesterID} requested shared lock from older transaction {lockHolderID}. Transaction {lockRequesterID} is aborted.")
                    transaction.setTranscationStatus(TransactionStatus.ABORTED)
                    #unlock semua resource yang dipegang oleh transaction setelah abort
                    self.release_locks(transaction)
                    return Response(ResponseType.ABORT, operation)

        else: #commit: unlock semua lock yang dihold oleh transaction
            transaction.setTransactionStatus(TransactionStatus.COMMITTED)
            self.release_locks(transaction)
            return Response(ResponseType.ALLOWED, operation)
        

    def log_object(self, operation: Operation): 
        print(f"Logging operation for resource: {operation.getOperationResource()}")
        print(f"Shared Lock List: ", self.shared_lock_table)
        print(f"Exclusive Lock List: ", self.exclusive_lock_table)

    def end_transaction(self, transaction_id: int):
        print(f"Ending transaction {transaction_id}.")
        
        transaction = self.schedule.getTransactionByID(transaction_id)

        if transaction.getTransactionStatus() == TransactionStatus.ABORTED:
            print(f"Transaction {transaction_id} aborted, adding to waiting list.")
            self.schedule.addWaitingTransaction(transaction)

        self.schedule.removeTransaction(transaction)
        transaction.setTransactionStatus(TransactionStatus.TERMINATED)
        print(f"Transaction {transaction_id} removed and terminated.")


#main di bawah cuma buat testing 
if __name__ == "__main__":
    print("1. Enter sequence")
    print("2. File Input")
    input_choice = input("Enter your option (1 or 2): ")
    
    if (input_choice == "1"):
        input_seq = input("Enter sequence (delimited by ';'): ")
    elif input_choice == "2":
        file_name = input("Enter file name: ")
        file_name = "./test/" + file_name
        try:
            with open(file_name, "r") as file:
                input_seq = file.read().strip()
        except FileNotFoundError:
            print("Error: File not found. Please check the file name and path.")
            exit()
    else:
        print("Invalid option. Exiting.")
        exit()

    try:
        tpl = TwoPhaseLockingv2(input_seq)
        i = 1
        for seq in tpl.sequence:
            print(f"sequence ke: {i}")
            seq["operation"].printOperation()
            print("transaction ke-",seq["transaction"].getTransactionID())
            seq["transaction"].printOperationList()
            i+=1

        tpl.testSL()
        print(tpl.transaction_history)
        tpl.testXL()
        print(tpl.transaction_history)
    except Exception as e:
        print(f"Error: {e}")