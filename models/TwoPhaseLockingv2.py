from abc import ABC
from collections import defaultdict
from ControllerMethod import ControllerMethod
from Schedule import Schedule,Transaction,Operation,Resource
class TwoPhaseLockingv2():
    def __init__(self, input_sequence: str):
        self.sequence = []
        self.timestamp = []
        self.schedule = Schedule()
        self.transactionIDSet = []
        self.exclusive_lock_table = defaultdict()
        self.shared_lock_table = defaultdict(list)
        self.transaction_history = []
        self.result = []
     # Parse input sequence
        try:
            input_sequence = input_sequence.rstrip(";").split(";")
            for entry in input_sequence:
                entry = entry.strip()
                if entry[0] in {'R', 'W'}:
                    #buat transaction baru jika belum pernah dibuat
                    newTx = None
                    if(int(entry[1]) not in self.transactionIDSet):
                        newTx = Transaction()
                        self.schedule.addTransaction(newTx)
                        self.transactionIDSet.append(int(entry[1]))
                        # txList = self.schedule.getTransactionList()
                        # # for transaction in txList:
                        # #     print(transaction.getTransactionID())
                    newOp = Operation(int(entry[1]),entry[0],entry[3])
                    tx = self.schedule.getTransactionByID(int(entry[1]))

                
                    tx.addOperation(newOp)
                    
                    self.sequence.append({
                        "operation": newOp,
                        "transaction": tx,
                        "resource": entry[3]
                    })
                    if int(entry[1]) not in self.timestamp:
                        self.timestamp.append(int(entry[1]))
                elif entry[0] == 'C':
                    newOp = Operation(int(entry[1]),entry[0],"")
                    tx = self.schedule.getTransactionByID(int(entry[1]))
                    tx.addOperation(newOp)
                    self.sequence.append({"operation": newOp, "transaction": tx})
                else:
                    raise ValueError("Invalid operation detected.")
                
            # Ensure each transaction has a commit
            if len([x for x in self.sequence if x["operation"].getOperationType() == 'C']) != len(set(self.timestamp)):
                raise ValueError("Each transaction must have a commit operation.")
            # print(self.sequence)

            # transactionList = self.schedule.getTransactionList()

            # # for txID  in transactionList:
            # #     print("transaction id: ", txID)
            # #     transactionList[txID].printOperationList()
                
                
        except ValueError as e:
            raise ValueError(f"Invalid input sequence: {e}")
        
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
    
    def testSL(self):
        current = self.sequence.pop(0)
        if(current["operation"].getOperationType() == "R" and (self.shared_lock(current["transaction"],current["resource"]))):
            self.result.append(current)

    def testXL(self):
        current = self.sequence.pop(0)
        if(current["operation"].getOperationType() == "W" and (self.exclusive_lock(current["transaction"],current["resource"]))):
            self.result.append(current)


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