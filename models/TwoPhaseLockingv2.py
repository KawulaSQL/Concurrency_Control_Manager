from abc import ABC
from collections import defaultdict
from ControllerMethod import ControllerMethod
from Schedule import Schedule,Transaction,Operation,Resource
class TwoPhaseLocking():
    def __init__(self, input_sequence: str):
        self.sequence = []
        self.timestamp = []
        self.schedule = Schedule([],[],[])
        self.transactionIDSet = []
        self.exclusive_lock_table = {}
        self.shared_lock_table = defaultdict(list)
     # Parse input sequence
        try:
            input_sequence = input_sequence.rstrip(";").split(";")
            for entry in input_sequence:
                entry = entry.strip()
                if entry[0] in {'R', 'W'}:
                    #buat transaction baru jika belum pernah dibuat
                    newTx = None
                    if(int(entry[1]) not in self.transactionIDSet):
                        newTx = Transaction(int(entry[1]))
                        newTx.setValidationTS(int(entry[1])) #timestamp dari transaction
                        self.schedule.addTransaction(newTx)
                        self.transactionIDSet.append(int(entry[1]))
                    
                    newRes = Resource(entry[3])
                    newOp = Operation(entry[0],"",newRes,int(entry[1]))

                    tx = self.schedule.getTransactionById(int(entry[1]))
                    tx.addOperation(newOp)
                    
                    self.sequence.append({
                        "operation": entry[0],
                        "transaction": int(entry[1]),
                        "table": entry[3]
                    })
                    if int(entry[1]) not in self.timestamp:
                        self.timestamp.append(int(entry[1]))
                elif entry[0] == 'C':
                    newOp = Operation(entry[0],"",None,int(entry[1]))
                    tx = self.schedule.getTransactionById(int(entry[1]))
                    tx.addOperation(newOp)
                    self.sequence.append({"operation": entry[0], "transaction": int(entry[1])})
                else:
                    raise ValueError("Invalid operation detected.")
                
            # Ensure each transaction has a commit
            if len([x for x in self.sequence if x["operation"] == 'C']) != len(set(self.timestamp)):
                raise ValueError("Each transaction must have a commit operation.")
            # print(self.sequence)

            transactionList = self.schedule.getTransactionList()

            for transaction in transactionList:
                print("transaction id: ", transaction.getTransactionID())
                transaction.printOperationList()
        except ValueError as e:
            raise ValueError(f"Invalid input sequence: {e}")
        
    def shared_lock(self, operation:Operation ) -> bool:
        """Acquire a shared lock."""
        transactionList = self.schedule.getTransactionList()

        for transaction in transactionList:
            exclusiveLockList = transaction.getExclusiveLockList()
            sharedLockList = transaction.getSharedLockList()

            transactionID = transaction.getTransactionID()


            #periksa apakah exclusive lock valid

            if(resource in exclusiveLockList):
                if(table.get)


    def exclusive_lock(self, transaction: int, table: str) -> bool:
        """Acquire an exclusive lock."""
        if table in self.shared_lock_table:
            if transaction in self.shared_lock_table[table] and len(self.shared_lock_table[table]) == 1:
                self.shared_lock_table[table].remove(transaction)
                self.exclusive_lock_table[table] = transaction
                self.log_transaction(transaction, table, "UPL", "Success")
                return True
            return False
        if table in self.exclusive_lock_table:
            return self.exclusive_lock_table[table] == transaction
        self.exclusive_lock_table[table] = transaction
        self.log_transaction(transaction, table, "XL", "Success")
        return True
if __name__ == "__main__":
    print("1. Enter sequence")
    print("2. File Input")
    input_choice = input("Enter your option (1 or 2): ")
    
    if (input_choice == 1):
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
        tpl = TwoPhaseLocking(input_seq)
    except Exception as e:
        print(f"Error: {e}")