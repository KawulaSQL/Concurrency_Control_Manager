from models.Transaction import Transaction
from models.Operation import Operation
from models.CCManagerEnums import OperationType, ResponseType, TransactionStatus
from models.Schedule import Schedule
from models.TwoPhaseLocking import TwoPhaseLocking

def print_status(msg: str):
    print("\n" + "="*50)
    print(msg)
    print("="*50 + "\n")

def simulate():
    print_status("Starting Complex Scenario Simulation")
    
    # Initialize TwoPhaseLocking with Schedule
    ccm = TwoPhaseLocking(deadlock_prevention="WAIT-DIE")

    # Create transactions
    t1 = Transaction()
    t2 = Transaction()
    t3 = Transaction()

    # Add transactions to schedule
    for tx in [t1, t2, t3]:
        ccm.schedule.addTransaction(tx)

    # Define operations sequence
    operations = [
        Operation(t1.getTransactionID(), OperationType.R, "student_1"),
        Operation(t1.getTransactionID(), OperationType.R, "student_2"),
        Operation(t2.getTransactionID(), OperationType.R, "student_2"),
        Operation(t2.getTransactionID(), OperationType.W, "student_1"),
        Operation(t3.getTransactionID(), OperationType.W, "student_2"),
        Operation(t1.getTransactionID(), OperationType.W, "student_1"),
    ]

    print("\nProcessing complex scenario:")
    for op in operations:
        print(f"\nProcessing: T{op.getOpTransactionID()} {op.getOperationType().value} on {op.getOperationResource()}")
        
        # Validate operation
        response = ccm.validate_object(op)
        
        # Handle response
        if response.responseType == ResponseType.ALLOWED:
            print(f"Operation ALLOWED - proceeding with lock acquisition")
            ccm.log_object(op)  # This will now handle the lock granting
        elif response.responseType == ResponseType.WAITING:
            print(f"Operation WAITING - transaction T{op.getOpTransactionID()} must wait")
        else:  # ABORT
            print(f"Operation caused ABORT for transaction T{op.getOpTransactionID()}")
            ccm.end_transaction(op.getOpTransactionID())

    # End any remaining active transactions
    for tx in [t1, t2, t3]:
        if tx.getTransactionStatus() not in [TransactionStatus.TERMINATED, TransactionStatus.ABORTED]:
            print(f"\nEnding remaining transaction T{tx.getTransactionID()}")
            ccm.end_transaction(tx.getTransactionID())

    print("\nFinal Status:")
    print(f"Waiting List Transactions: {ccm.schedule.getTransactionWaitingList()}")
    # print(f"Shared Lock Table: {dict(ccm.shared_lock_table)}")
    # print(f"Exclusive Lock Table: {ccm.exclusive_lock_table}")

def main():
    print_status("DEADLOCK PREVENTION SIMULATION")
    simulate()

if __name__ == "__main__":
    main()