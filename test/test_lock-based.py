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
    
    ccm = TwoPhaseLocking(deadlock_prevention="WAIT-DIE")
    schedule = Schedule()

    t1 = Transaction()
    t2 = Transaction()
    t3 = Transaction()

    for tx in [t1, t2, t3]:
        schedule.addTransaction(tx)

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
        
        response = ccm.validate_object(op)
        if response.responseType == ResponseType.ALLOWED:
            print(f"Operation ALLOWED - acquiring locks")
            ccm.log_object(op)
        elif response.responseType == ResponseType.WAITING:
            print(f"Operation WAITING - transaction must wait")
        else:  # ABORT
            print(f"Operation caused ABORT")
            ccm.end_transaction(op.getOpTransactionID())

    for tx in [t1, t2, t3]:
        if tx.getTransactionStatus() not in [TransactionStatus.TERMINATED, TransactionStatus.ABORTED]:
            ccm.end_transaction(tx.getTransactionID())

    print(f"Waiting List Transaction: {schedule.getTransactionWaitingList()}")

def main():
    print_status("DEADLOCK PREVENTION SIMULATION")
    simulate()

if __name__ == "__main__":
    main()