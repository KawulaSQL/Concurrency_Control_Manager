from models.Transaction import Transaction
from models.Operation import Operation
from models.CCManagerEnums import OperationType, ResponseType, TransactionStatus
from models.Schedule import Schedule
from models.TwoPhaseLockingv3 import TwoPhaseLockingv3

def print_status(msg: str):
    print("\n" + "="*50)
    print(msg)
    print("="*50 + "\n")

def simulate_wait_die():
    print_status("Starting Wait-Die Simulation")
    
    ccm = TwoPhaseLockingv3(deadlock_prevention="WAIT-DIE")
    schedule = Schedule()

    t1 = Transaction()
    t2 = Transaction()
    t3 = Transaction()

    schedule.addTransaction(t1)
    schedule.addTransaction(t2)
    schedule.addTransaction(t3)

    print("Created transactions with IDs:")
    print(f"T1 (oldest) : {t1.getTransactionID()}")
    print(f"T2 (middle): {t2.getTransactionID()}")
    print(f"T3 (youngest): {t3.getTransactionID()}")

    operations = [
        Operation(t1.getTransactionID(), OperationType.R, "student_1"),
        Operation(t2.getTransactionID(), OperationType.W, "student_1"),
        Operation(t3.getTransactionID(), OperationType.R, "student_1"),
        Operation(t1.getTransactionID(), OperationType.C, "student_1"),
        Operation(t2.getTransactionID(), OperationType.C, "student_1"),
    ]

    print("\nProcessing operations for Wait-Die:")
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

def simulate_wound_wait():
    print_status("Starting Wound-Wait Simulation")
    
    ccm = TwoPhaseLockingv3(deadlock_prevention="WOUND-WAIT")
    schedule = Schedule()

    t1 = Transaction()
    t2 = Transaction()
    t3 = Transaction()

    schedule.addTransaction(t1)
    schedule.addTransaction(t2)
    schedule.addTransaction(t3)

    print("Created transactions with IDs:")
    print(f"T1 (oldest) : {t1.getTransactionID()}")
    print(f"T2 (middle): {t2.getTransactionID()}")
    print(f"T3 (youngest): {t3.getTransactionID()}")

    operations = [
        Operation(t3.getTransactionID(), OperationType.W, "student_2"),
        Operation(t1.getTransactionID(), OperationType.R, "student_2"),
        Operation(t2.getTransactionID(), OperationType.W, "student_2"),
        Operation(t1.getTransactionID(), OperationType.C, "student_2"),
        Operation(t3.getTransactionID(), OperationType.W, "student_2"),
    ]

    print("\nProcessing operations for Wound-Wait:")
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

def simulate_complex_scenario():
    print_status("Starting Complex Scenario Simulation")
    
    ccm = TwoPhaseLockingv3(deadlock_prevention="WAIT-DIE")
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
        Operation(t1.getTransactionID(), OperationType.C, None),
        Operation(t2.getTransactionID(), OperationType.C, None),
        Operation(t3.getTransactionID(), OperationType.C, None),
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

def main():
    print_status("DEADLOCK PREVENTION SIMULATION")
    
    simulate_wait_die()
    simulate_wound_wait()
    simulate_complex_scenario()

if __name__ == "__main__":
    main()