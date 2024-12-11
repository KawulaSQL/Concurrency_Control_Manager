from enum import Enum

class LockType(Enum):
    S = "Shared"
    X = "Exclusive"
    N = "None"
class LockStatus(Enum):
    HOLDING = "Holding"
    WAITING = "Waiting"
class OperationType(Enum):
    R = "READ"
    W = "WRITE"
    C = "COMMIT"
    A = "ABORT"
    SL = "Shared Lock"
    XL = "Exclusive Lock"
    UL = "Unlock"
class OperationStatus(Enum):
    E = "Executed"
    NE = "Not Executed"
class TransactionStatus(Enum):
    ACTIVE = "Active"
    PARTIALCOMMITTED = "Partially Committed"
    COMMITTED = "Committed"
    FAILED = "Failed"
    ABORTED = "Aborted"
    TERMINATED = "Terminated"
class ResponseType(Enum):
    ALLOWED = 'Allowed'
    WAITING = 'Waiting'
    ABORT = 'Abort'
