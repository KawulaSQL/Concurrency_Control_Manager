from enum import Enum

class LockType(Enum):
    S = "Shared"
    X = "Exclusive"
    N = "None"
class OperationType(Enum):
    R = "Read"
    W = "Write"
    SL = "Shared Lock"
    XL = "Exclusive Lock"
    UL = "Unlock"
    C = "Commit"
    A = "Abort"
class OperationStatus(Enum):
    E = "Executed"
    NE = "Not Executed"