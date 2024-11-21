from enum import Enum

class LockType(Enum):
    S = "Shared"
    X = "Exclusive"
    N = "None"