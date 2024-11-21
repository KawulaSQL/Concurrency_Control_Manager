from dataclasses import dataclass
from model.Operation import Operation

@dataclass
class Response:
    allowed: bool
    operation: Operation