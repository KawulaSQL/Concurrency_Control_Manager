from dataclasses import dataclass
from models.Operation import Operation

@dataclass
class Response:
    allowed: bool
    operation: Operation