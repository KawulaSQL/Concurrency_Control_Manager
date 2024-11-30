from dataclasses import dataclass
from models.Operation import Operation
from models.CCManagerEnums import ResponseType

@dataclass
class Response:
    responseType: ResponseType
    operation: Operation