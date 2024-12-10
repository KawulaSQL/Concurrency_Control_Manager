from dataclasses import dataclass
from Operation import Operation
from CCManagerEnums import ResponseType

@dataclass
class Response:
    responseType: ResponseType
    operation: Operation