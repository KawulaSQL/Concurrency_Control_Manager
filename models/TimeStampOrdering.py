from ControllerMethod import ControllerMethod
from Resource import Resource
from CCManagerEnums import Action, ResponseType, OperationType
from Response import Response, Operation
from datetime import datetime

class TimestampOrdering(ControllerMethod):
    def __init__(self):
        self.transaction_history = []
        self.aborted_transactions = set()

    def process_query(self, query: Operation) -> Response:
        transaction_id = query.getOpTransactionID()
        resource = query.getOperationResource()
        
        # Skip jika transaksi sudah diabort
        if transaction_id in self.aborted_transactions:
            return Response(ResponseType.ABORT, query)

        # Validasi operasi berdasarkan aturan timestamp ordering
        if query.getOperationType() == OperationType.READ:
            action = Action.READ
        else:
            action = Action.WRITE

        response = self.validate_object(resource, transaction_id, action)
        if response.responseType == ResponseType.ALLOWED:
            self.log_object(resource, transaction_id)
            
        return response

    def validate_object(self, resource: Resource, transaction_id: int, action: Action) -> Response:
        if action == Action.READ:
            if transaction_id < resource.getWTS():
                return Response(ResponseType.ABORT, None)
            if transaction_id > resource.getRTS():
                resource.setRTS(transaction_id)
            return Response(ResponseType.ALLOWED, None)
            
        elif action == Action.WRITE:
            if (transaction_id < resource.getRTS() or 
                transaction_id < resource.getWTS()):
                return Response(ResponseType.ABORT, None)
            resource.setWTS(transaction_id)
            return Response(ResponseType.ALLOWED, None)

    def log_object(self, resource: Resource, transaction_id: int):
        self.transaction_history.append({
            "transaction": transaction_id,
            "resource": resource.getName(),
            "timestamp": datetime.now()
        })

    def commit(self, transaction_id: int):
        if transaction_id in self.aborted_transactions:
            self.aborted_transactions.remove(transaction_id)

    def abort(self, transaction_id: int):
        self.aborted_transactions.add(transaction_id)

    def get_transaction_history(self):
        return self.transaction_history
    
    def get_aborted_transactions(self):
        return self.aborted_transactions
    
    def clear_transaction_history(self):
        self.transaction_history = []

    def clear_aborted_transactions(self):
        self.aborted_transactions = set()