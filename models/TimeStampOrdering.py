from ControllerMethod import ControllerMethod
from Resource import Resource
from CCManagerEnums import Action, ResponseType, OperationType
from Response import Response
from Operation import Operation
from datetime import datetime

class TimestampOrdering(ControllerMethod):
    def validate_object(self, operation: Operation) -> Response: #rough implementation
        """
        Validating timestamp on object/resource
        """
        # if action == Action.READ:
        #     if transaction_id < resource.getWTS():
        #         return Response(ResponseType.ABORT, None)
        #     if transaction_id > resource.getRTS():
        #         resource.setRTS(transaction_id)
        #     return Response(ResponseType.ALLOWED, None)
            
        # elif action == Action.WRITE:
        #     if (transaction_id < resource.getRTS() or 
        #         transaction_id < resource.getWTS()):
        #         return Response(ResponseType.ABORT, None)
        #     resource.setWTS(transaction_id)
        #     return Response(ResponseType.ALLOWED, None)

    def log_object(self, Operation: operation): 
        """
        give timestamp to object.resource
        """
        # self.transaction_history.append({
        #     "transaction": transaction_id,
        #     "resource": resource.getName(),
        #     "timestamp": datetime.now()
        # })

    def end_transaction(self, transaction_id: int):
        """
        Implementation
        """

    # def commit(self, transaction_id: int):
    #     if transaction_id in self.aborted_transactions:
    #         self.aborted_transactions.remove(transaction_id)

    # def abort(self, transaction_id: int):
    #     self.aborted_transactions.add(transaction_id)

    # def get_transaction_history(self):
    #     return self.transaction_history
    
    # def get_aborted_transactions(self):
    #     return self.aborted_transactions
    
    # def clear_transaction_history(self):
    #     self.transaction_history = []

    # def clear_aborted_transactions(self):
    #     self.aborted_transactions = set()