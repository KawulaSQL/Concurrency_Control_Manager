from abc import ABC, abstractmethod

from Resource import Resource
from Response import Response,Operation

class ControllerMethod(ABC):
    """
    Abstract base class for concurrency control methods.
    """

    @abstractmethod
    def log_object(self, operation: Operation): 
        """
        Logs an operation involving a resource by a transaction. 
        Logs an object (Resource) on transaction <transaction_id>. 
        This log can be used to implement the lock on an object or to have a timestamp on the object based on the method.
        
        :param resource: The resource involved in the operation.
        :param transaction_id: The ID of the transaction performing the operation.
        """
        pass

    @abstractmethod
    def validate_object(self, operation: Operation) -> Response:
        """
        Validates an operation involving a resource by a transaction. 
        Validates a given object whether it is allowed to do a particular action or not. 
        he response would either be to allow the transaction or not.
        
        :param resource: The resource involved in the operation.
        :param transaction_id: The ID of the transaction performing the operation.
        :param action: The action (e.g., READ, WRITE) being performed.
        :return: A Response object indicating success or failure.
        """
        pass

    @abstractmethod
    def end_transaction(self, transaction_id: int):
        """
        Validates an operation involving a resource by a transaction. 
        Validates a given object whether it is allowed to do a particular action or not. 
        he response would either be to allow the transaction or not.
        
        :param resource: The resource involved in the operation.
        :param transaction_id: The ID of the transaction performing the operation.
        :param action: The action (e.g., READ, WRITE) being performed.
        :return: A Response object indicating success or failure.
        """
        pass
