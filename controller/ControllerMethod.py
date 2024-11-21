from abc import ABC, abstractmethod
from model.Resource import Resource
from model.Action import Action
from model.Response import Response


class ControllerMethod(ABC):
    """
    Abstract base class for concurrency control methods.
    """

    @abstractmethod
    def log_object(self, resource: Resource, transaction_id: int):
        """
        Logs an operation involving a resource by a transaction. 
        Logs an object (Resource) on transaction <transaction_id>. 
        This log can be used to implement the lock on an object or to have a timestamp on the object based on the method.
        
        :param resource: The resource involved in the operation.
        :param transaction_id: The ID of the transaction performing the operation.
        """
        pass

    @abstractmethod
    def validate_object(self, resource: Resource, transaction_id: int, action: Action) -> Response:
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
    def run(self):
        """
        Execute the concurrency control mechanism's logic (e.g., validate locks, resolve conflicts).
        """
        pass

    @abstractmethod
    def commit(self):
        """
        Commit a transaction, making its changes permanent.
        """
        pass

    @abstractmethod
    def abort(self):
        """
        Abort a transaction, rolling back any changes made.
        """
        pass
