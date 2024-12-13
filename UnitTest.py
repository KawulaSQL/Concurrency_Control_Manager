import unittest
from ConcurrencyControlManager import ConcurrencyControlManager, Operation
from models.Resource import Resource
from models.CCManagerEnums import OperationType, ResponseType
import time

class UnitTest(unittest.TestCase):
    def setUp(self):
        self.ccm_2PL = ConcurrencyControlManager("2PL")
        self.tx_id_1_2PL = self.ccm_2PL.begin_transaction()
        self.tx_id_2_2PL = self.ccm_2PL.begin_transaction()

        self.ccm_TSO = ConcurrencyControlManager("TSO")
        self.tx_id_1_TSO = self.ccm_TSO.begin_transaction()
        time.sleep(1)
        self.tx_id_2_TSO = self.ccm_TSO.begin_transaction()

        self.allowed_response = ResponseType.ALLOWED
        self.waiting_response = ResponseType.WAITING
        self.aborted_response = ResponseType.ABORT
        self.reponses = [self.allowed_response, self.waiting_response, self.aborted_response]

        self.resource_a = Resource("user")
        self.resource_b = Resource("schedule")
        self.resource_c = Resource("transportation")


    def test_normal_case_2PL(self):
        opList = [
            Operation(self.tx_id_1_2PL, OperationType.R, self.resource_a),
            Operation(self.tx_id_1_2PL, OperationType.W, self.resource_b),
            Operation(self.tx_id_1_2PL, OperationType.R, self.resource_c),
            Operation(self.tx_id_2_2PL, OperationType.R, self.resource_a)
        ]
        results = [self.ccm_2PL.validate_object(operation) for operation in opList]
        has_waiting_or_aborted = any(
            result.responseType == self.waiting_response or
            result.responseType == self.aborted_response
            for result in results
        )
        self.assertFalse(has_waiting_or_aborted, "There is Response that returned ResponseType.WAITING or ResponseType.ABORT.")

    def test_wait_case_2PL(self):
        opList = [
            Operation(self.tx_id_2_2PL, OperationType.W, self.resource_a),
            Operation(self.tx_id_1_2PL, OperationType.W, self.resource_a)
        ]
        results = []
        for operation in opList:
            result = self.ccm_2PL.validate_object(operation)
            self.ccm_2PL.log_object(operation)
            results.append(result)
        has_waiting = any(result.responseType == self.waiting_response for result in results)
        self.assertTrue(has_waiting, "No response returned ResponseType.WAITING.")

    def test_abort_case_2PL(self):
        opList = [
            Operation(self.tx_id_1_2PL, OperationType.W, self.resource_a),
            Operation(self.tx_id_2_2PL, OperationType.R, self.resource_b),
            Operation(self.tx_id_2_2PL, OperationType.W, self.resource_a)
        ]
        results = []
        for operation in opList:
            result = self.ccm_2PL.validate_object(operation)
            self.ccm_2PL.log_object(operation)
            results.append(result)
        has_aborted = any(result.responseType == self.aborted_response for result in results)
        self.assertTrue(has_aborted, "No response returned ResponseType.ABORTED.")

    def test_normal_case_TSO(self):
        opList = [
            Operation(self.tx_id_1_TSO, OperationType.R, self.resource_a),
            Operation(self.tx_id_2_TSO, OperationType.R, self.resource_a),
            Operation(self.tx_id_1_TSO, OperationType.W, self.resource_b),
            Operation(self.tx_id_2_TSO, OperationType.W, self.resource_c),
        ]
        results = []
        for operation in opList:
            result = self.ccm_TSO.validate_object(operation)
            self.ccm_TSO.log_object(operation)
            results.append(result)

        has_waiting_or_aborted = any(
            result.responseType == self.waiting_response or
            result.responseType == self.aborted_response
            for result in results
        )
        self.assertFalse(has_waiting_or_aborted, "There is Response that returned ResponseType.WAITING or ResponseType.ABORT.")

    def test_abort_case_TSO(self):
        opList = [
            Operation(self.tx_id_2_TSO, OperationType.R, self.resource_a),
            Operation(self.tx_id_2_TSO, OperationType.R, self.resource_b),
            Operation(self.tx_id_1_TSO, OperationType.W, self.resource_a),
        ]
        results = []
        for operation in opList:
            result = self.ccm_TSO.validate_object(operation)
            self.ccm_TSO.log_object(operation)
            results.append(result)
        has_aborted = any(result.responseType == self.aborted_response for result in results)
        self.assertTrue(has_aborted, "No response returned ResponseType.ABORTED.")

    def tearDown(self):
        # End transactions for cleanup
        self.ccm_2PL.end_transaction(self.tx_id_1_2PL)
        self.ccm_2PL.end_transaction(self.tx_id_2_2PL)
        self.ccm_TSO.end_transaction(self.tx_id_1_TSO)
        self.ccm_TSO.end_transaction(self.tx_id_2_TSO)

if __name__ == "__main__":
    unittest.main()
