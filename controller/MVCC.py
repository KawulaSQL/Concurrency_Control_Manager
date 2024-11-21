from datetime import datetime
from models.Operation import Operation
from models.Resource import Resource
from models.Transaction import Transaction
from models.CCManagerEnums import OperationType, OperationStatus, TransactionStatus

class MVCC:
    def __init__(self):
        self.sequence = []
        self.transactions = {}
        self.resources = {}
        self.result = []
        self.transaction_history = []

    def process_read(self, transaction: Transaction, resource: Resource):
        if transaction.startTS == datetime.max:
            transaction.markStartTS()

        valid_versions = [v for v in resource.versions if v["wts"] <= transaction.startTS]
        if not valid_versions:
            self.abort(transaction, resource, OperationType.READ)
            return

        latest_version = max(valid_versions, key=lambda v: v["wts"])
        latest_version["rts"] = max(latest_version["rts"], transaction.startTS)
        transaction.addToReadSet(resource)

        self.log_transaction(transaction, resource, OperationType.READ, OperationStatus.SUCCESS)
        self.result.append(f"R{transaction.txid}({resource.name})")

    def process_write(self, transaction: Transaction, resource: Resource):
        if transaction.startTS == datetime.max:
            transaction.markStartTS()

        valid_versions = [v for v in resource.versions if v["wts"] <= transaction.startTS]
        if valid_versions and transaction.startTS < max(v["rts"] for v in valid_versions):
            self.abort(transaction, resource, OperationType.WRITE)
            return

        new_version = {
            "value": f"Written by T{transaction.txid}",
            "wts": transaction.startTS,
            "rts": transaction.startTS
        }
        resource.versions.append(new_version)
        transaction.addToWriteSet(resource)

        self.log_transaction(transaction, resource, OperationType.WRITE, OperationStatus.SUCCESS)
        self.result.append(f"W{transaction.txid}({resource.name})")

    def commit(self, transaction: Transaction):
        transaction.markFinishTS()
        transaction.setTransactionStatus(TransactionStatus.COMMITTED)

        self.log_transaction(transaction, None, OperationType.COMMIT, OperationStatus.SUCCESS)
        self.result.append(f"C{transaction.txid}")
        del self.transactions[transaction.txid]

    def abort(self, transaction: Transaction, resource: Resource, operationType: OperationType):
        transaction.abort()
        transaction.setTransactionStatus(TransactionStatus.ABORTED)

        self.sequence = [op for op in self.sequence if op["transaction"] != transaction.txid]
        self.result = [op for op in self.result if not op.startswith(f"{operationType}{transaction.txid}")]
        self.log_transaction(transaction, resource, operationType, OperationStatus.ABORTED)

    def run(self):
        while self.sequence:
            current = self.sequence.pop(0)
            transaction_id = current["transaction"]
            transaction = self.transactions[transaction_id]

            if current["operation"] == OperationType.READ:
                self.process_read(transaction, self.resources[current["resource"]])
            elif current["operation"] == OperationType.WRITE:
                self.process_write(transaction, self.resources[current["resource"]])
            elif current["operation"] == OperationType.COMMIT:
                self.commit(transaction)

    def log_transaction(self, transaction: Transaction, resource: Resource, operation: OperationType, status: OperationStatus):
        resource_name = resource.name if resource else "-"
        self.transaction_history.append({
            "transaction": transaction.txid,
            "resource": resource_name,
            "operation": operation,
            "status": status
        })

    def result_string(self) -> str:
        return "; ".join(self.result)

    def history_string(self) -> str:
        return "\n".join(
            f"{entry['operation']} {entry['transaction']} {entry['resource']} ({entry['status']})"
            for entry in self.transaction_history
        )

    def resource_versions_string(self) -> str:
        result = []
        for resource_name, resource in self.resources.items():
            result.append(f"Resource {resource_name}:")
            for version in resource.versions:
                result.append(f"  Value: {version['value']}, WTS: {version['wts']}, RTS: {version['rts']}")
        return "\n".join(result)
