from datetime import datetime
from collections import defaultdict
from models.Operation import Operation
from models.Resource import Resource
from models.Transaction import Transaction


class MVCC:
    def __init__(self, input_sequence: str):
        self.sequence = []
        self.transactions = {}
        self.resources = {}
        self.result = []
        self.transaction_history = []

        self.parse_input(input_sequence)

    def parse_input(self, input_sequence: str):
        """Parse input sequence into operations."""
        try:
            input_sequence = input_sequence.rstrip(";").split(";")
            for entry in input_sequence:
                entry = entry.strip()
                if entry[0] in {'R', 'W'}:
                    transaction_id = entry[1]
                    resource_name = entry[3]
                    if transaction_id not in self.transactions:
                        self.transactions[transaction_id] = Transaction(
                            txid=transaction_id, sl=[], xl=[]
                        )
                    if resource_name not in self.resources:
                        self.resources[resource_name] = Resource(
                            data=resource_name, rts=datetime.min, wts=datetime.min
                        )
                        self.resources[resource_name].versions = []  # Initialize versions
                        self.resources[resource_name].add_version("Initial", datetime.min)
                    self.sequence.append({
                        "operation": entry[0],
                        "transaction": transaction_id,
                        "resource": resource_name
                    })
                elif entry[0] == 'C':
                    transaction_id = entry[1]
                    self.sequence.append({
                        "operation": entry[0],
                        "transaction": transaction_id
                    })
                else:
                    raise ValueError(f"Invalid operation in sequence: {entry}")

            # Ensure each transaction has a commit operation
            active_transactions = {entry["transaction"] for entry in self.sequence if entry["operation"] != 'C'}
            committed_transactions = {entry["transaction"] for entry in self.sequence if entry["operation"] == 'C'}
            if active_transactions != committed_transactions:
                raise ValueError("Each transaction must have a commit operation.")
        except ValueError as e:
            raise ValueError(f"Invalid input sequence: {e}")

    def process_read(self, transaction_id: str, resource_name: str):
        """Process a READ operation."""
        transaction = self.transactions[transaction_id]
        resource = self.resources[resource_name]

        # Find the latest version of the resource with a write timestamp <= transaction's timestamp
        if transaction.startTS == datetime.max:
            transaction.markStartTS()

        valid_versions = [v for v in resource.versions if v["wts"] <= transaction.startTS]
        if not valid_versions:
            self.abort(transaction_id, resource_name, "R")
            return

        latest_version = max(valid_versions, key=lambda v: v["wts"])
        latest_version["rts"] = max(latest_version["rts"], transaction.startTS)
        transaction.addToReadSet(resource)

        self.log_transaction(transaction_id, resource_name, "R", "Success")
        self.result.append(f"R{transaction_id}({resource_name})")

    def process_write(self, transaction_id: str, resource_name: str):
        """Process a WRITE operation."""
        transaction = self.transactions[transaction_id]
        resource = self.resources[resource_name]

        if transaction.startTS == datetime.max:
            transaction.markStartTS()

        valid_versions = [v for v in resource.versions if v["wts"] <= transaction.startTS]
        if valid_versions and transaction.startTS < max(v["rts"] for v in valid_versions):
            self.abort(transaction_id, resource_name, "W")
            return

        # Create a new version of the resource
        new_version = {
            "value": f"Written by T{transaction_id}",
            "wts": transaction.startTS,
            "rts": transaction.startTS
        }
        resource.versions.append(new_version)
        transaction.addToWriteSet(resource)

        self.log_transaction(transaction_id, resource_name, "W", "Success")
        self.result.append(f"W{transaction_id}({resource_name})")

    def commit(self, transaction_id: str):
        """Commit a transaction and finalize its operations."""
        transaction = self.transactions[transaction_id]
        transaction.markFinishTS()

        self.log_transaction(transaction_id, "-", "Commit", "Success")
        self.result.append(f"C{transaction_id}")
        del self.transactions[transaction_id]

    def abort(self, transaction_id: str, resource_name: str, operation_type: str):
        """Abort a transaction."""
        transaction = self.transactions[transaction_id]
        transaction.abort()

        self.sequence = [op for op in self.sequence if op["transaction"] != transaction_id]
        self.result = [op for op in self.result if not op.startswith(f"{operation_type}{transaction_id}")]
        self.log_transaction(transaction_id, resource_name, "Abort", "Abort")

    def run(self):
        """Execute the sequence of operations."""
        while self.sequence:
            current = self.sequence.pop(0)
            transaction_id = current["transaction"]
            if current["operation"] == "R":
                self.process_read(transaction_id, current["resource"])
            elif current["operation"] == "W":
                self.process_write(transaction_id, current["resource"])
            elif current["operation"] == "C":
                self.commit(transaction_id)

    def log_transaction(self, transaction_id: str, resource: str, operation: str, status: str):
        """Log a transaction operation."""
        self.transaction_history.append({
            "transaction": transaction_id,
            "resource": resource,
            "operation": operation,
            "status": status
        })

    def result_string(self) -> str:
        """Generate a result string from the operations."""
        return "; ".join(self.result)

    def history_string(self) -> str:
        """Generate a string representation of transaction history."""
        return "\n".join(
            f"{entry['operation']} {entry['transaction']} {entry['resource']} ({entry['status']})"
            for entry in self.transaction_history
        )

    def resource_versions_string(self) -> str:
        """Generate a string representation of resource versions."""
        result = []
        for resource_name, resource in self.resources.items():
            result.append(f"Resource {resource_name}:")
            for version in resource.versions:
                result.append(f"  Value: {version['value']}, WTS: {version['wts']}, RTS: {version['rts']}")
        return "\n".join(result)


if __name__ == "__main__":
    input_seq = input("Enter sequence (delimited by ';'): ")
    try:
        mvcc = MVCC(input_seq)
        mvcc.run()
        print("\nExecution Result:")
        print(mvcc.result_string())
        print("\nTransaction History:")
        print(mvcc.history_string())
        print("\nResource Versions:")
        print(mvcc.resource_versions_string())
    except Exception as e:
        print(f"Error: {e}")
