from collections import defaultdict

class TwoPhaseLocking:
    def __init__(self, input_sequence: str):
        self.sequence = []
        self.timestamp = []
        self.exclusive_lock_table = {}
        self.shared_lock_table = defaultdict(list)
        self.transaction_history = []
        self.result = []
        self.queue = []

     # Parse input sequence
        try:
            input_sequence = input_sequence.rstrip(";").split(";")
            for entry in input_sequence:
                entry = entry.strip()
                if entry[0] in {'R', 'W'}:
                    self.sequence.append({
                        "operation": entry[0],
                        "transaction": int(entry[1]),
                        "table": entry[3]
                    })
                    if int(entry[1]) not in self.timestamp:
                        self.timestamp.append(int(entry[1]))
                elif entry[0] == 'C':
                    self.sequence.append({"operation": entry[0], "transaction": int(entry[1])})
                else:
                    raise ValueError("Invalid operation detected.")
            # Ensure each transaction has a commit
            if len([x for x in self.sequence if x["operation"] == 'C']) != len(set(self.timestamp)):
                raise ValueError("Each transaction must have a commit operation.")
        except ValueError as e:
            raise ValueError(f"Invalid input sequence: {e}")

    def shared_lock(self, transaction: int, table: str) -> bool:
        """Acquire a shared lock."""
        if table in self.exclusive_lock_table:
            if self.exclusive_lock_table[table] == transaction:
                return True  # Already holds an exclusive lock
            return False
        if transaction in self.shared_lock_table[table]:
            return True  # Already holds a shared lock
        self.shared_lock_table[table].append(transaction)
        self.log_transaction(transaction, table, "SL", "Success")
        return True

    def exclusive_lock(self, transaction: int, table: str) -> bool:
        """Acquire an exclusive lock."""
        if table in self.shared_lock_table:
            if transaction in self.shared_lock_table[table] and len(self.shared_lock_table[table]) == 1:
                self.shared_lock_table[table].remove(transaction)
                self.exclusive_lock_table[table] = transaction
                self.log_transaction(transaction, table, "UPL", "Success")
                return True
            return False
        if table in self.exclusive_lock_table:
            return self.exclusive_lock_table[table] == transaction
        self.exclusive_lock_table[table] = transaction
        self.log_transaction(transaction, table, "XL", "Success")
        return True

    def release_locks(self, transaction: int):
        """Release all locks held by a transaction."""
        for table, holder in list(self.exclusive_lock_table.items()):
            if holder == transaction:
                del self.exclusive_lock_table[table]
                self.log_transaction(transaction, table, "UL", "Success")
        for table, holders in list(self.shared_lock_table.items()):
            if transaction in holders:
                holders.remove(transaction)
                self.log_transaction(transaction, table, "UL", "Success")
                if not holders:
                    del self.shared_lock_table[table]

    def wait_die(self, current: dict):
        """Deadlock prevention using the wait-die scheme."""
        transaction = current["transaction"]
        table = current["table"]
        if (
            table in self.exclusive_lock_table and
            self.timestamp.index(transaction) < self.timestamp.index(self.exclusive_lock_table[table])
        ) or (
            table in self.shared_lock_table and
            all(self.timestamp.index(transaction) < self.timestamp.index(t) for t in self.shared_lock_table[table])
        ):
            self.queue.append(current)
            self.log_transaction(transaction, table, current["operation"], "Queue")
        else:
            self.abort(current)

    def commit(self, transaction: int):
        """Commit a transaction and release its locks."""
        self.release_locks(transaction)
        self.log_transaction(transaction, "-", "Commit", "Success")

    def abort(self, current: dict):
        """Abort a transaction."""
        transaction = current["transaction"]
        self.sequence = [op for op in self.sequence if op["transaction"] != transaction]
        self.result = [op for op in self.result if op["transaction"] != transaction]
        self.release_locks(transaction)
        self.log_transaction(transaction, current.get("table", "-"), "Abort", "Abort")

    def run_queue(self):
        """Process queued operations."""
        while self.queue:
            transaction = self.queue.pop(0)
            if transaction["operation"] == "R" and self.shared_lock(transaction["transaction"], transaction["table"]):
                self.result.append(transaction)
            elif transaction["operation"] == "W" and self.exclusive_lock(transaction["transaction"], transaction["table"]):
                self.result.append(transaction)
            else:
                self.queue.insert(0, transaction)
                break

    def run(self):
        """Execute the sequence of operations."""
        while self.sequence:
            self.run_queue()
            current = self.sequence.pop(0)
            if current["operation"] == "C":
                self.commit(current["transaction"])
            elif current["operation"] == "R" and self.shared_lock(current["transaction"], current["table"]):
                self.result.append(current)
            elif current["operation"] == "W" and self.exclusive_lock(current["transaction"], current["table"]):
                self.result.append(current)
            else:
                self.wait_die(current)

    def log_transaction(self, transaction, table, operation, status):
        """Log transaction operations."""
        self.transaction_history.append({
            "transaction": transaction, "table": table, "operation": operation, "status": status
        })

    def result_string(self) -> str:
        """Generate a result string from the operations."""
        return ";".join(
            f"{op['operation']}{op['transaction']}({op['table']})" if "table" in op else f"{op['operation']}{op['transaction']}"
            for op in self.result
        )

    def history_string(self) -> str:
        """Generate a string representation of transaction history."""
        return "\n".join(
            f"{entry['operation']} {entry['transaction']} {entry['table']} ({entry['status']})"
            for entry in self.transaction_history
        )


if __name__ == "__main__":
    input_seq = input("Enter sequence (delimited by ';'): ")
    try:
        tpl = TwoPhaseLocking(input_seq)
        tpl.run()
        print("\nExecution Result:")
        print(tpl.result_string())
        print("\nTransaction History:")
        print(tpl.history_string())
    except Exception as e:
        print(f"Error: {e}")