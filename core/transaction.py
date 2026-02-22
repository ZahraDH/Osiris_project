# core/transaction.py

class TransactionEngine:
    def __init__(self):
        pass

    def execute(self, tx, mode='read', state=None):
        if state is None:
            raise ValueError("State must be provided to execute transaction")

        op = tx.get("op")
        args = tx.get("args", {})

        if mode == 'write':
            return self._execute_write(op, args, state)
        else:
            return self._execute_read(op, args, state)

    def _execute_write(self, op, args, state):
        key = args.get("key")
        value = args.get("value")

        if op == "SET":
            state[key] = value
            return "OK"
        elif op == "ADD":
            current = state.get(key, 0)
            state[key] = current + value
            return state[key]
        elif op == "SUB":
            current = state.get(key, 0)
            state[key] = current - value
            return state[key]
        else:
            raise ValueError(f"Unknown WRITE op: {op}")

    def _execute_read(self, op, args, state):
        if op == "GET":
            key = args.get("key")
            return state.get(key)
        elif op == "MULTIPLY":
            key = args.get("key")
            factor = args.get("factor", 1)
            val = state.get(key, 0)
            return val * factor
        elif op == "SUM_ALL":

            total = 0
            for v in state.values():
                if isinstance(v, (int, float)):
                    total += v
            return total
        else:
            raise ValueError(f"Unknown READ op: {op}")

