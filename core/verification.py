# core/verification_rules.py

class VerificationOperators:
    @staticmethod
    def estimate_output_size(tx):
        """
        Implementation of outputSize(t).
        Deterministically calculates the exact number of expected records 
        based solely on the transaction parameters, BEFORE execution.
        """
        op = tx.get("op", "")
        if not op and "task" in tx and isinstance(tx["task"], dict):
            op = tx["task"].get("op", "")
        if op in ["SUM_ALL", "COUNT", "MAX", "MIN", "AVERAGE"]:
            return 1
        elif op == "GET":
            return 1
        elif op == "GET_RANGE":
            params = tx.get("params", {})
            start = params.get("start", 0)
            end = params.get("end", 0)
            return (end - start) if (end >= start) else 0
            
        return -1 
    
    
    @staticmethod
    def happens_before(rec_a, rec_b):
        """
        Implementation of happensBefore(a, b).
        Mathematically ensures that Record A strictly precedes Record B (a ≺ b).
        This prevents the executor from injecting duplicate or unordered malicious data.
        """
        if isinstance(rec_a, dict) and isinstance(rec_b, dict):
            if "key" in rec_a and "key" in rec_b:
                return str(rec_a["key"]) < str(rec_b["key"])
        try:
            return rec_a < rec_b
        except TypeError:
            return False


    @staticmethod
    def verify_ordering(all_records):
        """
        Iterates through the chunk and applies happensBefore(a, b) to all adjacent pairs.
        """
        if len(all_records) < 2:
            return True 
        for i in range(len(all_records) - 1):
            if not VerificationOperators.happens_before(all_records[i], all_records[i + 1]):
                return False
                
        return True

    @staticmethod
    def verify_records_validity(chunk_data, tx):
        """
        Sanity checks (Valid(r) function). 
        Ensures the data structure and mathematical boundaries are respected.
        """
        if not isinstance(chunk_data, list):
            return False
            
        op = tx.get("op", "")
        if not op and "task" in tx and isinstance(tx["task"], dict):
            op = tx["task"].get("op", "")

        for record in chunk_data:
            if op == "SUM_ALL":
                extracted_value = None
                if isinstance(record, dict):
                    for v in record.values():
                        if isinstance(v, (int, float)):
                            extracted_value = v
                            break
                elif isinstance(record, (int, float)):
                    extracted_value = record
                
                if extracted_value is None:
                    return False 
                if extracted_value < 0:
                    print(f"[FRAUD DETECTED] Impossible negative value for SUM: {extracted_value}")
                    return False
            if op in ["GET", "GET_RANGE"]:
                if not isinstance(record, dict) or "key" not in record or "value" not in record:
                    return False
                    
        return True



