# core/verification_rules.py

class VerificationOperators:
    @staticmethod
    def estimate_output_size(tx):
        op = tx.get("op")
        if op in ["SUM_ALL", "GET", "MULTIPLY"]:
            return 1
        elif op == "GET_RANGE":
            start = tx.get("params", {}).get("start", 0)
            end = tx.get("params", {}).get("end", 0)
            return (end - start) if (end >= start) else 0        
        return -1 


    @staticmethod
    def verify_records_validity(chunk_data, tx):
        if not isinstance(chunk_data, list):
            return False
            
        op = tx.get("op")
        for record in chunk_data:
            if op == "SUM_ALL":
                extracted_value = None
                if isinstance(record, dict):
                    for v in record.values():
                        if isinstance(v, (int, float)):
                            extracted_value = v
                            break
                    if extracted_value is None:
                        return False 
                elif isinstance(record, (int, float)):
                    extracted_value = record
                else:
                    return False 
                if extracted_value < 0:
                    print(f"[FRAUD DETECTED] Impossible value for SUM: {extracted_value}")
                    return False
                
            if op in ["GET", "GET_RANGE"]:
                if not isinstance(record, dict) or "key" not in record or "value" not in record:
                    return False
                    
        return True

    @staticmethod
    def verify_ordering(all_records):
        if len(all_records) < 2:
            return True

        try:
            for i in range(len(all_records) - 1):
                rec_a = all_records[i]
                rec_b = all_records[i + 1]
                
                if isinstance(rec_a, dict) and isinstance(rec_b, dict):
                    if "key" in rec_a and "key" in rec_b:
                        if str(rec_a["key"]) >= str(rec_b["key"]):
                            return False
        except Exception:
            return False
            
        return True
