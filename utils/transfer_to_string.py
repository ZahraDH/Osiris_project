import json

def get_deterministic_string(data: dict) -> str:
    return json.dumps(data, sort_keys=True, separators=(',', ':'))
