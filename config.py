FAULT_TOLERANCE_F = 1
QUORUM_SIZE = FAULT_TOLERANCE_F + 1  # 2

NETWORK_CONFIG = {
    "VP_CO": [
        {"id": "co_0", "host": "127.0.0.1", "port": 5001},
        {"id": "co_1", "host": "127.0.0.1", "port": 5002},
        {"id": "co_2", "host": "127.0.0.1", "port": 5003}
    ],
    "EXECUTORS": [
        {"id": "exec_1", "host": "127.0.0.1", "port": 6001}
    ]
}
