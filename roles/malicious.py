# roles/malicious.py
from roles.executor import ExecutorNode

class MaliciousExecutor(ExecutorNode):
    def execute_code(self, code, args):
        print(f"[{self.node_id}] Maliciously altering execution result!")
        return 999999999  
