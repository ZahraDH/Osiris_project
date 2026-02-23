# core/types.py
import json
import time
import uuid

class Task:
    def __init__(self, payload, task_type="COMPUTATION", task_id=None):
        self.id = task_id if task_id else str(uuid.uuid4())
        self.payload = payload  
        self.type = task_type   
        self.timestamp = time.time()

    def to_dict(self):
        return self.__dict__

    @staticmethod
    def from_dict(data):
        t = Task(data['payload'], data['type'], data['id'])
        t.timestamp = data['timestamp']
        return t

class Message:
    def __init__(self, sender_id, msg_type, content, signature=None):
        self.sender_id = sender_id
        self.type = msg_type      
        self.content = content    
        self.signature = signature 

    def serialize(self):
        return json.dumps(self.__dict__).encode('utf-8')

    @staticmethod
    def deserialize(bytes_data):
        try:
            data = json.loads(bytes_data.decode('utf-8'))
            return Message(data['sender_id'], data['type'], data['content'], data['signature'])
        except Exception as e:
            print(f"Deserialization Error: {e}")
            return None
        
class ResultChunk:
    def __init__(self, task_id, chunk_index, total_chunks, data):
        self.task_id = task_id
        self.chunk_index = chunk_index
        self.total_chunks = total_chunks
        self.data = data 

    def to_dict(self):
        return self.__dict__

class MessageType:
    HELLO = "HELLO"
    CLIENT_REQUEST = "CLIENT_REQUEST" 
    STATE_UPDATE = "STATE_UPDATE"      
    COMPUTE_TASK = "COMPUTE_TASK"     
    RESULT = "RESULT"  
    TASK_ASSIGNMENT = "TASK_ASSIGNMENT"  
    RESULT_CHUNK = "RESULT_CHUNK"  
    VERIFICATION_VOTE = "VERIFICATION_VOTE"
    CHUNK_RESULT = "CHUNK_RESULT"
