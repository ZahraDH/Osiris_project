# core/crypto.py
import os
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization

class CryptoManager:
    def __init__(self, node_id, keys_dir="keys"):
        self.node_id = node_id
        self.keys_dir = keys_dir
        self.private_key = None
        self.public_keys = {} 
        
        if not os.path.exists(keys_dir):
            os.makedirs(keys_dir)

    def load_keys(self):
        priv_path = os.path.join(self.keys_dir, f"{self.node_id}_private.pem")
        if os.path.exists(priv_path):
            with open(priv_path, "rb") as f:
                self.private_key = serialization.load_pem_private_key(
                    f.read(), password=None
                )
        else:
            raise FileNotFoundError(f"Private key for {self.node_id} not found! Run keygen first.")

        for filename in os.listdir(self.keys_dir):
            if filename.endswith("_public.pem"):
                filepath = os.path.join(self.keys_dir, filename)
                
                if os.path.isfile(filepath):
                    target_id = filename.replace("_public.pem", "")
                    with open(filepath, "rb") as f:
                        self.public_keys[target_id] = serialization.load_pem_public_key(f.read())


    def sign(self, message_str):
        if not self.private_key:
            raise Exception("Private key not loaded")
        
        signature = self.private_key.sign(
            message_str.encode('utf-8'),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256()
        )
        return signature.hex() 

    def verify(self, sender_id, message_str, signature_hex):
        if sender_id not in self.public_keys:
            print(f"No public key found for {sender_id}")
            return False
            
        public_key = self.public_keys[sender_id]
        try:
            public_key.verify(
                bytes.fromhex(signature_hex),
                message_str.encode('utf-8'),
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
                hashes.SHA256()
            )
            return True
        except Exception as e:
            print(f"Signature verification failed: {e}")
            return False
