# tools/keygen.py
import os
import json
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

CONFIG_PATH = "../config.json" 
KEYS_DIR = "../keys"

if not os.path.exists(KEYS_DIR):
    os.makedirs(KEYS_DIR)

def generate_key_pair(node_id):
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    public_key = private_key.public_key()

    with open(f"{KEYS_DIR}/{node_id}_private.pem", "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))

    with open(f"{KEYS_DIR}/{node_id}_public.pem", "wb") as f:
        f.write(public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ))
    print(f"Generated keys for {node_id}")

try:
    with open("config.json", "r") as f:
        config = json.load(f)
        for node_id in config["nodes"]:
            generate_key_pair(node_id)
except FileNotFoundError:
    # utils/keygen.py
    nodes = ["CO_1", "CO_2", "CO_3", "EP_1", "EP_Bad", "VP_1", "VP_2", "Client"]
    for n in nodes:
        generate_key_pair(n)
