# roles/coordinator.py
import asyncio
import json
import uuid
import time
import hashlib
from core.node import Node
from utils.transfer_to_string import get_deterministic_string
from core.state import VersionedKVStore
from core.transaction import TransactionEngine
from core.types import MessageType

class CoordinatorNode(Node):
    def __init__(self, node_id,crypto_manager, config_path=None, f=1):
        if config_path:
            super().__init__(node_id, config_path)
        else:
            super().__init__(node_id)
        self.pending_tasks = {}
        self.crypto = crypto_manager
        self.seq_counter = 0
        self.f = f
        self.verification_state = {}
        self.tx_engine = TransactionEngine()
        self.tasks = {}
        self.is_leader = (self.node_id == "CO_1" or self.node_id == "VP_CO_1") 
        self.global_seq = 0
        self.pending_consensus = {}
        self.quorum = self.f + 1 

    async def process_message(self, message):
        if 'signature' in message:
            sender_id = message.get("sender_id")
            signature = message.get("signature")
            content_str = message.get("content") 
            
            is_valid = self.crypto.verify(sender_id, content_str, signature)
            
            if not is_valid:
                print(f"[BFT ALERT] Invalid signature from {sender_id}. Dropping fake message!")
                return {"status": "error"}
            
            try:
                msg_data = json.loads(content_str)
            except json.JSONDecodeError:
                return {"status": "error"}
        else:
            msg_data = message

        msg_type = msg_data.get("type")
        
        if msg_type == "CLIENT_REQUEST" or msg_type == getattr(MessageType, 'CLIENT_REQUEST', 'CLIENT_REQUEST'):
            payload = msg_data.get("payload", msg_data)
            await self.handle_client_request(payload)
            return {"status": "received"}
            
        elif msg_type == "CONSENSUS_PROPOSE":
            await self.handle_consensus_propose(msg_data, message.get("sender_id"))
            return {"status": "received"}
            
        elif msg_type == "CONSENSUS_VOTE":
            await self.handle_consensus_vote(msg_data, message.get("sender_id"))
            return {"status": "received"}

        elif msg_type == "RESULT":
            task_id = msg_data.get("task_id")
            result = msg_data.get("result")
            sender = msg_data.get("sender")
            if task_id in self.pending_tasks:
                future = self.pending_tasks[task_id]
                if not future.done():
                    future.set_result((sender, result))
            return {"status": "received"}
            
        elif msg_type == "TASK_ASSIGNMENT" or msg_type == getattr(MessageType, 'TASK_ASSIGNMENT', 'TASK_ASSIGNMENT'):
            await self.handle_task_assignment(msg_data)
            return {"status": "received"}
        
        elif msg_type == "VERIFICATION_VOTE" or msg_type == getattr(MessageType, 'VERIFICATION_VOTE', 'VERIFICATION_VOTE'):
            task_id = msg_data.get("task_id", "UNKNOWN")
            verifier_id = msg_data.get("verifier_id", "Unknown")
            is_valid = msg_data.get("is_valid", False)
            reason = msg_data.get("result", "")
            
            status = "APPROVED" if is_valid else "REJECTED"
            print(f"[{self.node_id}] Vote from {verifier_id} for Task {task_id[:8]}: {status}")
            
            if task_id in self.tasks:
                if verifier_id not in self.tasks[task_id]["votes"]["voters"]:
                    self.tasks[task_id]["votes"]["voters"].add(verifier_id)
                    if is_valid:
                        self.tasks[task_id]["votes"]["approve"] += 1
                    else:
                        self.tasks[task_id]["votes"]["reject"] += 1
                        if self.tasks[task_id]["votes"]["reject"] >= self.quorum and not self.tasks[task_id]["reassigned"]:
                            self.tasks[task_id]["reassigned"] = True
                            asyncio.create_task(self.reassign_task(task_id))
            return {"status": "received"}
            
        return await super().process_message(message)

    
    async def handle_client_request(self, tx):
        if isinstance(tx, str):
            try: tx = json.loads(tx)
            except: pass

        if self.is_leader:
            await self.initiate_consensus(tx)
        else:
            print(f"[{self.node_id}] I am a follower. Waiting for Leader to PROPOSE this tx.")
            
            
    async def initiate_consensus(self, tx):
        self.global_seq += 1
        proposed_seq = self.global_seq
        tx_id = tx.get("id", str(uuid.uuid4())[:8])
        tx["id"] = tx_id
        
        print(f"[LEADER {self.node_id}] Proposing Sequence {proposed_seq} for TX {tx_id}")
        
        propose_msg = {
            "type": "CONSENSUS_PROPOSE",
            "tx": tx,
            "seq": proposed_seq
        }
        
        self.pending_consensus[tx_id] = {
            "tx": tx,
            "seq": proposed_seq,
            "votes": {self.node_id}, 
            "committed": False
        }
        
        await self._broadcast_to_coordinators(propose_msg)

    async def handle_consensus_propose(self, msg_data, sender_id):
        tx = msg_data["tx"]
        proposed_seq = msg_data["seq"]
        tx_id = tx["id"]
        
        if proposed_seq > self.global_seq:
            self.global_seq = proposed_seq
            print(f"[{self.node_id}] Validated PROPOSE from {sender_id}. Sequence: {proposed_seq}. Voting YES.")
            
            self.pending_consensus[tx_id] = {
                "tx": tx,
                "seq": proposed_seq,
                "votes": {sender_id, self.node_id}, 
                "committed": False
            }
            
            vote_msg = {
                "type": "CONSENSUS_VOTE",
                "tx_id": tx_id,
                "seq": proposed_seq
            }
            await self._broadcast_to_coordinators(vote_msg)

    async def handle_consensus_vote(self, msg_data, voter_id):
        tx_id = msg_data["tx_id"]
        if tx_id in self.pending_consensus:
            state = self.pending_consensus[tx_id]
            state["votes"].add(voter_id)
            
            print(f"[{self.node_id}] Vote received from {voter_id}. Total votes: {len(state['votes'])}/{self.quorum}")
            
            if len(state["votes"]) >= self.quorum and not state["committed"]:
                state["committed"] = True
                agreed_seq = state["seq"]
                tx = state["tx"]
                
                print(f"[CONSENSUS REACHED - {self.node_id}] TX {tx_id} safely assigned to Sequence {agreed_seq}!")
                
                op = tx.get("op", "").upper()
                if op in ["SET", "ADD", "SUB", "ADD_EDGE"]:
                    await self.propose_state_update(tx, agreed_seq, list(state["votes"]))
                else:
                    await self.assign_compute_task(tx, agreed_seq, list(state["votes"]))
                    
    async def _broadcast_to_coordinators(self, payload_dict):
        coordinators = [nid for nid in self.peers if nid.startswith("CO_") or nid.startswith("VP_CO")]
        for co_id in coordinators:
            if co_id != self.node_id:
                await self._send_signed_message_fire_and_forget(co_id, payload_dict)



    async def propose_state_update(self, tx, agreed_seq, consensus_proof):
        update_msg = {
            "type": "STATE_UPDATE", 
            "seq": agreed_seq, 
            "tx": tx,
            "consensus_proof": consensus_proof
        }
        print(f"[{self.node_id}] Broadcasting State Update Seq {agreed_seq}")
        for peer_id in self.peers:
            await self._send_signed_message_fire_and_forget(peer_id, update_msg)
            

    async def assign_compute_task(self, tx, agreed_seq, consensus_proof):
        tx_string = json.dumps(tx, sort_keys=True)
        task_id_source = f"{tx_string}_seq{agreed_seq}"
        task_id = hashlib.sha256(task_id_source.encode()).hexdigest()[:16]
        
        executors = sorted([nid for nid in self.peers if nid.startswith("EP")])
        if not executors: return
            
        hash_int = int(task_id, 16)
        assigned_executor = executors[hash_int % len(executors)]
        
        verifiers = sorted([nid for nid in self.peers if nid.startswith("VP") and "CO" not in nid])
            
        self.tasks[task_id] = {
            "tx": tx,
            "executor_id": assigned_executor,
            "votes": {"approve": 0, "reject": 0, "voters": set()},
            "reassigned": False
        }

        assignment_msg = {
            "type": "TASK_ASSIGNMENT", 
            "task_id": task_id, 
            "tx": tx, 
            "version": agreed_seq,
            "executor_id": assigned_executor,
            "verifiers": verifiers,
            "consensus_proof": consensus_proof
        }
        print(f"[{self.node_id}] Broadcasting Assignment <Task:{task_id[:8]}, Exec:{assigned_executor}> to Verifiers")
        
        for v_id in verifiers:
            await self._send_signed_message_fire_and_forget(v_id, assignment_msg)
            
        await asyncio.sleep(0.05)
        await self._send_signed_message_fire_and_forget(assigned_executor, assignment_msg)
        

            
    async def handle_task_assignment(self, msg_data):
        task_id = msg_data.get("task_id")
        tx = msg_data.get("tx")
        executor_id = msg_data.get("executor_id")
        if task_id not in self.verification_state:
            expected_size = self._calculate_expected_output_size(tx)
            
            self.verification_state[task_id] = {
                "expected_size": expected_size,
                "seen_count": 0,
                "last_record": None,
                "executor_id": executor_id
            }
            print(f"[{self.node_id}] Registered Verification context for Task {task_id}. Expecting {expected_size} records.")


    async def handle_result_chunk(self, msg_data):
        task_id = msg_data.get("task_id")
        chunk = msg_data.get("chunk", [])
        is_final = msg_data.get("is_final", False)
        sender = msg_data.get("sender_id") 
        if task_id not in self.verification_state:
            print(f"[{self.node_id}] WARNING: Received chunk for unknown/unassigned Task {task_id}.")
            return
        v_state = self.verification_state[task_id]
        if sender and sender != v_state["executor_id"]:
            print(f"[{self.node_id}] BFT ALERT: Chunk sender {sender} is not assigned executor {v_state['executor_id']}!")
            return
        is_chunk_valid = True
        for record in chunk:
            if not self._is_valid_record(record):
                print(f"[{self.node_id}] BFT ALERT: Invalid record detected in chunk!")
                is_chunk_valid = False
                break
            if v_state["last_record"] and not self._happens_before(v_state["last_record"], record):
                print(f"[{self.node_id}] BFT ALERT: Ordering violation (happensBefore failed)!")
                is_chunk_valid = False
                break
            
            v_state["last_record"] = record
            v_state["seen_count"] += 1
        if not is_chunk_valid:
            print(f"[{self.node_id}] Dropping invalid chunk for task {task_id}.")
            return
        if is_final:
            if v_state["seen_count"] == v_state["expected_size"]:
                print(f"[{self.node_id}] SUCCESS: Task {task_id} fully verified. ({v_state['seen_count']} records)") 
            else:
                print(f"[{self.node_id}] BFT ALERT: Output size mismatch for Task {task_id}! Expected {v_state['expected_size']}, got {v_state['seen_count']}.")
            del self.verification_state[task_id]
            

    async def _send_signed_message_fire_and_forget(self, target_node_id, payload_dict):
        if target_node_id not in self.peers: return
        target_info = self.peers[target_node_id]
        content_str = get_deterministic_string(payload_dict)
        signature = self.crypto.sign(content_str)
        
        final_payload = {
            "sender_id" : self.node_id,
            "content" : content_str,
            "signature" : signature
        }
        
        try:
            reader, writer = await asyncio.open_connection(
                target_info.get('ip', '127.0.0.1'), target_info['port']
            )
            writer.write(json.dumps(final_payload).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            pass
            
            
            
    async def submit_task_with_retry(self, target_nodes, args, expected_check=None):
        task_id = str(uuid.uuid4())[:8]
        
        for node_id in target_nodes:
            print(f"\n[VP_CO] Attempting to assign Task {task_id} to {node_id}...")
            
            success = await self._send_task_and_wait(node_id, task_id, args, expected_check)
            
            if success:
                print(f"[VP_CO] Task {task_id} completed successfully by {node_id}.")
                return
            else:
                print(f"[VP_CO] Task {task_id} failed on {node_id}. Re-assigning...")
        
        print(f"[VP_CO] CRITICAL: Task {task_id} failed on ALL available nodes.")
        

    async def _send_task_and_wait(self, target_node_id, task_id, payload_dict, expected_check):
        if target_node_id not in self.peers:
            return False

        target_info = self.peers[target_node_id]
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_tasks[task_id] = future
        
        content_str = get_deterministic_string(payload_dict)
        signature = self.crypto.sign(content_str)
        
        final_payload = {
            "sender_id" : self.node_id,
            "content" : content_str,
            "signature" : signature
        }
        
        try:
            reader, writer = await asyncio.open_connection(
                target_info.get('ip', '127.0.0.1'), target_info['port']
            )
            writer.write(json.dumps(final_payload).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()

            sender, result = await asyncio.wait_for(future, timeout=3.0)
            
            is_valid = (result == expected_check) if expected_check is not None else True
            
            if is_valid:
                print(f"[VP_CO] Result Verified from {sender}: {result}")
                return True
            else:
                print(f"[VP_CO] Result REJECTED: {result} != {expected_check}")
                return False

        except asyncio.TimeoutError:
            print(f"[VP_CO] Timeout waiting for {target_node_id}")
            return False
        except Exception as e:
            print(f"[VP_CO] Error communicating with {target_node_id}: {e}")
            return False
        finally:
            self.pending_tasks.pop(task_id, None)
            
    async def reassign_task(self, old_task_id):
        old_task = self.tasks.get(old_task_id)
        if not old_task:
            return
            
        original_tx = old_task.get("tx")
        bad_executor = old_task.get("executor_id")

        executors = sorted([nid for nid in self.peers if nid.startswith("EP")])
        new_executor = next((ep for ep in executors if ep != bad_executor), None)

        if not new_executor:
            print(f"[{self.node_id}] FATAL: No healthy executors left to reassign Task {old_task_id[:8]}!")
            return

        new_task_id = hashlib.sha256(f"{old_task_id}_retry".encode()).hexdigest()[:16]

        print(f"[{self.node_id}]REASSIGNING! Task {old_task_id[:8]} taken from {bad_executor} -> Given to {new_executor} (New Task ID: {new_task_id[:8]})")

        self.tasks[new_task_id] = {
            "tx": original_tx,
            "executor_id": new_executor,
            "votes": {"approve": 0, "reject": 0, "voters": set()},
            "reassigned": False
        }

        verifiers = sorted([nid for nid in self.peers if nid.startswith("VP")])

        assignment_msg = {
            "type": "TASK_ASSIGNMENT", 
            "task_id": new_task_id, 
            "tx": original_tx, 
            "version": self.seq_counter,
            "executor_id": new_executor,
            "verifiers": verifiers
        }

        for v_id in verifiers:
            await self._send_signed_message_fire_and_forget(v_id, assignment_msg)
            
        await asyncio.sleep(0.05)
        await self._send_signed_message_fire_and_forget(new_executor, assignment_msg)



            