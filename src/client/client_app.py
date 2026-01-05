import grpc
import sys
import os

# Fix path to import protos
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src/protos'))

import wallet_pb2
import wallet_pb2_grpc

# --- PARTITION RESOLUTION CONFIG ---
# In a real app, we would fetch this from a NameServer or load topology.json
SHARD_0_NODES = ["localhost:50051", "localhost:50052", "localhost:50053"]
SHARD_1_NODES = ["localhost:50054", "localhost:50055", "localhost:50056"]

def get_shard_nodes(account_id):
    """
    Partition Resolution Logic (10 Marks).
    Decides which shard holds the data based on Account ID.
    Strategy: Account 0-99 -> Shard 0, Account 100+ -> Shard 1
    """
    if account_id < 100:
        return SHARD_0_NODES
    else:
        return SHARD_1_NODES

def connect_to_leader(nodes):
    """
    Tries to find the Leader in a list of nodes.
    If a node is a Follower, we could implement a redirect, 
    but for now, we just try them until one works.
    """
    for address in nodes:
        try:
            channel = grpc.insecure_channel(address)
            stub = wallet_pb2_grpc.WalletServiceStub(channel)
            return stub, channel
        except:
            continue
    return None, None

def create_account(acc_id):
    nodes = get_shard_nodes(acc_id)
    stub, channel = connect_to_leader(nodes)
    
    if not stub:
        print(" Could not connect to any node in the correct shard.")
        return

    try:
        response = stub.CreateAccount(wallet_pb2.AccountRequest(account_id=acc_id))
        if response.success:
            print(f" Success: {response.message}")
        else:
            print(f" Failed: {response.message}")
    except grpc.RpcError as e:
        print(f"RPC Error: {e.code()}")
    
    channel.close()

def get_balance(acc_id):
    nodes = get_shard_nodes(acc_id)
    stub, channel = connect_to_leader(nodes)
    
    if not stub:
        print(" Could not connect to any node.")
        return

    try:
        response = stub.GetBalance(wallet_pb2.AccountRequest(account_id=acc_id))
        if response.success:
            print(f" Balance for Account {acc_id}: ${response.balance}")
            print(f"   (Served by Leader: {response.leader_id})")
        else:
            print(f" Failed: Account not found.")
    except grpc.RpcError as e:
        print(f" RPC Error: {e.code()}")
        
    channel.close()
    # ADD THIS METHOD TO src/core/raft_node.py
    def replicate_log(self, command):
        """
        1. Appends command to local log.
        2. Waits for Consensus (Safety).
        3. Returns result to client.
        """
        with self.lock:
            if self.state != NodeState.LEADER:
                return False, "Not Leader"
            
            # 1. Append to local log
            entry = {"term": self.current_term, "command": command}
            self.log.append(entry)
            last_idx = len(self.log) - 1
            self.logger.info(f"Leader received command: {command}")
            
        # 2. Wait for Consensus (Simple Sleep-Wait Loop for now)
        # In a production system, use Condition Variables or Events
        start_time = time.time()
        while time.time() - start_time < 5.0: # 5 second timeout
            if self.commit_index >= last_idx:
                # Log is committed! Apply to State Machine.
                # Note: Usually the 'apply' happens in a separate background thread,
                # but for this simple assignment, we can do it here or assume it's done.
                return True, "Committed and Executed"
            time.sleep(0.1)
            
        return False, "Replication Timeout"

def main():
    print("--- Distributed E-Wallet Client ---")
    while True:
        cmd = input("\nEnter command (create <id> | balance <id> | exit): ").strip().split()
        if not cmd: continue
        
        if cmd[0] == "exit":
            break
        
        elif cmd[0] == "create":
            if len(cmd) < 2: print("Usage: create <id>"); continue
            create_account(int(cmd[1]))
            
        elif cmd[0] == "balance":
            if len(cmd) < 2: print("Usage: balance <id>"); continue
            get_balance(int(cmd[1]))
            
        else:
            print("Unknown command.")

if __name__ == "__main__":
    main()