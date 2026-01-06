import grpc
import sys
import os

# Fix path to import protos
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src/protos'))

import wallet_pb2
import wallet_pb2_grpc

# --- PARTITION RESOLUTION CONFIG ---
SHARD_0_NODES = ["localhost:50051", "localhost:50052", "localhost:50053"]
SHARD_1_NODES = ["localhost:50054", "localhost:50055", "localhost:50056"]

def get_shard_nodes(account_id):
    """
    Decides which shard holds the data based on Account ID.
    """
    if account_id < 100:
        return SHARD_0_NODES
    else:
        return SHARD_1_NODES

def create_account(acc_id):
    """
    ROBUST VERSION: Retries all nodes until the Leader is found.
    """
    nodes = get_shard_nodes(acc_id)
    
    for address in nodes:
        channel = grpc.insecure_channel(address)
        stub = wallet_pb2_grpc.WalletServiceStub(channel)
        
        try:
            response = stub.CreateAccount(wallet_pb2.AccountRequest(account_id=acc_id))
            
            if response.success:
                print(f" Success: {response.message}")
                channel.close()
                return
            elif "Not Leader" in response.message:
                # Connected to a Follower, try next node
                channel.close()
                continue
            else:
                print(f" Failed: {response.message}")
                channel.close()
                return

        except grpc.RpcError:
            # Node is down, try next one
            channel.close()
            continue
            
    print(" Error: Could not create account. Cluster unavailable.")

def get_balance(acc_id):
    """
    ROBUST VERSION: Retries all nodes until the Leader is found.
    """
    nodes = get_shard_nodes(acc_id)
    
    for address in nodes:
        channel = grpc.insecure_channel(address)
        stub = wallet_pb2_grpc.WalletServiceStub(channel)
        
        try:
            response = stub.GetBalance(wallet_pb2.AccountRequest(account_id=acc_id))
            
            if response.success:
                print(f" Balance for Account {acc_id}: ${response.balance}")
                print(f"   (Served by Leader: {response.leader_id})")
                channel.close()
                return
            else:
                print(f" Failed: Account not found.")
                channel.close()
                return # If account missing, don't retry others (unless eventual consistency)
                
        except grpc.RpcError:
            # Node is down, try next one
            channel.close()
            continue
            
    print(" Error: Could not fetch balance. Cluster unavailable.")

def execute_transaction(user_id, amount, operation):
    """
    ROBUST VERSION: Retries all nodes until the Leader is found.
    """
    nodes = get_shard_nodes(user_id)
    
    for address in nodes:
        channel = grpc.insecure_channel(address)
        stub = wallet_pb2_grpc.WalletServiceStub(channel)
        
        try:
            req = wallet_pb2.TransactionRequest(
                account_id=user_id,
                amount=amount,
                op=operation
            )
            response = stub.ExecuteTransaction(req)
            
            if response.success:
                print(f" Success: {response.message}")
                channel.close()
                return
            elif "Not Leader" in response.message:
                channel.close()
                continue
            else:
                print(f" Failed: {response.message}")
                channel.close()
                return

        except grpc.RpcError:
            channel.close()
            continue
            
    print(" Error: Could not execute transaction. Cluster unavailable.")

def main():
    print("--- Distributed E-Wallet Client ---")
    print("Commands: create <id> | balance <id> | deposit <id> <amt> | withdraw <id> <amt> | exit")
    
    while True:
        try:
            cmd = input("\nEnter command: ").strip().split()
            if not cmd: continue
            
            if cmd[0] == "exit": break
            
            elif cmd[0] == "create":
                if len(cmd) < 2: print("Usage: create <id>"); continue
                create_account(int(cmd[1]))
                
            elif cmd[0] == "balance":
                if len(cmd) < 2: print("Usage: balance <id>"); continue
                get_balance(int(cmd[1]))
                
            elif cmd[0] == "deposit":
                if len(cmd) < 3: print("Usage: deposit <id> <amount>"); continue
                execute_transaction(int(cmd[1]), float(cmd[2]), "DEPOSIT")
                
            elif cmd[0] == "withdraw":
                if len(cmd) < 3: print("Usage: withdraw <id> <amount>"); continue
                execute_transaction(int(cmd[1]), float(cmd[2]), "WITHDRAW")
                
            else:
                print("Unknown command.")
        except ValueError:
            print(" Invalid input format.")
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()