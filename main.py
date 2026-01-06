import sys
import time
import json
import os
import grpc
from concurrent import futures

# --- PATH CONFIGURATION ---

base_dir = os.path.dirname(os.path.abspath(__file__))

# Add 'src' to path
sys.path.append(os.path.join(base_dir, "src"))

# CRITICAL FIX: Add 'src/protos' to path so the generated code works
sys.path.append(os.path.join(base_dir, "src", "protos"))

import wallet_pb2 as wallet_pb2
import wallet_pb2_grpc as wallet_pb2_grpc

from src.core.raft_node import RaftNode
from src.core.state_machine import WalletStateMachine
from src.infrastructure.service_handlers import WalletServiceHandler
from src.utils.logger import get_logger
from src.infrastructure.service_handlers import ConsensusServiceHandler

# ... rest of the code stays exactly the same ...
def load_config(node_id):
    """Reads topology.json to find my IP and Peers."""
    config_path = os.path.join("config", "topology.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError("config/topology.json not found!")
        
    with open(config_path, "r") as f:
        config = json.load(f)
    
    # Search for my node_id in the config
    for shard_name, shard_data in config["shards"].items():
        if node_id in shard_data["nodes"]:
            my_info = shard_data["nodes"][node_id]
            # Get list of peers (everyone else in my shard)
            peers = [
                f"{info['host']}:{info['port']}" 
                for nid, info in shard_data["nodes"].items() 
                if nid != node_id
            ]
            return {
                "shard_id": shard_name,
                "host": my_info["host"],
                "port": my_info["port"],
                "peers": peers
            }
            
    raise ValueError(f"Node ID '{node_id}' not found in topology.json")

def serve():
    if len(sys.argv) < 2:
        print("Usage: python main.py <node_id>")
        sys.exit(1)
        
    node_id = sys.argv[1]
    
    # 2. Load Configuration
    try:
        config = load_config(node_id)
    except Exception as e:
        print(f"Error loading config: {e}")
        return

    logger = get_logger(f"MAIN-{node_id}")
    logger.info(f"--- Starting Node {node_id} (Shard: {config['shard_id']}) ---")
    logger.info(f"Listening on {config['host']}:{config['port']}")

    # 3. Initialize Core Logic
    state_machine = WalletStateMachine()
    raft_node = RaftNode(node_id, config['shard_id'], config['peers'], state_machine)

    # 4. Start gRPC Server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Register our Services
    wallet_service = WalletServiceHandler(raft_node)
    wallet_pb2_grpc.add_WalletServiceServicer_to_server(wallet_service, server)
    
    consensus_service = ConsensusServiceHandler(raft_node)
    wallet_pb2_grpc.add_ConsensusServiceServicer_to_server(consensus_service, server)
    # ----------------------
    
    # Start Network
    server.add_insecure_port(f"{config['host']}:{config['port']}")
    server.start()
    
    logger.info("Server started successfully.")

    # 5. Keep Alive Loop
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.stop(0)

if __name__ == "__main__":
    serve()