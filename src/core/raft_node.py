import threading
import time
import random
import grpc
from enum import Enum
from src.utils.logger import get_logger

# --- IMPORTS ---
# These are critical. If these fail, the thread dies silently.
import wallet_pb2 as wallet_pb2
import wallet_pb2_grpc as wallet_pb2_grpc

class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftNode:
    def __init__(self, node_id, shard_id, peers, state_machine):
        self.node_id = node_id
        self.shard_id = shard_id
        self.peers = peers 
        self.state_machine = state_machine
        self.logger = get_logger(f"{shard_id}-{node_id}")

        # Persistent State
        self.current_term = 0
        self.voted_for = None
        self.log = [] 

        # Volatile State
        self.state = NodeState.FOLLOWER
        self.leader_id = None
        
        # Timers
        self.last_heartbeat_time = time.time()
        self.election_timeout = random.uniform(1.5, 3.0) 
        self.heartbeat_interval = 0.5
        
        self.lock = threading.Lock()
        self.running = True
        
        # Track votes
        self.votes_received = 0
        
        # Start the background loop
        threading.Thread(target=self.run_cycle, daemon=True).start()

    def run_cycle(self):
        """The main heartbeat loop."""
        self.logger.info("Raft Node Cycle Started")
        while self.running:
            try:
                time.sleep(0.1) 
                
                with self.lock:
                    current_time = time.time()
                    time_since = current_time - self.last_heartbeat_time
                    
                    # FIX: Check timeout for BOTH Follower and Candidate
                    if self.state in [NodeState.FOLLOWER, NodeState.CANDIDATE]:
                        if time_since > self.election_timeout:
                            self.logger.warning("Timeout! Leader is dead (or election failed). Starting Election.")
                            self.start_election()
                            
                    elif self.state == NodeState.LEADER:
                        if time_since > self.heartbeat_interval:
                            self.last_heartbeat_time = current_time
                            self.send_heartbeats()
                            
            except Exception as e:
                self.logger.error(f"CRASH IN RUN_CYCLE: {e}")
                time.sleep(1)

    def start_election(self):
        """Promotes self to Candidate and asks for votes."""
        # Note: We are already inside a lock from run_cycle, 
        # but we need to be careful not to deadlock if we call methods that also lock.
        # For simplicity in this assignment, we assume recursive locking or carefully structured calls.
        
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1 # Vote for self
        self.last_heartbeat_time = time.time()
        
        self.logger.info(f"Became CANDIDATE. Term: {self.current_term}")
        
        # Send RequestVote to all peers
        self.request_votes()

    def request_votes(self):
        """Spawns threads to send vote requests."""
        for peer_address in self.peers:
            threading.Thread(target=self._send_vote_request, args=(peer_address,), daemon=True).start()

    def _send_vote_request(self, peer_address):
        """The actual network call."""
        try:
            # self.logger.info(f"Sending Vote Request to {peer_address}") # Uncomment to debug
            channel = grpc.insecure_channel(peer_address)
            stub = wallet_pb2_grpc.ConsensusServiceStub(channel)
            
            request = wallet_pb2.VoteRequest(
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=0, 
                last_log_term=0
            )
            
            # Timeout is crucial so we don't hang
            response = stub.RequestVote(request, timeout=0.5)
            
            with self.lock:
                if response.term > self.current_term:
                    self.step_down(response.term)
                    return
                
                if self.state == NodeState.CANDIDATE and response.vote_granted:
                    self.votes_received += 1
                    self.logger.info(f"Vote received from {peer_address}. Total: {self.votes_received}")
                    
                    # Check for Majority
                    # (len(peers) + 1) because peers list doesn't include self
                    total_nodes = len(self.peers) + 1
                    if self.votes_received > total_nodes // 2:
                        self.become_leader()
                        
        except grpc.RpcError as e:
            # This is normal if other nodes aren't up yet
            # self.logger.warning(f"Failed to connect to {peer_address}") 
            pass
        except Exception as e:
            self.logger.error(f"Error in _send_vote_request: {e}")

    def handle_vote_request(self, term, candidate_id):
        """Logic to decide: Do I vote for this guy?"""
        with self.lock:
            if term < self.current_term:
                return self.current_term, False
            
            if term > self.current_term:
                self.current_term = term
                self.state = NodeState.FOLLOWER
                self.voted_for = None
                self.leader_id = None
            
            if self.voted_for is None or self.voted_for == candidate_id:
                self.voted_for = candidate_id
                self.last_heartbeat_time = time.time()
                self.logger.info(f"Voted for {candidate_id}")
                return self.current_term, True
                
            return self.current_term, False

    def become_leader(self):
        if self.state != NodeState.LEADER:
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            self.logger.info(f"!!! BECAME LEADER (Term {self.current_term}) !!!")
            self.send_heartbeats()

 # 1. ADD THIS METHOD: Sending the heartbeat network call
    def send_heartbeats(self):
        """Spawns threads to send heartbeats to all peers."""
        for peer_address in self.peers:
            threading.Thread(target=self._send_heartbeat, args=(peer_address,), daemon=True).start()

    # 2. ADD THIS METHOD: The actual gRPC call
    def _send_heartbeat(self, peer_address):
        try:
            channel = grpc.insecure_channel(peer_address)
            stub = wallet_pb2_grpc.ConsensusServiceStub(channel)
            
            request = wallet_pb2.AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                # For now, empty entries act as a heartbeat
                entries=[], 
                leader_commit=0
            )
            
            # fast timeout for heartbeats
            stub.AppendEntries(request, timeout=0.2) 
            
        except Exception:
            # It's okay if a peer is down, we just keep trying
            pass

    # 3. ADD THIS METHOD: Handling incoming heartbeats (Resetting the timer)
    def handle_heartbeat(self, term, leader_id):
        with self.lock:
            # If the leader's term is older, ignore him
            if term < self.current_term:
                return self.current_term, False
            
            # A valid leader is talking to us!
            self.current_term = term
            self.leader_id = leader_id
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            
            # CRITICAL: Reset the timer so we don't start a revolution
            self.last_heartbeat_time = time.time()
            
            # self.logger.info(f"Heartbeat received from {leader_id}") # Uncomment for verbose logs
            return self.current_term, True

    def step_down(self, new_term):
        self.current_term = new_term
        self.state = NodeState.FOLLOWER
        self.voted_for = None
        self.leader_id = None
        self.logger.info(f"Stepping down. New Term: {new_term}")