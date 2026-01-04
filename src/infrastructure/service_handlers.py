import grpc

# UPDATED IMPORTS:
# Since we added 'src/protos' to the path in main.py, we import directly
import wallet_pb2 as wallet_pb2
import wallet_pb2_grpc as wallet_pb2_grpc

class WalletServiceHandler(wallet_pb2_grpc.WalletServiceServicer):
    # ... rest of the code stays the same ...
    """
    This class handles requests from the CLIENT.
    It acts as the 'API Layer'.
    """
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def CreateAccount(self, request, context):
        # In a real Raft, we send this command to the log.
        # For Phase 1, we just talk to the State Machine directly to test.
        success, msg = self.raft_node.state_machine.apply_log(f"CREATE {request.account_id}")
        return wallet_pb2.AccountResponse(success=success, message=msg)

    def GetBalance(self, request, context):
        # Strong Consistency: Only Leader should answer (Read-Your-Writes)
        # Partition Resolution: If not leader, tell client who is.
        if self.raft_node.state != self.raft_node.state.LEADER:
            # We will implement redirect logic later. For now, just warn.
            pass
            
        balance = self.raft_node.state_machine.get_balance(request.account_id)
        return wallet_pb2.BalanceResponse(
            account_id=request.account_id,
            balance=balance,
            success=True,
            leader_id=str(self.raft_node.node_id)
        )

class ConsensusServiceHandler(wallet_pb2_grpc.ConsensusServiceServicer):
    """
    Handles Raft Consensus RPCs: RequestVote and AppendEntries.
    """
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def RequestVote(self, request, context):
        # Delegate logic to the Raft Node
        term, vote_granted = self.raft_node.handle_vote_request(
            request.term, 
            request.candidate_id
        )
        return wallet_pb2.VoteResponse(term=term, vote_granted=vote_granted)

    def AppendEntries(self, request, context):
        # Forward heartbeat to Raft Node
        term, success = self.raft_node.handle_heartbeat(
            request.term,
            request.leader_id
        )
        return wallet_pb2.AppendEntriesResponse(term=term, success=success)