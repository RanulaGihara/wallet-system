import grpc
import wallet_pb2
import wallet_pb2_grpc

class WalletServiceHandler(wallet_pb2_grpc.WalletServiceServicer):
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def CreateAccount(self, request, context):
        """
        Now correctly calls replicate_log which exists in RaftNode.
        """
        command = f"CREATE {request.account_id}"
        
        success, msg = self.raft_node.replicate_log(command)
        
        return wallet_pb2.AccountResponse(success=success, message=str(msg))

    def GetBalance(self, request, context):
       
        if str(self.raft_node.state.name) != "LEADER":
            pass 

        balance = self.raft_node.state_machine.get_balance(request.account_id)
        return wallet_pb2.BalanceResponse(
            account_id=request.account_id,
            balance=balance,
            success=True,
            leader_id=str(self.raft_node.node_id)
        )

    def ExecuteTransaction(self, request, context):
        """
        Handles Deposit and Withdraw commands.
        """
        command = f"TRANSACTION {request.account_id} {request.amount} {request.op}"
        
        # Replicate to Raft Log
        success, msg = self.raft_node.replicate_log(command)
        
        return wallet_pb2.TransactionResponse(success=success, message=str(msg))


class ConsensusServiceHandler(wallet_pb2_grpc.ConsensusServiceServicer):
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def RequestVote(self, request, context):
        term, vote_granted = self.raft_node.handle_vote_request(
            request.term, 
            request.candidate_id,
            request.last_log_index,
            request.last_log_term
        )
        return wallet_pb2.VoteResponse(term=term, vote_granted=vote_granted)

    def AppendEntries(self, request, context):
        term, success = self.raft_node.handle_heartbeat(
            request.term,
            request.leader_id,
            request.entries,
            request.leader_commit
        )
        return wallet_pb2.AppendEntriesResponse(term=term, success=success)