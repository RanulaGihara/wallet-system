class WalletStateMachine:
    def __init__(self):
        # The database: { account_id (int): balance (float) }
        self.accounts = {}
        
    def get_balance(self, account_id):
        return self.accounts.get(account_id, 0.0)

    def apply_log(self, command):
        """
        Executes a command committed by Raft.
        Format: "OP ARG1 ARG2"
        Example: "DEPOSIT 101 50.0"
        """
        parts = command.split()
        op = parts[0]
        
        if op == "CREATE":
            acc_id = int(parts[1])
            if acc_id in self.accounts:
                return False, "Account already exists"
            self.accounts[acc_id] = 0.0
            return True, "Account created"
            
        elif op == "DEPOSIT":
            acc_id = int(parts[1])
            amount = float(parts[2])
            if acc_id not in self.accounts:
                return False, "Account does not exist"
            self.accounts[acc_id] += amount
            return True, f"Deposited {amount}"
            
        # We will add TRANSFER logic here later
        return False, "Unknown Command"