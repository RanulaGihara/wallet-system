import threading

class WalletStateMachine:
    def __init__(self):
        self.accounts = {}
        self.lock = threading.Lock()
        
    def get_balance(self, account_id):
        """
        Safely gets the balance, ensuring the ID is an integer.
        """
        with self.lock:
            try:
                acc_id_int = int(account_id)
                return self.accounts.get(acc_id_int, 0.0)
            except ValueError:
                return 0.0

    def apply_log(self, command):
        """
        Executes a command.
        """
        with self.lock:
            try:
                parts = command.split()
                op = parts[0]
                
                if op == "CREATE":
                    acc_id = int(parts[1])
                    if acc_id in self.accounts:
                        return False, "Account already exists"
                    self.accounts[acc_id] = 0.0
                    return True, "Account created"
                
                elif op == "TRANSACTION":
                    user_id = int(parts[1])
                    amount = float(parts[2])
                    operation = parts[3]
                    
                    if user_id not in self.accounts:
                        return False, "User does not exist"
                    
                    if operation == "DEPOSIT":
                        self.accounts[user_id] += amount
                        return True, f"Deposited {amount}"
                    
                    elif operation == "WITHDRAW":
                        if self.accounts[user_id] >= amount:
                            self.accounts[user_id] -= amount
                            return True, f"Withdrew {amount}"
                        else:
                            return False, "Insufficient funds"

                return False, "Unknown Command"
                
            except Exception as e:
                return False, f"Execution Error: {str(e)}"
        """
        Executes a command committed by Raft.
        Handles both STRING commands (Simple) and DICT commands (Complex).
        """
        with self.lock:
            try:
                if isinstance(command, str):
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

                elif isinstance(command, dict):
                    op_type = command.get("type")
                    if op_type == "TRANSACTION":
                        user_id = int(command["user_id"])
                        amount = float(command["amount"])
                        operation = command["op"]
                        
                        if user_id not in self.accounts:
                            return False, "User does not exist"
                        
                        if operation == "DEPOSIT":
                            self.accounts[user_id] += amount
                            return True, f"Deposited {amount}"
                        elif operation == "WITHDRAW":
                            if self.accounts[user_id] >= amount:
                                self.accounts[user_id] -= amount
                                return True, f"Withdrew {amount}"
                            else:
                                return False, "Insufficient funds"

                return False, "Unknown Command Format"
                
            except Exception as e:
                print(f"State Machine Error: {e}")
                return False, f"Execution Error: {str(e)}"