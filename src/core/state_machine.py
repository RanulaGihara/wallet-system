import threading

class WalletStateMachine:
    def __init__(self):
        # In-memory database: {account_id (int): balance (float)}
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
        Executes a command 
        """
        with self.lock:
            try:
                if not isinstance(command, str):
                    command = str(command)

                parts = command.split()
                if not parts:
                    return False, "Empty Command"

                op_type = parts[0]
                
               
                if op_type == "CREATE":
                    acc_id = int(parts[1])
                    if acc_id in self.accounts:
                        return False, "Account already exists"
                    self.accounts[acc_id] = 0.0
                    return True, "Account created"
                
                
                elif op_type == "TRANSACTION":

                    user_id = int(parts[1])
                    amount = float(parts[2])
                    operation = parts[3]
                    
                    if user_id not in self.accounts:
                        return False, f"User {user_id} does not exist"
                    
                   
                    if operation == "DEPOSIT":
                        self.accounts[user_id] += amount
                        return True, f"Deposited {amount}"
                    
                    # -> WITHDRAW
                    elif operation == "WITHDRAW":
                        if self.accounts[user_id] >= amount:
                            self.accounts[user_id] -= amount
                            return True, f"Withdrew {amount}"
                        else:
                            return False, "Insufficient funds"
                            
                  
                    elif operation == "TRANSFER":
                        
                        if len(parts) < 5:
                            return False, "Missing receiver ID for transfer"
                            
                        to_id = int(parts[4])
                        
                        if to_id not in self.accounts:
                            return False, f"Receiver {to_id} does not exist"
                        
                       
                        if self.accounts[user_id] >= amount:
                            self.accounts[user_id] -= amount
                            self.accounts[to_id] += amount
                            return True, f"Transferred {amount} from {user_id} to {to_id}"
                        else:
                            return False, "Insufficient funds for transfer"

                return False, "Unknown Command"
                
            except Exception as e:
               
                print(f"State Machine Error: {e}")
                return False, f"Execution Error: {str(e)}"