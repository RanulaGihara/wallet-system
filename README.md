# Distributed E-Wallet System (Raft Consensus & Sharding)

This project implements a **Distributed E-Wallet System** using **gRPC** and **Python**. It demonstrates core distributed systems concepts including **Sharding (Partitioning)**, **Raft Consensus (Leader Election)**, **Fault Tolerance**, and **Strong Consistency**.

## System Architecture

- **Technology Stack:** Python 3.8+, gRPC, Protobuf.
- **Sharding Strategy:**
  - **Shard 0:** Nodes A, B, C (Handling Accounts 0-99).
  - **Shard 1:** Nodes D, E, F (Handling Accounts 100+).
- **Consensus:** Raft Algorithm (Leader Election, Log Replication).
- **Client:** Smart client with automatic leader discovery and failover retries.

---

## Setup & Installation

### 1. Prerequisites

- Python 3.8 or higher.
- `pip` (Python Package Manager).

### 2. Install Dependencies

Run the following command to install the required gRPC libraries:

```bash
pip install grpcio grpcio-tools
```

### 3. Compile Protobuf Files

```bash
# Windows (PowerShell)
python -m grpc_tools.protoc -I protos --python_out=src/protos --grpc_python_out=src/protos wallet.proto

# Mac/Linux (Terminal)
python -m grpc_tools.protoc -I protos --python_out=src/protos --grpc_python_out=src/protos wallet.proto
```

## How to Run the System

### Step 1: Start Shard 0 (The Main Cluster)

```bash
python main.py node_A
python main.py node_B
python main.py node_C

Wait a few seconds. You should see one node print !!! BECAME LEADER !!!.
```

### Step 2: Start Shard 1 (Scalability Cluster)

```bash
python main.py node_D
python main.py node_E
python main.py node_F
```

### Step 3: Run the Client

```bash
python -m src.client.client_app
```
## Testing Scenarios (Concepts Verification)

Follow these exact steps to verify each distributed system concept for your report or demo.

### 1.  Test Partitioning (Sharding)
**Goal:** Prove that data is split between different clusters based on Account ID.

1.  **Start Shard 0** (A, B, C) and **Shard 1** (D, E, F).
2.  Run the Client.
3.  **Command:** `create 10`
    * **Result:** `Success: Account created`.
    * **Observation:** Check **Node A/B/C terminal**. You will see logs for Account 10.
4.  **Command:** `create 150`
    * **Result:** `Success: Account created`.
    * **Observation:** Check **Node D/E/F terminal**. You will see logs for Account 150.
    * **Proof:** Node A (Shard 0) has zero knowledge of Account 150. Data is partitioned.



### 2.  Test Consensus (Raft Leader Election)
**Goal:** Prove that nodes automatically elect a single Leader to manage writes.

1.  **Setup:** Start Nodes A, B, C.
2.  **Observation:** Watch the logs.
    * Initially, all nodes start as **Followers**.
    * They wait for a heartbeat.
    * One node times out and starts an Election.
    * It prints: `!!! BECAME LEADER !!!`.
    * The other nodes print: `Accepted new Leader: node_X`.
3.  **Client Check:** Run `balance 10` on the client. It will confirm: `(Served by Leader: node_X)`.



### 3.  Test Replication
**Goal:** Prove that data written to the Leader is copied to Followers.

1.  **Command:** `deposit 10 100` (on Account 10).
2.  **Observation:**
    * **Leader Terminal:** Prints `Leader received command: TRANSACTION...`.
    * **Follower Terminals:** Print `Replicating: TRANSACTION...` or `Heartbeat received...`.
3.  **Proof:** If the Followers did not receive this, the next test (Fault Tolerance) would fail.



### 4.  Test Fault Tolerance (The "Kill Test")
**Goal:** Prove the system survives a server crash without losing data.

1.  **Baseline:** Ensure Account 10 has **$100.0**. Run `balance 10` to confirm. Note the current Leader (e.g., Node A).
2.  **Kill the Leader:** Go to Node A's terminal and press `Ctrl+C`. The node stops.
3.  **Wait:** Watch Node B and C. Within 5 seconds, one will print `Timeout! Starting Election` and become the new Leader.
4.  **Verify:** Run `balance 10` in the Client.
    * **Result:** `Balance for Account 10: $100.0`.
    * **Proof:** The system recovered automatically, and the money was **NOT** lost.



### 5.  Test Client Resilience
**Goal:** Prove the client automatically retries if it hits a dead node or a follower.

1.  **Setup:** Ensure the previous "Kill Test" is active (Node A is dead).
2.  **Command:** `withdraw 10 50`
3.  **Internal Process:**
    * The client tries to connect to Node A -> **Fails** (Connection Refused).
    * It catches the error and moves to Node B.
    * Node B might be a Follower -> Returns `"Not Leader"`.
    * It moves to Node C (Leader) -> **Success**.
4.  **User Experience:** The user simply sees `Success: Withdrew 50.0` without knowing about the chaos behind the scenes.

### 6.  Test Strong Consistency
**Goal:** Prove that we never read "stale" data (e.g., $0 balance) after a crash.

1.  **Setup:** Immediately after the new Leader is elected in Test 4.
2.  **Command:** `balance 10`
3.  **Result:** It **MUST** show `$100.0`.
4.  **Why:** Because the Raft protocol ensures a node cannot become a Leader unless it has all committed entries. If Node B became Leader, it *guarantees* it has the latest data.