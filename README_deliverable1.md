# EDGEFLOW — SDN-Orchestrated Distributed IoT Sensor Analytics

**Computer Networks Mini Project — Deliverable 1**

---

## Project Overview

**EDGEFLOW** is a distributed edge-computing framework designed to offload IoT sensor analytics processing from resource-constrained gateways to distributed edge worker nodes over persistent TCP sockets. Network traffic between the central Task Manager and edge worker nodes is dynamically prioritized at Layer 4 by a POX Software-Defined Networking (SDN) controller enforcing high-priority OpenFlow rules across an Open vSwitch instance.

---

## System Architecture & Topology

The system separates control plane decision-making from data plane packet forwarding:

* **Control Plane:** Remote POX SDN Controller (`c0` at `127.0.0.1:6633`) running `task_sdn.py`.
* **Data Plane:** Open vSwitch (`s1`) interconnecting virtual host nodes in a Mininet star topology.
* **Host Nodes (`10.0.0.0/24` Subnet):**
  * **`h1` (`10.0.0.1`):** Task Manager hosting the Flask Web Dashboard (Port 5000) and TCP Socket Server (Port 9090).
  * **`h2` (`10.0.0.2`):** Edge Worker 1 running `worker.py`.
  * **`h3` (`10.0.0.3`):** Edge Worker 2 running `worker.py`.

```
                 +---------------------------------+
                 |     POX SDN Controller (c0)     |
                 |      127.0.0.1 : TCP 6633       |
                 +----------------+----------------+
                                  |
                        OpenFlow Control Path
                                  |
                        +---------+---------+
                        |  Open vSwitch (s1)|
                        +----+----+----+----+
                             |    |    |
          +------------------+    |    +------------------+
          |                       |                       |
+---------+---------+   +---------+---------+   +---------+---------+
| Task Manager (h1) |   | Edge Worker 1(h2) |   | Edge Worker 2(h3) |
|     10.0.0.1      |   |     10.0.0.2      |   |     10.0.0.3      |
| Flask: Port 5000  |   | Persistent Socket |   | Persistent Socket |
| Server: Port 9090 |   |  to 10.0.0.1:9090 |   |  to 10.0.0.1:9090 |
+-------------------+   +-------------------+   +-------------------+
```

---

## File Map & Directory Structure

* **`topology.py`**: Defines the Mininet network topology, creates `h1`–`h3` and switch `s1`, sets up NAT bridging, and launches `manager.py` and `worker.py` daemons.
* **`manager.py`**: Multithreaded Task Orchestrator hosting the Flask web dashboard on Port 5000 and the TCP socket server on Port 9090.
* **`worker.py`**: Edge worker daemon that connects persistently to `10.0.0.1:9090`, responds to heartbeat `PING`s, and executes analytics operations.
* **`task_sdn.py`**: POX controller module performing Layer-4 packet inspection to install **Priority 100** flow rules for TCP Port 9090 traffic.
* **`index.html`**: Web UI displaying real-time worker liveness, task dispatching controls, system metrics, and execution result logs.

---

## Network & Port Configuration

| Service / Node | IP Address | Port | Protocol / Function |
| :--- | :--- | :--- | :--- |
| **POX Controller (`c0`)** | `127.0.0.1` | `6633` | OpenFlow Control Channel |
| **Task Manager (`h1`)** | `10.0.0.1` | `5000` | Flask Web UI Dashboard |
| **Task Manager (`h1`)** | `10.0.0.1` | `9090` | Persistent TCP Socket Orchestrator |
| **Edge Worker 1 (`h2`)** | `10.0.0.2` | Dynamic | TCP Socket Client (`Worker_1`) |
| **Edge Worker 2 (`h3`)** | `10.0.0.3` | Dynamic | TCP Socket Client (`Worker_2`) |

---

## Supported IoT Analytics Workloads

| Task Type | Description | Sample Parameter Input |
| :--- | :--- | :--- |
| **`AVERAGE`** | Calculates arithmetic mean across a batch of sensor readings | `22.5,23.1,22.8` |
| **`PEAK`** | Identifies maximum reading in dataset | `22.5,23.1,22.8` |
| **`MIN`** | Identifies minimum reading in dataset | `22.5,23.1,22.8` |
| **`POWER`** | Estimates signal power (val³) | `4` |
| **`ANOMALY`** | Evaluates reading against threshold (`threshold,value`) | `25,42` |
| **`ALERT`** | Broadcasts system alert message | `Zone-3 temperature spike detected` |

---

## Prerequisites & Dependencies

* **Operating System:** Ubuntu Linux 20.04 / 22.04 LTS (or virtual machine environment like UTM / VirtualBox)
* **Core Dependencies:**
  * Mininet (`sudo apt install mininet openvswitch-switch`)
  * POX Controller (`python3.8` compatible)
  * Python 3 & Flask (`pip install flask`)

---

## Execution Instructions

### Step 0: Clean Stale Mininet State
Clear existing locks, virtual interfaces, and background processes before launching:
```bash
sudo mn -c
```

### Step 1: Start POX SDN Controller
In Terminal 1, navigate to your POX directory and run `task_sdn.py`:
```bash
python3.8 ./pox.py log.level --DEBUG task_sdn
```

### Step 2: Start Mininet Topology & Application Daemons
In Terminal 2, run the topology script:
```bash
sudo python3 topology.py
```
*`topology.py` automatically initializes switch `s1`, hosts `h1`–`h3`, starts `manager.py` on `h1`, and connects `worker.py` on `h2` and `h3`.*

### Step 3: Access Web Control Panel
Open your browser and navigate to:
```
http://10.0.0.1:5000
```
Dispatch IoT sensor tasks via the UI to observe real-time execution in the **Task Results Log**.

---

## Technical Highlights

1. **Persistent TCP Socket Architecture:** Workers connect once on startup and maintain an open TCP socket, eliminating repeated connection overhead.
2. **Heartbeat Liveness Engine:** Idle connections use a periodic `PING`/`PONG` message exchange to verify worker status.
3. **SDN Layer-4 Traffic Prioritization:** POX inspects Layer-4 headers and installs **Priority 100** flow rules on switch `s1` for TCP Port 9090 orchestration traffic.
