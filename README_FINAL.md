# EdgeFlow - SDN-orchestrated distributed IoT sensor analytics (Deliverable 2)

Distributed Task Notification and Worker System. Edge workers receive sensor-analytics tasks and return results over
**persistent TCP sockets** inside a **Mininet** network. A **POX SDN controller** watches the network and the application and
reacts to link failures, congestion and unauthorized hosts. The manager schedules tasks with either a static baseline
(`round_robin`) or a workload-aware policy that uses the controller's feedback (`sdn_aware`).

**Use case:** a factory floor with temperature and vibration sensors. Edge workers analyse the readings close to the machines
(averages, peaks, anomaly checks) and the SDN controller keeps that processing reliable when a node or link fails, a rogue device
appears, or unrelated traffic congests a worker.

**Repository:** https://github.com/ryanprabhukumar-rgb/Computer-Networks-Mini-Project

```
   h1  Task Manager  (TCP 9090, dashboard :5000)        POX controller (task_sdn.py)
        |   persistent TCP: REGISTER / TASK / RESULT / PING        ^   OpenFlow
        |                                                         |
        +----------------------  s1  (Open vSwitch)  -------------+
                     |        |        |        |        |
                    h2       h3       h4       h5       h6
                 Worker_1  Worker_2  Worker_3  (spare)  intruder
```

## 1. Files (copy all of them into one folder, e.g. ~/d2, in the VM)

| File | Role | Lines |
|---|---|---|
| `manager.py` | Task manager: TCP server, scheduler, timeouts, failure detection, dashboard server, scenario buttons | 473 |
| `worker.py` | Worker node: runs tasks, answers heartbeats, accepts delay / hang commands for the demos | 150 |
| `task_sdn.py` | POX controller: forwarding, monitoring, link-failure and congestion handling, host blocking | 226 |
| `topology.py` | Mininet: 6 hosts + 1 switch, 10 Mbit/s links, starts the manager and the 3 workers | 61 |
| `index.html` | Dashboard (plain HTML + JavaScript, no libraries) | 196 |
| `evaluate.py` | Baseline vs SDN-aware comparison, started by the **Run evaluation** button | 68 |
| `rogue.py` | Intruder script, run inside h6 by the **Unauthorized access** button | 27 |

## 2. Run

Prerequisites (once): `sudo apt install python3-flask`; Mininet, Open vSwitch and POX installed; `which mnexec` prints a path
(it ships with Mininet and is used by the scenario buttons).

```
# terminal 1 - controller (free port 6633 first:  sudo ss -ltnp | grep 6633  then  sudo kill <pid>)
cd ~/pox && PYTHONPATH=$HOME/d2 python3 pox.py task_sdn

# terminal 2 - network (also starts the manager and 3 workers)
sudo mn -c
cd ~/d2 && sudo python3 topology.py
```

Open **http://10.0.0.1:5000** in the VM's browser. Start POX before the topology, run `topology.py` from inside the project
folder, and restart POX before every new run (it remembers blocked hosts until it restarts).
Logs: `h1 tail -f /tmp/manager.log`, `h2 tail /tmp/worker1.log`. Flow table: `sh ovs-ofctl dump-flows s1` at the `mininet>` prompt.

## 3. The dashboard

| Part | What it shows |
|---|---|
| Architecture strip | Manager and active policy, "POX controller: connected / silent", state of each worker |
| Dispatch row | Task type, parameters, number of copies, target (Auto-assign or one worker) |
| Policy + scenario row | Policy selector, worker selector, **Worker failure**, **Link failure**, **Restore link**, **Unauthorized access (h6)**, **Reset** |
| Workers table | State (online / failed / link down / congested), load, tasks done, timeouts, how the last failure was detected |
| Live timeline | Every protocol and SDN event, coloured by kind (TCP, SCHED, SDN, FAULT, SYS) |
| Latest tasks | State, worker (and "was Worker_1" when a task was moved), result |
| Performance evaluation | **Run evaluation** button, results table, one summary line per condition |

A task pinned to one worker whose worker is offline is moved to another worker (timeline: "Worker_1 is offline: task N redirected
to another worker"). That is task redistribution, not an error.

## 3a. Task types

`AVERAGE`, `PEAK`, `MIN` (comma-separated readings), `POWER` (one number, cubed), `ANOMALY` (threshold,value),
`ALERT` (a message). Bad input is rejected with a message; a task whose numbers do not fit its type (for example `ANOMALY:1`) comes
back as a worker error and is marked FAILED without retrying.

## 4. How it works

1. **Register** - a worker connects to TCP 9090 and sends `REGISTER: Worker_1|token`. A wrong id or token gets `REJECT`.
2. **Assign** - the scheduler picks a worker. `round_robin` hands tasks out in turn, ignoring load and network state.
   `sdn_aware` skips workers the controller reports DOWN, avoids CONGESTED ones, keeps at most 2 tasks queued per worker and gives
   the next task to the worker with the fewest outstanding tasks.
3. **Execute** - the worker sends `START` then `RESULT`. The 8-second task timeout runs only while a task is being processed.
4. **Monitor** - the manager sends `PING` every second. No `PONG` for 5 seconds, a socket error, or a link-down report from the
   controller marks the worker failed. Its tasks return to the queue and are redistributed (a task is abandoned after 3 attempts).
5. **SDN link** - manager to controller: small UDP events (LOAD, REG, DOWN, UNAUTH, HELLO) to the virtual address 10.0.0.100:9999,
   which the controller answers at the switch. Controller to manager: the controller writes each worker's network state to
   `/tmp/edgeflow_net.json` and the manager reads it several times a second (both run on the same VM, so a shared file is the
   simplest feedback channel; a real deployment would use a socket).

Protocol (TCP 9090, one message per line): `REGISTER`, `ACK` / `REJECT`, `TASK:<id>|<type>:<params>`, `START:<id>`,
`RESULT:<id>|<text>` / `ERROR:<id>|<text>`, `PING` / `PONG`, and `CMD:DELAY|HANG <seconds>` for fault injection.

## 5. What the SDN controller does (task_sdn.py)

| Behaviour | Trigger | Action |
|---|---|---|
| Forwarding and **prioritization** | packet-in | learning switch; TCP 9090 task traffic gets priority-100 rules, other traffic priority 10 |
| **Monitoring** | every 2 s, plus manager events | reads flow statistics (traffic per worker) and the workload the manager reports |
| **Link-failure handling** | OpenFlow port-down event | marks the worker DOWN, removes its flows, tells the manager at once |
| **Recovery** | port up, or the worker registers again | clears the state, the worker rejoins |
| **Blocking** | manager rejects a host (wrong token) | drops TCP 9090 from that host (priority 200) |
| **Congestion mitigation** | non-task traffic into a worker above a threshold | marks it CONGESTED and drops the heaviest offending flow for 20 s |
| Static baseline mode | manager runs `round_robin` | controller only forwards, so the comparison is fair |

## 6. Requirements and where they are

| Problem statement / rubric | Where |
|---|---|
| TCP task communication, register workers, assign tasks | `manager.py` (register, pick_worker, schedule), `worker.py` |
| Track worker workload | per-worker load on the dashboard, LOAD events to the controller |
| Handle task timeouts | `monitor_loop` (8 s limit) then `requeue` |
| Detect worker failures, redistribute tasks | heartbeat in `monitor_loop`, `fail_worker`, link-down report from the controller |
| SDN manages communication based on workload | `task_sdn.py` monitoring and mitigation; `pick_worker` uses the controller's state |
| **Complete application functionality** (4 marks) | all rows above; steps 1-2 of the demo |
| **Dynamic SDN functionality** (6) | monitoring, link-failure reaction, recovery, host blocking, prioritization, workload-aware load balancing |
| **Challenging scenario / robustness** (4) | worker failure, link failure, unauthorized access (steps 3-5 below) |
| **Performance evaluation and comparison** (5) | static baseline vs SDN-aware, two conditions, four metrics (section 8) |
| Documentation, demo, viva (6) | this file, the dashboard timeline, section 9 |

## 7. Demo steps (everything from the dashboard)

Before: POX connected, 3 workers online, policy **sdn_aware**, click **Reset**. Keep the POX terminal visible.

1. **Normal operation** - Auto-assign, count 10, **Dispatch**. Tasks complete and every worker's Done count rises. Optional: `sh ovs-ofctl dump-flows s1` shows priority-100 rules for TCP 9090.
2. **Pinned task** - target "Worker_2 only", dispatch 4: all run on Worker_2.
3. **Worker failure** - pick Worker_3, click **Worker failure**, then dispatch 6 tasks. After about 5 s: red badge, "FAILED (no heartbeat for 5s); N task(s) redistributed", all tasks still finish; the worker returns after roughly 10-15 s.
4. **Link failure and recovery** - pick Worker_1, dispatch 10, click **Link failure**. POX prints `[SDN FAILURE] port 2 went DOWN`; the timeline shows "controller reports Worker_1: OK -> DOWN" and "FAILED (SDN controller reported its link down)"; the Last failure column says `sdn`. Click **Restore link**: POX prints `[SDN RECOVERY]` and the worker rejoins within about 10-20 s.
5. **Unauthorized access** (last - the block lasts until POX restarts) - click **Unauthorized access (h6)**. After about 15 s the timeline shows attempt 1 "REJECT: bad token", "controller BLOCKED 10.0.0.6", then attempts 2 and 3 "no answer (dropped by the network)"; POX prints `[SDN BLOCK]`; `sh ovs-ofctl dump-flows s1` shows a priority=200 drop rule.
6. **Evaluation** - click **Run evaluation** (section 8) and leave the other controls alone until it finishes.

How the buttons work: `topology.py` saves each host's process id in `/tmp/edgeflow_hosts.json`; the manager then uses Mininet's `mnexec` to run
`ip link set s1-eth2 down|up` in the root namespace (the same thing `link s1 h2 down` does, so the switch reports a real port failure) and to run
`rogue.py` inside h6. Worker failure sends a `CMD:HANG 10` to the worker, which then ignores all traffic for 10 seconds.


7. **Task timeout (terminal)** - a slow task is different from a dead worker: the worker still answers heartbeats, but one task runs longer than 8 s. At the `mininet>` prompt:
   `h1 curl -s -X POST localhost:5000/api/fault -H 'Content-Type: application/json' -d '{"worker":"Worker_1","action":"delay","seconds":10}'`,
   then on the dashboard dispatch 1 task with target "Worker_1 only". After 8 s the timeline shows `task N TIMED OUT on Worker_1 after 8s -> redistributing`; the task finishes on a different worker (shown as "was Worker_1") and the late result from Worker_1 is discarded.
   Restore normal speed with the same command and `"seconds":0`.

## 8. Performance evaluation

Click **Run evaluation**. The same 12 tasks run under the static baseline (`round_robin`) and the proposed approach (`sdn_aware`) in two conditions:

- **normal** - all workers equally fast (0.5 s per task);
- **heterogeneous** - Worker_3 is slower (1.8 s per task, about 3.6x) while the others stay at 0.5 s.

The slowdown is emulated: the manager sends `CMD:DELAY <seconds>` to the worker, which adds that pause to every task. All delays are reset afterwards.
Metrics: **makespan** (first task submitted to last result), **throughput**, **average task latency**, **tasks per worker** (Worker_1 / Worker_2 / Worker_3).
The table fills in and one summary line per condition appears.

Reading the table: round-robin deals tasks in turn, so it always gives 4/4/4. In the normal condition SDN-aware also ends at 4/4/4, with the same
makespan, so it adds no overhead. In the heterogeneous condition SDN-aware gives the slow worker fewer tasks (for example 5/5/2) because the fast workers
free up sooner, and the makespan drops (a reference run on the VM: 3.71 s vs 7.21 s, about 49% faster). The exact split can shift by one task between runs.

## 9. Error handling and robustness

- **Sockets:** newline-framed messages, 4 KB line limit, 5-second registration deadline, send timeout of 2 s so a stuck worker cannot freeze the manager, reconnect after a lost connection.
- **Registration:** id format and shared token checked; rejected hosts are reported to the controller, which blocks them.
- **Tasks:** validated before queueing; worker errors are returned as `ERROR` and not retried; at most 3 attempts; late results from tasks that were already redistributed are discarded.
- **Back-pressure:** the queue is capped (HTTP 429 when full); every HTTP error is returned as JSON.
- **Controller:** unknown or malformed events are ignored; if the controller goes silent for 5 s the manager shows "silent" and keeps scheduling without feedback; nothing is forwarded into a failed link.
- **Threading:** one re-entrant lock around shared state; the scheduler loop catches and logs its own errors.

## 10. Notes and limits

- OpenFlow priority decides which flow rule matches; it is not bandwidth QoS. Congestion is handled by detection and by dropping the offending flow, and it is not part of the dashboard demo.
- The shared token and the controller's blocking are lab-grade security (no TLS). A blocked host stays blocked until POX restarts.
- One switch, so there is no rerouting between alternative paths.
- Heartbeat timeout is 5 seconds (`HB_TIMEOUT` in `manager.py`), task timeout 8 seconds (`TASK_TIMEOUT`).

## 11. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Dashboard does not load | `h1 tail -30 /tmp/manager.log`; run `topology.py` from inside the project folder |
| POX: "Address already in use" on 6633 | an old controller is running: `sudo ss -ltnp \| grep 6633`, then `sudo kill <pid>` |
| POX controller shows "silent" | start POX before the topology, then restart `topology.py` |
| A scenario button says "needs Mininet" | `topology.py` is not running, or `which mnexec` prints nothing |
| Link failure does not mark the worker down | the policy must be `sdn_aware` |
| Worker does not return after Restore link | allow up to 20 s to reconnect; check `h2 tail /tmp/worker1.log` |
| "need at least 2 online workers" | wait until the workers have rejoined |

## 11a. Test cases

*VM* = observed in the Mininet VM run (dashboard screenshots, 29 Oct). *Local* = automated run of the same code on one machine
without Mininet (manager, 3 workers and the dashboard API on localhost). T2, T8 and T9 were confirmed in the VM on 31 Oct.

| # | Test | Steps | Expected result | Observed |
|---|---|---|---|---|
| T1 | Registration | start topology | 3 workers online, dashboard shows `controller: connected` | **Pass** (*VM*): Worker_1-3 online at 10.0.0.2-10.0.0.4, POX controller "connected" |
| T2 | Bad token | Unauthorized access button | attempt 1 `REJECT`, host blocked, attempts 2-3 dropped, POX `[SDN BLOCK]`, priority=200 rule | **Pass** (*VM*, 31 Oct): attempt 1 `REJECT: bad token`; timeline "controller BLOCKED 10.0.0.6 from TCP 9090"; POX `[SDN BLOCK] 10.0.0.6 blocked from TCP 9090: bad token`; attempts 2-3 "no answer (dropped by the network)" |
| T3 | Normal tasks | Auto-assign, count 10 | all 10 DONE, every worker's Done count rises | **Pass** (*Local*): 10/10 DONE in about 1 s, split 4/3/3, result "Average sensor reading = 22.80 (n=3)" |
| T4 | Pinned task | target Worker_2 only, count 4 | all 4 run on Worker_2 | **Pass** (*Local*): all 4 ran on Worker_2 |
| T5 | Bad task input | dispatch `ANOMALY:1` | worker returns ERROR, task FAILED, no retry | **Pass** (*Local*): FAILED after 1 attempt, "worker error: ANOMALY needs two numbers: threshold,value"; `AVERAGE:a,b` rejected before queueing ("parameters must be numbers separated by commas") |
| T6 | Worker failure | Worker failure + dispatch 6 | FAILED after about 5 s (heartbeat), tasks redistributed, all finish, worker returns | **Pass** (*Local*): detected after 4.6 s ("heartbeat, silent 5.0s"), 2 tasks redistributed, 6/6 completed, worker back online 10.2 s after the fault |
| T7 | Task timeout | step 7 (delay Worker_1 by 10 s, pinned task) | TIMED OUT after 8 s, finished on another worker, late result discarded | **Pass** (*Local*): "task TIMED OUT on Worker_1 after 8s", finished on Worker_2 (shown as "was Worker_1"), late result from Worker_1 discarded |
| T8 | Link failure | Link failure on Worker_1 | POX `[SDN FAILURE]`, last failure `sdn`, tasks moved | **Pass** (*VM*, 31 Oct): POX `[SDN FAILURE] port 2 went DOWN` and `manager reports Worker_1 DOWN - removing its flows`; Worker_1 failed, last failure "sdn, silent 0.2s"; tasks 5-10 pinned to Worker_1 redirected and completed on Worker_2 / Worker_3 ("was Worker_1") |
| T9 | Link recovery | Restore link | POX `[SDN RECOVERY]`, worker rejoins in 10-20 s | **Pass** (*VM*, 31 Oct): POX `[SDN RECOVERY] port 2 is UP again` then `[SDN LOAD] Worker_1 registered (10.0.0.2) - state cleared`; Worker_1 back online 2 s after Restore link (09:53:05 -> 09:53:07) |
| T10 | Evaluation | Run evaluation | 4 rows, sdn_aware not slower in normal, faster with a slow worker | **Pass** (*VM*): normal 2.11 s vs 2.11 s (no overhead); heterogeneous 3.71 s vs 7.21 s (49% faster), split 5/5/2 vs 4/4/4. *Local* re-run of the final code: 2.08 vs 2.01 s and 3.65 vs 7.28 s |
