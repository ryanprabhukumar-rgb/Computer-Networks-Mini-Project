"""
EdgeFlow Task Manager (runs on h1)

  TCP  9090  persistent connections to workers: register, assign tasks, results, heartbeat
  HTTP 5000  dashboard + small JSON API
  SDN link   manager -> controller: UDP events to the virtual address 10.0.0.100:9999
             controller -> manager: the controller writes the network state of each worker to a file

Two scheduling policies:
  round_robin  static baseline - tasks handed out in turn, ignores load and network state, and the
               controller only forwards (no congestion mitigation, no link-failure shortcut)
  sdn_aware    dynamic - fewest outstanding tasks first, avoids workers the controller reports
               as CONGESTED or DOWN, and the controller reacts to congestion and failures
"""
import json, os, re, socket, struct, subprocess, threading, time
import evaluate                                         # evaluate.py (Run evaluation button)
from collections import deque
from flask import Flask, jsonify, render_template, request

app = Flask(__name__, template_folder=".")

# ------------------------------------------------------------------ settings
TASK_TIMEOUT = float(os.getenv("TASK_TIMEOUT", 8))     # max processing time of one task (s)
HB_INTERVAL = 1                                        # send PING every second
HB_TIMEOUT = float(os.getenv("HB_TIMEOUT", 5))         # no reply for this long -> worker failed
PREFETCH = 2                                           # sdn_aware: max tasks queued per worker
MAX_ATTEMPTS = 3                                       # give up on a task after 3 assignments
MAX_PENDING = int(os.getenv("MAX_PENDING", 500))       # queue limit (back-pressure)
TOKEN = "edgeflow-d2"                                  # shared secret workers must present
TASK_TYPES = ("AVERAGE", "PEAK", "MIN", "POWER", "ANOMALY", "ALERT")
host, port = os.getenv("CTRL_ADDR", "10.0.0.100:9999").split(":")
CTRL_ADDR = (host, int(port))                          # virtual address answered by POX
NET_FILE = "/tmp/edgeflow_net.json"                    # written by the controller, read here
HOSTS_FILE = "/tmp/edgeflow_hosts.json"                # written by topology.py: process id of each Mininet host
HERE = os.path.dirname(os.path.abspath(__file__))
MANAGER_IP = os.getenv("MANAGER_IP", "10.0.0.1")

# --------------------------------------------------------------------- state
lock = threading.RLock()
tasks, pending, workers = {}, deque(), {}              # all tasks / waiting task ids / registered workers
events = deque(maxlen=60)                              # timeline shown on the dashboard
state = dict(policy="sdn_aware", next_id=1, rr=0, controller_seen=0.0, blocked=set())
stats = dict(timeouts=0, redistributed=0, rejected=0, failures=0, sdn_failures=0)
evaluation = dict(running=False, status="", results=[])  # filled by the Run evaluation button
udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def log(kind, text):
    """kind: TCP (worker <-> manager) | SDN | SCHED | FAULT | SYS"""
    events.appendleft({"time": time.strftime("%H:%M:%S"), "kind": kind, "text": text})
    print(f"[{time.strftime('%H:%M:%S')}] {kind:5} {text}", flush=True)


class Worker:
    def __init__(self, wid, ip):
        self.id, self.ip, self.sock = wid, ip, None
        self.online = False
        self.outstanding = set()                       # task ids sent but not finished
        self.done = self.timeouts = self.failures = 0
        self.last_seen = self.last_ping = 0.0
        self.net, self.rate = "OK", 0.0                # network state reported by the controller
        self.last_failure = "-"
        self.send_lock = threading.Lock()

    def send(self, line):
        try:
            with self.send_lock:
                self.sock.sendall((line + "\n").encode())
            return True
        except (OSError, AttributeError):
            return False


# ------------------------------------------------------- talking to the SDN controller
def notify(msg):
    """Application event -> controller (fire and forget; the app works without the controller)."""
    try:
        udp.sendto(json.dumps(msg, separators=(",", ":")).encode(), CTRL_ADDR)
    except OSError:
        pass


def read_controller_state():
    """Controller -> manager: the controller writes the network state of every worker to NET_FILE.
    (Manager and controller run on the same VM, so a shared file is the simplest feedback channel.)"""
    try:
        with open(NET_FILE) as f:
            net = json.load(f)
    except (OSError, ValueError):
        return
    if time.time() - net.get("time", 0) > 5 or net["time"] <= state["controller_seen"]:
        return                                         # stale or already processed
    state["controller_seen"] = net["time"]
    for ip in set(net.get("blocked", [])) - state["blocked"]:
        state["blocked"].add(ip)
        log("SDN", f"controller BLOCKED {ip} from TCP 9090 (drop rule installed on the switch)")
    for wid, info in net.get("workers", {}).items():
        w = workers.get(wid)
        if not w:
            continue
        if info["s"] != w.net:
            log("SDN", f"controller reports {wid}: {w.net} -> {info['s']}")
        w.net, w.rate = info["s"], info.get("r", 0.0)
    for w in workers.values():                         # link failure seen by the switch (dynamic policy only)
        if w.net == "DOWN" and w.online and state["policy"] == "sdn_aware":
            fail_worker(w, "SDN controller reported its link down", "sdn")


# ----------------------------------------------------------------------- tasks
def new_task(payload, target=None):
    with lock:
        tid = state["next_id"]
        state["next_id"] += 1
        tasks[tid] = dict(id=tid, payload=payload, target=target, state="PENDING", worker=None, attempts=0,
                          created=time.time(), started=None, finished=None, result=None, was=None, avoid=None)
        pending.append(tid)
        return tid


def requeue(task, why):
    """Put a task back at the front of the queue, or give up after MAX_ATTEMPTS."""
    task["was"] = task["worker"] or task.get("was")      # remember who had it (dashboard: "was Worker_1")
    task["avoid"] = task["worker"]                       # do not hand it straight back to the worker that just failed it
    task.update(state="PENDING", worker=None, started=None, target=None)
    if task["attempts"] >= MAX_ATTEMPTS:
        task.update(state="FAILED", finished=time.time(), result=f"gave up after {task['attempts']} attempts ({why})")
        log("SCHED", f"task {task['id']} abandoned: {why}")
    else:
        pending.appendleft(task["id"])


# -------------------------------------------------------------------- scheduler
def pick_worker(task):
    online = sorted((w for w in workers.values() if w.online), key=lambda w: w.id)
    if not online:
        return None
    usable = [w for w in online if w.id != task.get("avoid")] or online      # prefer a different worker after a timeout
    pinned = workers.get(task["target"]) if task["target"] else None
    if pinned and not pinned.online:
        pinned = None                                  # target is gone: any worker will do
    if state["policy"] == "round_robin":               # static: no load or network awareness
        if pinned:
            return pinned
        state["rr"] += 1
        return usable[state["rr"] % len(usable)]
    # sdn_aware: skip DOWN workers, limit queue depth, avoid CONGESTED, then fewest outstanding
    candidates = [pinned] if pinned else usable
    free = [w for w in candidates if w.net != "DOWN"
            and len(w.outstanding) < (1 if w.net == "CONGESTED" else PREFETCH)]
    return min(free, key=lambda w: (w.net == "CONGESTED", len(w.outstanding), w.id)) if free else None


def schedule():
    with lock:
        for tid in list(pending):
            task = tasks[tid]
            w = pick_worker(task)
            if not w:
                continue
            pending.remove(tid)
            task.update(state="ASSIGNED", worker=w.id)
            task["attempts"] += 1
            if task["target"] and task["target"] != w.id:      # pinned worker is offline
                task["was"] = task["target"]
                log("SCHED", f"{task['target']} is offline: task {tid} redirected to {w.id}")
            w.outstanding.add(tid)
            if w.send(f"TASK:{tid}|{task['payload']}"):
                log("TCP", f"Manager -> {w.id}: TASK {tid} ({task['payload'][:30]}) [{state['policy']}]")
            else:
                fail_worker(w, "could not send task", "socket")


def fail_worker(w, reason, source):
    """Worker failed: close it and redistribute everything it still owed us."""
    with lock:
        if not w.online:
            return
        w.online = False
        w.failures += 1
        stats["failures"] += 1
        stats["sdn_failures"] += source == "sdn"
        w.last_failure = f"{source}, silent {time.time() - w.last_seen:.1f}s"
        moved = sorted(w.outstanding)
        for tid in reversed(moved):
            requeue(tasks[tid], f"{w.id} failed")
        stats["redistributed"] += len(moved)
        w.outstanding.clear()
        try:
            w.sock.shutdown(socket.SHUT_RDWR)
            w.sock.close()
        except (OSError, AttributeError):
            pass
        log("SYS", f"{w.id} FAILED ({reason}); {len(moved)} task(s) redistributed")
        notify({"e": "DOWN", "w": w.id, "ip": w.ip})


def monitor_loop():
    """Every 0.25 s: heartbeats, task timeouts, workload report to the controller."""
    last_report = 0.0
    while True:
        time.sleep(0.25)
        now = time.time()
        with lock:
            read_controller_state()
            for w in list(workers.values()):
                if w.online and now - w.last_ping >= HB_INTERVAL:
                    w.last_ping = now
                    w.send("PING")
                if w.online and now - w.last_seen > HB_TIMEOUT:
                    fail_worker(w, f"no heartbeat for {HB_TIMEOUT:g}s", "heartbeat")
            for t in list(tasks.values()):
                if t["state"] == "RUNNING" and now - t["started"] > TASK_TIMEOUT:
                    w = workers[t["worker"]]
                    w.outstanding.discard(t["id"])
                    w.timeouts += 1
                    stats["timeouts"] += 1
                    log("SCHED", f"task {t['id']} TIMED OUT on {w.id} after {TASK_TIMEOUT:g}s -> redistributing")
                    requeue(t, "timeout")
            if now - state["controller_seen"] > 5:     # controller silent: ignore stale network state
                for w in workers.values():
                    w.net = "OK"
            if now - last_report >= 1:
                last_report = now
                notify({"e": "HELLO", "p": state["policy"]})   # tells the controller which policy is active
                for w in workers.values():
                    notify({"e": "LOAD", "w": w.id, "ip": w.ip, "o": len(w.outstanding), "d": w.done,
                            "s": "UP" if w.online else "DOWN"})


def scheduler_loop():
    while True:
        time.sleep(0.1)
        try:
            schedule()
        except Exception as e:                         # the scheduler must never die
            log("SYS", f"scheduler error: {e}")


# ------------------------------------------------------------ worker connections
def register(sock, line, ip):
    """Check the REGISTER line. Returns the Worker, or None if rejected."""
    m = re.fullmatch(r"REGISTER:\s*(Worker_\d{1,3})\|(\S+)", line.strip())
    old = workers.get(m.group(1)) if m else None
    reason = "malformed registration" if not m else "bad token" if m.group(2) != TOKEN else None
    if reason:
        stats["rejected"] += 1
        log("SYS", f"REJECTED connection from {ip}: {reason}")
        notify({"e": "UNAUTH", "ip": ip, "r": reason})
        try:
            sock.sendall(f"REJECT: {reason}\n".encode())
        except OSError:
            pass
        return None
    with lock:
        w = old or Worker(m.group(1), ip)
        if w.online:
            fail_worker(w, "re-registered", "socket")  # same host reconnecting: drop the stale session
        workers[w.id] = w
        w.ip, w.sock, w.online, w.net = ip, sock, True, "OK"
        w.last_seen = w.last_ping = time.time()
    sock.sendall(b"ACK\n")
    log("TCP", f"{w.id} ({ip}) -> Manager: REGISTER")
    notify({"e": "REG", "w": w.id, "ip": ip})
    return w


def handle_message(w, line):
    with lock:
        now = time.time()
        w.last_seen = now
        if line == "PONG":
            return
        kind, _, rest = line.partition(":")
        tid, _, body = rest.partition("|")
        task = tasks.get(int(tid)) if tid.isdigit() else None
        if kind not in ("START", "RESULT", "ERROR") or not task:
            log("TCP", f"{w.id}: ignored unknown message {line[:40]!r}")
            return
        if task["worker"] != w.id or task["state"] not in ("ASSIGNED", "RUNNING"):
            log("TCP", f"{w.id}: stale {kind} for task {tid} discarded (already redistributed)")
            return
        if kind == "START":                            # processing began: the timeout clock starts now
            task.update(state="RUNNING", started=now)
            return
        w.outstanding.discard(task["id"])
        if kind == "RESULT":
            w.done += 1
            task.update(state="DONE", result=body, finished=now)
            log("TCP", f"{w.id} -> Manager: RESULT task {tid}: {body[:50]}")
        else:                                          # bad input: retrying would fail again
            task.update(state="FAILED", result=f"worker error: {body}", finished=now)
            log("TCP", f"{w.id} -> Manager: ERROR task {tid}: {body[:50]}")


def serve_connection(sock, addr):
    w = None
    try:
        sock.settimeout(5)                             # registration must arrive within 5 s
        reader = sock.makefile("r")
        first = reader.readline(256)
        w = register(sock, first, addr[0]) if first else None
        if not w:
            return
        sock.settimeout(None)                          # reads wait for data; sends give up after 2 s:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, struct.pack("ll", 2, 0))
        for line in iter(lambda: reader.readline(4096), ""):   # runs until the connection closes
            handle_message(w, line.strip())
    except (OSError, ValueError):
        pass
    finally:
        if w and w.sock is sock:
            fail_worker(w, "connection closed", "socket")
        sock.close()


def tcp_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 9090))
    server.listen(20)
    while True:
        conn, addr = server.accept()
        threading.Thread(target=serve_connection, args=(conn, addr), daemon=True).start()


# --------------------------------------------------------------- HTTP / dashboard
def error(msg, code=400):
    return jsonify(ok=False, error=msg), code


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    with lock:
        recent = sorted(tasks.values(), key=lambda t: -t["id"])[:10]
        return jsonify(
            policy=state["policy"], pending=len(pending), stats=stats, events=list(events)[:25],
            completed=sum(t["state"] == "DONE" for t in tasks.values()),
            failed=sum(t["state"] == "FAILED" for t in tasks.values()),
            workers=[dict(id=w.id, ip=w.ip, online=w.online, net=w.net, rate=w.rate, load=len(w.outstanding),
                          done=w.done, timeouts=w.timeouts, failures=w.failures, last_failure=w.last_failure)
                     for w in sorted(workers.values(), key=lambda w: w.id)],
            tasks=[{k: t[k] for k in ("id", "payload", "state", "worker", "attempts", "result", "was")} for t in recent],
            sdn_connected=time.time() - state["controller_seen"] < 5, evaluation=evaluation)


@app.route("/api/tasks")                               # every task with timestamps (used by evaluate.py)
def api_tasks():
    with lock:
        return jsonify([{k: t[k] for k in ("id", "state", "worker", "created", "started", "finished")}
                        for t in tasks.values()])


def run_evaluation():
    try:
        evaluation["results"] = evaluate.evaluate(report=lambda text: evaluation.update(status=text))
    except Exception as e:
        evaluation["status"] = f"evaluation failed: {e}"
    finally:
        evaluation["running"] = False


@app.route("/api/evaluate", methods=["POST"])           # "Run evaluation" button
def api_evaluate():
    if evaluation["running"]:
        return error("an evaluation is already running", 409)
    if sum(w.online for w in workers.values()) < 2:
        return error("need at least 2 online workers")
    evaluation.update(running=True, status="starting ...", results=[])
    threading.Thread(target=run_evaluation, daemon=True).start()
    return jsonify(ok=True)


@app.route("/api/dispatch", methods=["POST"])
def api_dispatch():
    data = request.get_json(silent=True) or {}
    payload, target = str(data.get("task", "")).strip(), data.get("target_worker", "ANY")
    kind, _, params = payload.partition(":")
    try:
        count = int(data.get("count", 1))
    except (TypeError, ValueError):
        return error("count must be a number")
    if kind.upper() not in TASK_TYPES:
        return error(f"unknown task type (allowed: {', '.join(TASK_TYPES)})")
    if "|" in payload or (kind.upper() != "ALERT" and not re.fullmatch(r"\s*-?[\d.]+(\s*,\s*-?[\d.]+)*\s*", params)):
        return error("parameters must be numbers separated by commas")
    if not 1 <= count <= 100:
        return error("count must be between 1 and 100")
    if len(pending) + count > MAX_PENDING:
        return error(f"task queue is full ({len(pending)}/{MAX_PENDING})", 429)
    pin = target if re.fullmatch(r"Worker_\d{1,3}", str(target)) else None
    ids = [new_task(payload, pin) for _ in range(count)]
    log("SCHED", f"{count} task(s) queued: {payload[:30]} -> {pin or 'any worker'}")
    return jsonify(ok=True, ids=ids)


@app.route("/api/policy", methods=["POST"])
def api_policy():
    value = (request.get_json(silent=True) or {}).get("policy")
    if value not in ("round_robin", "sdn_aware"):
        return error("policy must be round_robin or sdn_aware")
    state["policy"] = value
    log("SCHED", f"scheduling policy: {value}")
    return jsonify(ok=True)


def in_host(host, *command):
    """Run a command inside another Mininet host, exactly like typing it in the Mininet CLI."""
    with open(HOSTS_FILE) as f:
        pid = json.load(f)[host]
    return subprocess.Popen(["mnexec", "-a", str(pid), *command], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)


def run_intruder():
    """Unauthorized access scenario: h6 tries to register 3 times; the output goes to the timeline."""
    out = in_host("h6", "python3", os.path.join(HERE, "rogue.py"), MANAGER_IP).communicate(timeout=30)[0]
    for line in out.strip().splitlines():
        log("FAULT", f"h6 (rogue host) {line}")


@app.route("/api/fault", methods=["POST"])             # scenario buttons (and evaluate.py)
def api_fault():
    data = request.get_json(silent=True) or {}
    w, action = workers.get(data.get("worker")), str(data.get("action", "")).lower()
    try:
        seconds = float(data.get("seconds", 10))
    except (TypeError, ValueError):
        return error("seconds must be a number")
    if action == "intruder":                           # unauthorized access from h6
        threading.Thread(target=run_intruder, daemon=True).start()
        log("FAULT", "h6 (rogue host) is trying to register with a wrong token ...")
        return jsonify(ok=True)
    if not w:
        return error(f"unknown worker {data.get('worker')}")
    if action in ("link_down", "link_up"):             # link failure: same as `link s1 h2 down` in the CLI
        port = "s1-eth" + w.ip.split(".")[-1]          # h2 (10.0.0.2) is on switch port s1-eth2, and so on
        try:
            in_host("root", "ip", "link", "set", port, action[5:])
        except (OSError, KeyError, ValueError) as e:
            return error(f"needs Mininet (topology.py): {e}")
        log("FAULT", f"link to {w.id} ({port}) set {action[5:].upper()}")
        return jsonify(ok=True)
    if action not in ("delay", "hang"):
        return error("unknown action")
    if not w.online:
        return error(f"{w.id} is not online")
    w.send(f"CMD:{action.upper()} {seconds}")
    log("FAULT", f"{action.upper()} {seconds:g}s injected on {w.id}")
    return jsonify(ok=True)


@app.route("/api/reset", methods=["POST"])
def api_reset():
    with lock:
        tasks.clear(), pending.clear(), events.clear()
        stats.update({k: 0 for k in stats})
        for w in workers.values():
            w.outstanding.clear()
            w.done = w.timeouts = w.failures = 0
            w.last_failure = "-"
    return jsonify(ok=True)


if __name__ == "__main__":
    for t in (tcp_server, monitor_loop, scheduler_loop):
        threading.Thread(target=t, daemon=True).start()
    log("SYS", "EdgeFlow manager up: TCP 9090, dashboard http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, threaded=True)
