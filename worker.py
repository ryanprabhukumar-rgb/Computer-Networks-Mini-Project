"""
EdgeFlow worker (runs on h2, h3, h4 ...).

Keeps ONE persistent TCP connection to the manager and speaks a line-based protocol:

  worker  -> manager   REGISTER: <id>|<token>
  manager -> worker    ACK  |  REJECT: <reason>
  manager -> worker    TASK:<task_id>|<TYPE>:<params>
  worker  -> manager   START:<task_id>            (worker began processing)
  worker  -> manager   RESULT:<task_id>|<output>  |  ERROR:<task_id>|<reason>
  manager -> worker    PING            worker -> manager   PONG   (heartbeat)
  manager -> worker    CMD:DELAY <s> | CMD:HANG <s>    (fault injection for demos and evaluation)

Usage: python3 worker.py [manager_ip] [worker_id] [--delay SECONDS]
"""
import argparse
import queue
import socket
import threading
import time

TOKEN = "edgeflow-d2"      # shared secret (simple application-level authentication)
BASE_COST = 0.2            # simulated processing time of every task (seconds)
RECONNECT_DELAY = 2        # wait before reconnecting after the connection drops


def execute_task(task_str):
    """Runs one IoT sensor analytics task. Raises ValueError for bad input."""
    action, _, param = task_str.partition(":")
    action, param = action.strip().upper(), param.strip()

    if action in ("AVERAGE", "PEAK", "MIN"):
        values = [float(x) for x in param.split(",")]
        if action == "AVERAGE":
            return f"Average sensor reading = {sum(values) / len(values):.2f} (n={len(values)})"
        if action == "PEAK":
            return f"Peak reading = {max(values):.2f}"
        return f"Minimum reading = {min(values):.2f}"
    if action == "POWER":
        return f"Signal power estimate = {float(param) ** 3:.2f}"
    if action == "ANOMALY":
        if param.count(",") != 1:
            raise ValueError("ANOMALY needs two numbers: threshold,value")
        threshold, value = (float(x) for x in param.split(","))
        if abs(value) > threshold:
            return f"ANOMALY DETECTED - value={value} exceeds threshold={threshold}"
        return f"Normal reading - value={value} within threshold={threshold}"
    if action == "ALERT":
        return f'Alert broadcast: "{param}"'
    raise ValueError(f"unknown task type '{action}'")


class Worker:
    def __init__(self, ip, worker_id, delay):
        self.ip, self.id, self.delay = ip, worker_id, delay
        self.sock = None
        self.send_lock = threading.Lock()
        self.tasks = queue.Queue()
        self.blackhole_until = 0.0      # while set, all traffic in both directions is dropped
        self.generation = 0             # bumped on every (re)connect so stale tasks are skipped
        threading.Thread(target=self._process_loop, daemon=True).start()

    # ------------------------------------------------------------------ sending
    def send(self, line):
        if time.time() < self.blackhole_until:
            return                       # simulated network blackhole: message lost
        with self.send_lock:
            try:
                self.sock.sendall((line + "\n").encode())
            except (OSError, AttributeError):
                pass                     # reader loop notices the dead socket

    # ------------------------------------------------------- task processing thread
    def _process_loop(self):
        while True:
            gen, task_id, payload = self.tasks.get()
            if gen != self.generation:
                continue                 # task belonged to an old (dropped) connection
            self.send(f"START:{task_id}")
            time.sleep(BASE_COST + self.delay)
            try:
                self.send(f"RESULT:{task_id}|{execute_task(payload)}")
            except Exception as e:       # bad input must never kill the worker
                self.send(f"ERROR:{task_id}|{e}")

    # ------------------------------------------------------------ connection loop
    def run(self):
        while True:
            try:
                self._session()
            except Exception as e:
                print(f"[{self.id}] connection lost ({e}); reconnecting in {RECONNECT_DELAY}s", flush=True)
            finally:
                self.generation += 1
                try:
                    self.sock.close()
                except Exception:
                    pass
            time.sleep(RECONNECT_DELAY)
            while time.time() < self.blackhole_until:   # a blackholed node cannot reconnect either
                time.sleep(0.2)

    def _session(self):
        self.sock = socket.create_connection((self.ip, 9090), timeout=5)
        self.sock.settimeout(6)                 # no PING for 6 s: connection is dead, reconnect
        self.sock.sendall(f"REGISTER: {self.id}|{TOKEN}\n".encode())
        reader = self.sock.makefile("r")
        first = reader.readline().strip()
        if first != "ACK":
            raise ConnectionError(f"registration refused: {first or 'no reply'}")
        print(f"[{self.id}] registered with manager {self.ip}:9090 (delay={self.delay}s)", flush=True)

        for raw in reader:
            line = raw.strip()
            if time.time() < self.blackhole_until:
                continue                 # blackhole: incoming message lost
            if line == "PING":
                self.send("PONG")
            elif line.startswith("TASK:"):
                task_id, _, payload = line[5:].partition("|")
                self.tasks.put((self.generation, task_id, payload))
                print(f"[{self.id}] TASK {task_id}: {payload}", flush=True)
            elif line.startswith("CMD:"):
                self._command(line[4:])
            elif line:
                print(f"[{self.id}] ignoring unknown message: {line[:60]!r}", flush=True)
        raise ConnectionError("manager closed the connection")

    def _command(self, cmd):
        name, _, arg = cmd.partition(" ")
        try:
            seconds = float(arg)
        except ValueError:
            return
        if name == "DELAY":
            self.delay = max(0.0, seconds)
            print(f"[{self.id}] processing delay set to {self.delay}s", flush=True)
        elif name == "HANG":              # silent failure: node stops answering
            self.blackhole_until = time.time() + seconds
            print(f"[{self.id}] simulated blackhole for {seconds}s", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("manager_ip", nargs="?", default="10.0.0.1")
    ap.add_argument("worker_id", nargs="?", default="Worker_1")
    ap.add_argument("--delay", type=float, default=0.0, help="extra processing time per task (s)")
    a = ap.parse_args()
    Worker(a.manager_ip, a.worker_id, a.delay).run()

