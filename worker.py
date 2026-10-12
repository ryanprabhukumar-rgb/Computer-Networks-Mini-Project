import socket
import sys
import time

RECONNECT_DELAY = 2  # Seconds to wait before attempting to reconnect if socket drops


def execute_task(task_str):
    """Executes IoT sensor analytics tasks and returns formatted result string."""
    try:
        if ":" in task_str:
            action, param = task_str.split(":", 1)
        else:
            action, param = "ALERT", task_str

        action = action.strip().upper()
        param = param.strip()

        # 1. AVERAGE / ADD (Mean sensor reading)
        if action in ("AVERAGE", "ADD"):
            values = [float(x) for x in param.split(",")]
            res = sum(values) / len(values)
            time.sleep(0.2)
            return f"Average sensor reading = {res:.2f} (n={len(values)})"

        # 2. PEAK / MAX / MULTIPLY (Maximum sensor reading)
        elif action in ("PEAK", "MAX", "MULTIPLY"):
            values = [float(x) for x in param.split(",")]
            res = max(values)
            time.sleep(0.2)
            return f"Peak reading = {res:.2f}"

        # 3. MIN (Minimum sensor reading)
        elif action == "MIN":
            values = [float(x) for x in param.split(",")]
            res = min(values)
            time.sleep(0.2)
            return f"Minimum reading = {res:.2f}"

        # 4. POWER / CUBE (Signal power estimation)
        elif action in ("POWER", "CUBE"):
            val = float(param)
            res = val ** 3
            time.sleep(0.2)
            return f"Signal power estimate = {res:.2f}"

        # 5. ANOMALY Check (Compares value against threshold)
        elif action == "ANOMALY":
            threshold_str, value_str = param.split(",")
            threshold, value = float(threshold_str), float(value_str)
            time.sleep(0.2)
            if abs(value) > threshold:
                return f"ANOMALY DETECTED - value={value} exceeds threshold={threshold}"
            return f"Normal reading - value={value} within threshold={threshold}"

        # 6. ALERT / PRINT (Broadcast alert message)
        elif action in ("ALERT", "PRINT"):
            time.sleep(0.1)
            return f'Alert broadcast: "{param}"'

        else:
            return f"Unknown task type: {task_str}"

    except Exception as e:
        return f"Error executing task '{task_str}': {str(e)}"


def start_worker(server_ip, worker_id, server_port=9090):
    """Maintains a single persistent TCP socket connection to manager.py."""
    while True:
        sock = None
        try:
            # 1. Establish long-lived TCP connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(15)  # 15s timeout for idle keep-alives
            sock.connect((server_ip, server_port))

            # 2. Register once per session
            sock.sendall(f"REGISTER: {worker_id}".encode())
            print(f"[{worker_id}] Connected and registered with Manager at {server_ip}:{server_port}")

            # 3. Persistent receive/execute loop over the same open socket
            while True:
                data = sock.recv(1024)
                if not data:
                    raise ConnectionError("Manager closed connection")

                msg = data.decode().strip()

                # Handle keep-alive ping from manager
                if msg == "PING":
                    sock.sendall(b"PONG")
                    continue

                # Process task sent over open socket
                if msg.startswith("TASK:"):
                    task_payload = msg.replace("TASK:", "").strip()
                    output = execute_task(task_payload)
                    sock.sendall(f"RESULT: {output}".encode())

        except Exception as e:
            print(f"[{worker_id}] Connection lost ({e}). Reconnecting in {RECONNECT_DELAY}s...")
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

        time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    target_ip = sys.argv[1] if len(sys.argv) > 1 else '10.0.0.1'
    node_id = sys.argv[2] if len(sys.argv) > 2 else 'Worker_1'
    start_worker(target_ip, node_id)
