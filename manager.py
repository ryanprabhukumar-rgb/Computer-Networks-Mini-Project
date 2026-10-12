import socket
import threading
import queue
import time
from flask import Flask, render_template, jsonify, request

app = Flask(__name__, template_folder='.')

# Dedicated queues for targeted worker routing
worker_queues = {
    "Worker_1": queue.Queue(),
    "Worker_2": queue.Queue(),
    "ANY": queue.Queue()
}

active_workers = {}  # worker_id -> timestamp
lock = threading.Lock()
system_data = {
    "results": []
}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/dispatch', methods=['POST'])
def dispatch():
    data = request.get_json()
    task = data.get('task', 'AVERAGE:22.5,23.1,22.8')
    target = data.get('target_worker', 'ANY')

    if target in worker_queues:
        worker_queues[target].put(task)
    else:
        worker_queues['ANY'].put(task)

    return jsonify({"status": "queued", "target": target})

@app.route('/api/data')
def get_data():
    now = time.time()
    with lock:
        online = [w for w, last_t in active_workers.items() if now - last_t < 6]
        results_copy = list(system_data["results"])

    return jsonify({
        "workers": online,
        "results": results_copy
    })

def handle_worker(conn, addr):
    worker_id = None
    try:
        conn.settimeout(10)
        reg_msg = conn.recv(1024).decode().strip()
        if not reg_msg.startswith("REGISTER"):
            return

        worker_id = reg_msg.replace("REGISTER: ", "").replace("REGISTER:", "").strip()
        with lock:
            active_workers[worker_id] = time.time()

        print(f"[MANAGER] Worker '{worker_id}' connected persistently from {addr}")

        while True:
            task = None
            if worker_id in worker_queues and not worker_queues[worker_id].empty():
                task = worker_queues[worker_id].get_nowait()
            elif not worker_queues["ANY"].empty():
                task = worker_queues["ANY"].get_nowait()

            if task:
                conn.sendall(f"TASK:{task}".encode())
                conn.settimeout(10)
                resp = conn.recv(1024).decode().strip()

                if resp.startswith("RESULT:"):
                    result_body = resp.replace("RESULT:", "").strip()
                    with lock:
                        system_data["results"].insert(0, f"<b>[{worker_id}]</b> {task} ➔ {result_body}")
                        if len(system_data["results"]) > 20:
                            system_data["results"].pop()
            else:
                conn.sendall(b"PING")
                conn.settimeout(5)
                pong = conn.recv(1024).decode().strip()
                if pong != "PONG":
                    raise ConnectionError("Invalid heartbeat reply")

            with lock:
                active_workers[worker_id] = time.time()

            time.sleep(0.5)

    except Exception as e:
        print(f"[MANAGER] Worker '{worker_id}' disconnected: {e}")
    finally:
        if worker_id:
            with lock:
                active_workers.pop(worker_id, None)
        try:
            conn.close()
        except Exception:
            pass

def start_tcp_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', 9090))
    server.listen(10)
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_worker, args=(conn, addr), daemon=True).start()

if __name__ == '__main__':
    threading.Thread(target=start_tcp_server, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)
