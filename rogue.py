"""
Unauthorized-access generator. The dashboard's "Unauthorized access" button runs this inside h6.
Manual:   h6 python3 rogue.py 10.0.0.1

It tries to register as a worker three times with a wrong token.
Attempt 1: the manager answers REJECT and tells the controller, which blocks this host.
Attempts 2 and 3: no answer - the controller drops the traffic at the switch.
"""
import socket
import sys
import time

manager = sys.argv[1] if len(sys.argv) > 1 else "10.0.0.1"
for i in range(1, 4):
    s = socket.socket()
    s.settimeout(4)
    try:
        s.connect((manager, 9090))
        s.sendall(b"REGISTER: Worker_9|letmein\n")           # wrong token
        print(f"attempt {i}: manager answered {s.recv(100).decode().strip()}", flush=True)
    except socket.timeout:
        print(f"attempt {i}: no answer (dropped by the network)", flush=True)
    except OSError as e:
        print(f"attempt {i}: {e}", flush=True)
    finally:
        s.close()
    time.sleep(1)
