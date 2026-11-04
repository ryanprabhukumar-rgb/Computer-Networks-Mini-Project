"""
Performance evaluation: static baseline vs SDN-aware under two conditions (about 20 seconds).
Started by the "Run evaluation" button on the dashboard (manager.py calls evaluate()).

  normal         all workers equally fast
  heterogeneous  the last worker is about 3.6x slower (1.8 s vs 0.5 s per task)

The SAME 12 tasks run under round_robin (static baseline) and sdn_aware (proposed approach).
Metrics: makespan, throughput, average task latency, tasks per worker.
"""
import json, statistics, time, urllib.request

BASE = "http://127.0.0.1:5000"
TASKS = ["AVERAGE:22.5,23.1,22.8,24.0", "PEAK:22.5,23.1,22.8", "MIN:22.5,23.1,22.8", "POWER:4", "ANOMALY:25,42"]
N_TASKS = 12


def call(path, body=None):
    req = urllib.request.Request(BASE + path, data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def run(scenario, policy, workers):
    call("/api/reset", {})
    call("/api/policy", {"policy": policy})
    for w in workers:                       # extra seconds per task: normal 0.3 s each, heterogeneous last worker 1.6 s
        slow = scenario == "heterogeneous" and w == workers[-1]
        call("/api/fault", {"worker": w, "action": "delay", "seconds": 1.6 if slow else 0.3})
    time.sleep(0.6)
    ids = {call("/api/dispatch", {"task": TASKS[i % len(TASKS)]})["ids"][0] for i in range(N_TASKS)}

    start = time.time()
    while time.time() - start < 60:         # wait until every task is finished
        mine = [t for t in call("/api/tasks") if t["id"] in ids]
        if all(t["state"] in ("DONE", "FAILED") for t in mine):
            break
        time.sleep(0.5)

    done = [t for t in mine if t["state"] == "DONE"]
    makespan = max(t["finished"] for t in done) - min(t["created"] for t in done)
    return dict(scenario=scenario, policy=policy, completed=f"{len(done)}/{N_TASKS}", makespan_s=round(makespan, 2),
                throughput=round(len(done) / makespan, 2),
                avg_latency_s=round(statistics.mean(t["finished"] - t["created"] for t in done), 2),
                per_worker="/".join(str(sum(t["worker"] == w for t in done)) for w in workers))


def evaluate(report=print):
    """Runs both conditions under both policies and returns the results."""
    workers = sorted(w["id"] for w in call("/api/data")["workers"] if w["online"])
    if len(workers) < 2:
        raise RuntimeError("need at least 2 online workers")
    results = []
    for scenario in ("normal", "heterogeneous"):
        for policy in ("round_robin", "sdn_aware"):
            report(f"running {scenario} / {policy} ...")
            results.append(run(scenario, policy, workers))
    for w in workers:                       # back to normal speed
        call("/api/fault", {"worker": w, "action": "delay", "seconds": 0})
    call("/api/policy", {"policy": "sdn_aware"})
    report("finished")
    return results


if __name__ == "__main__":                  # can also be run on h1: python3 evaluate.py
    for r in evaluate():
        print(r)
