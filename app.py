from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

def fresh_state():
    return {
        "clock": 0,
        "processes": [
            {"pid": i, "request_ts": None, "replies": [], "requesting": False,
             "in_cs": False, "deferred": []}
            for i in range(3)
        ],
        "logs": [
            "Simulator ready. Select a process to request the critical section.",
            "Rule: requests are ordered by (timestamp, process ID)."
        ]
    }

state = fresh_state()

def log(message):
    state["logs"].append(message)

def tick(received_ts=None):
    if received_ts is None:
        state["clock"] += 1
    else:
        state["clock"] = max(state["clock"], received_ts) + 1

def priority(p):
    return (p["request_ts"], p["pid"])

def find_process(pid):
    return state["processes"][pid]

def handle_request(receiver, requester):
    should_defer = receiver["in_cs"]
    if receiver["requesting"] and receiver["request_ts"] is not None:
        if priority(receiver) < priority(requester):
            should_defer = True

    if should_defer:
        if requester["pid"] not in receiver["deferred"]:
            receiver["deferred"].append(requester["pid"])
        log(f'P{receiver["pid"]} defers REPLY to P{requester["pid"]} (own request has priority / in CS).')
    else:
        tick()
        if receiver["pid"] not in requester["replies"]:
            requester["replies"].append(receiver["pid"])
        log(f'P{receiver["pid"]} sends REPLY to P{requester["pid"]}.')

def try_enter_cs(p):
    if p["requesting"] and not p["in_cs"] and len(p["replies"]) == 2:
        p["in_cs"] = True
        log(f'✓ P{p["pid"]} ENTERS the critical section.')
        log(f'P{p["pid"]} has received all 2 REPLY messages.')

def snapshot():
    return {"clock": state["clock"], "processes": state["processes"], "logs": state["logs"]}

@app.route("/")
def index():
    return render_template("index.html")

@app.get("/api/state")
def get_state():
    return jsonify(snapshot())

@app.post("/api/request/<int:pid>")
def request_cs(pid):
    if pid not in (0, 1, 2):
        return jsonify({"error": "Invalid process"}), 400
    p = find_process(pid)
    if p["in_cs"]:
        log(f"P{pid} is already in the critical section.")
    elif p["requesting"]:
        log(f"P{pid} has already requested the critical section.")
    else:
        tick()
        p["request_ts"] = state["clock"]
        p["requesting"] = True
        p["replies"] = []
        log(f'P{pid} broadcasts REQUEST(timestamp={p["request_ts"]}).')
        for other in state["processes"]:
            if other["pid"] == pid:
                continue
            tick(p["request_ts"])
            log(f'P{other["pid"]} receives REQUEST from P{pid}.')
            handle_request(other, p)
        try_enter_cs(p)
    return jsonify(snapshot())

@app.post("/api/release")
def release_cs():
    active = [p for p in state["processes"] if p["in_cs"]]
    if not active:
        log("No process is currently in the critical section.")
        return jsonify(snapshot())
    p = active[0]
    p["in_cs"] = False
    p["requesting"] = False
    p["request_ts"] = None
    p["replies"] = []
    log(f'P{p["pid"]} RELEASES the critical section.')
    waiting_ids = sorted(p["deferred"])
    p["deferred"] = []
    for requester_id in waiting_ids:
        requester = find_process(requester_id)
        if requester["requesting"]:
            tick()
            if p["pid"] not in requester["replies"]:
                requester["replies"].append(p["pid"])
            log(f'P{p["pid"]} sends deferred REPLY to P{requester_id}.')
            try_enter_cs(requester)
    return jsonify(snapshot())

@app.post("/api/concurrent")
def concurrent():
    if any(p["requesting"] or p["in_cs"] for p in state["processes"]):
        log("Finish current requests and release the critical section before starting a concurrent round.")
        return jsonify(snapshot())
    tick()
    shared_ts = state["clock"]
    for p in state["processes"]:
        p["requesting"] = True
        p["request_ts"] = shared_ts
        p["replies"] = []
    log(f"--- Concurrent round: P0, P1 and P2 request at timestamp {shared_ts} ---")
    # Deliver all requests, then resolve priority. This makes the lowest PID win ties.
    for requester in state["processes"]:
        for receiver in state["processes"]:
            if receiver["pid"] == requester["pid"]:
                continue
            tick(shared_ts)
            log(f'P{receiver["pid"]} receives REQUEST from P{requester["pid"]}.')
            handle_request(receiver, requester)
    for p in state["processes"]:
        try_enter_cs(p)
    return jsonify(snapshot())

@app.post("/api/reset")
def reset():
    global state
    state = fresh_state()
    return jsonify(snapshot())

if __name__ == "__main__":
    app.run(debug=True)
