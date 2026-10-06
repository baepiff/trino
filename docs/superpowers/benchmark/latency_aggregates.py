import json, os, statistics, sys, time, urllib.request, urllib.error, base64

BASE = os.environ["OS_SQL_URL"].rstrip("/")
IDX = "process_composer_request"
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 25
QUERIES = {
    "COUNT(*)": f"SELECT COUNT(*) FROM {IDX}",
    "MIN": f"SELECT MIN(requestDuration) FROM {IDX}",
    "MAX": f"SELECT MAX(requestDuration) FROM {IDX}",
    "SUM": f"SELECT SUM(requestDuration) FROM {IDX}",
    "AVG": f"SELECT AVG(requestDuration) FROM {IDX}",
}
headers = {"Content-Type": "application/json"}
if os.environ.get("OS_SQL_USER"):
    token = base64.b64encode(f"{os.environ['OS_SQL_USER']}:{os.environ.get('OS_SQL_PASSWORD','')}".encode()).decode()
    headers["Authorization"] = "Basic " + token

def run(q):
    req = urllib.request.Request(BASE + "/_plugins/_sql", data=json.dumps({"query": q}).encode(), headers=headers, method="POST")
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            body = json.loads(r.read())
            status = r.status
    except urllib.error.HTTPError as e:
        status, body = e.code, {}
    except Exception as e:
        status, body = -1, {"error": str(e)[:100]}
    return (time.perf_counter() - start) * 1000, status, body

results = {k: [] for k in QUERIES}
last = {}
schema = {}
failures = {k: 0 for k in QUERIES}
for i in range(RUNS):
    for name, q in QUERIES.items():  # interleaved so drift hits all queries equally
        ms, status, body = run(q)
        if status == 200:
            results[name].append(ms)
            last[name] = body["datarows"][0][0] if body.get("datarows") else None
            schema[name] = body["schema"][0]["type"]
        else:
            failures[name] += 1
    time.sleep(0.5)

def pct(v, p):
    s = sorted(v)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]

print(f"runs per query: {RUNS}, interleaved, 0.5 s pause between rounds")
print(f"{'query':<9} {'ok':>3} {'fail':>4} {'min':>7} {'p50':>7} {'mean':>7} {'p95':>7} {'max':>7}  (ms)  type  last value")
for name, v in results.items():
    if not v:
        print(f"{name:<9} no successful runs")
        continue
    print(f"{name:<9} {len(v):>3} {failures[name]:>4} {min(v):>7.0f} {pct(v,50):>7.0f} {statistics.mean(v):>7.0f} {pct(v,95):>7.0f} {max(v):>7.0f}        {schema[name]:<8} {last[name]}")
