import json, os, statistics, sys, time, urllib.request, urllib.error, base64

BASE = os.environ["OS_SQL_URL"].rstrip("/")
IDX = "process_composer_request"
TENANT = os.environ["OS_BENCH_TENANT"]
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 25
QUERIES = {
    "WHERE count": f"SELECT COUNT(*) FROM {IDX} WHERE tenantId = '{TENANT}'",
    "SORT top10": f"SELECT requestCreatedDate FROM {IDX} WHERE tenantId = '{TENANT}' ORDER BY requestCreatedDate DESC LIMIT 10",
    "SORT top10 all": f"SELECT requestCreatedDate FROM {IDX} ORDER BY requestCreatedDate DESC LIMIT 10",
    "GROUP BY": f"SELECT workspaceType, COUNT(*) FROM {IDX} GROUP BY workspaceType",
    "WHERE+GROUP BY": f"SELECT workspaceType, COUNT(*) FROM {IDX} WHERE tenantId = '{TENANT}' GROUP BY workspaceType",
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
errs = {}
for i in range(RUNS):
    for name, q in QUERIES.items():  # interleaved so drift hits all queries equally
        ms, status, body = run(q)
        if status == 200:
            results[name].append(ms)
            last[name] = f"rows={len(body.get('datarows', []))}"
            schema[name] = body["schema"][0]["type"]
        else:
            failures[name] += 1; errs[name] = (status, str(body)[:160])
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

for k, e in errs.items():
    print("error", k, e)
