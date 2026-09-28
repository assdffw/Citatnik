"""Самопроверка витрины по HTTP. Только стандартная библиотека.

  python selftest.py                       # сервис на localhost:8000 запущен как `python main.py`
  python selftest.py --catalog-url http://host.docker.internal:9000   # сервис в Docker
Флаги: --fast (пропустить проверку свежести, ~1 мин), --big (снимок на 340 000 записей).
Поднимает у себя фальшивый каталог на порту 9000.
"""
import argparse, http.client, json, statistics, sys, threading, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ap = argparse.ArgumentParser()
ap.add_argument("--service", default="http://localhost:8000")
ap.add_argument("--catalog-url", default="http://127.0.0.1:9000")
ap.add_argument("--fast", action="store_true")
ap.add_argument("--big", action="store_true")
A = ap.parse_args()

# ---------------- фальшивый каталог ----------------
C = {"quotes": {}, "hits": {}, "total": 0, "mode": "ok", "delay": 0.0}
lock = threading.Lock()


class Cat(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        qid = urllib.parse.unquote(self.path.rsplit("/", 1)[-1])
        with lock:
            C["total"] += 1
            C["hits"][qid] = C["hits"].get(qid, 0) + 1
        if C["delay"]:
            time.sleep(C["delay"])
        if C["mode"] == "down":  # не отвечает вовсе
            time.sleep(6)
            self.close_connection = True
            return
        if C["mode"] == "busy":
            self.send_response(503)
            self.send_header("Retry-After", "3")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        rec = C["quotes"].get(qid)
        body = json.dumps(rec or {"detail": "not found"}, ensure_ascii=False).encode()
        self.send_response(200 if rec else 404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


srv = ThreadingHTTPServer(("0.0.0.0", 9000), Cat)
srv.daemon_threads = True
threading.Thread(target=srv.serve_forever, daemon=True).start()

# ---------------- клиент ----------------
U = urllib.parse.urlparse(A.service)


def call(method, path, body=None, timeout=20):
    if isinstance(body, (dict, list)):
        body = json.dumps(body).encode()
    c = http.client.HTTPConnection(U.hostname, U.port, timeout=timeout)
    t = time.perf_counter()
    try:
        c.request(method, path, body=body, headers={"Content-Type": "application/json"} if body else {})
        r = c.getresponse()
        data = r.read()
        return r.status, {k.lower(): v for k, v in r.getheaders()}, data, time.perf_counter() - t
    finally:
        c.close()


def stats():
    return json.loads(call("GET", "/stats")[2])


def snapshot(recs, junk=()):
    lines = [json.dumps(r, ensure_ascii=False) for r in recs] + list(junk)
    return ('{"quotes":[\n' + ",\n".join(lines) + "\n]}\n").encode()


def rec(qid, text="текст"):
    return {"id": qid, "author": "Автор", "text": text, "tags": ["t"], "year": 1, "lang": "ru"}


fails = 0


def check(name, cond, info=""):
    global fails
    fails += 0 if cond else 1
    print(("PASS  " if cond else "FAIL  ") + name + (f"   [{info}]" if info else ""))


P = f"t{int(time.time())}"
N = 20000

# ---------------- проверки ----------------
s, h, b, _ = call("GET", "/health")
check("health", s == 200 and json.loads(b) == {"status": "healthy"})
s, _, b, _ = call("POST", "/catalog/source", {"url": A.catalog_url})
check("catalog/source", s == 200 and json.loads(b) == {"source": A.catalog_url})

junk = ["not json,", '{"id":"x","author":"a"},', "[1,2],"]
s, _, b, _ = call("POST", "/import", snapshot([rec(f"{P}-{i}") for i in range(N)], junk))
check("import", s == 200 and json.loads(b)["imported"] == N, b.decode())
st0 = stats()
check("catalog count == snapshot", st0["catalog"] == N, st0["catalog"])
s, _, b, _ = call("POST", "/import", snapshot([rec(f"{P}-{i}") for i in range(N - 100)]))
check("dropped == 100", s == 200 and json.loads(b) == {"imported": N - 100, "dropped": 100}, b.decode())
s, *_ = call("POST", "/import", b'{"quotes":[\n{"id":"a","author":"b","text":"c"}\n')
check("truncated snapshot -> 400", s == 400)
call("POST", "/import", snapshot([rec(f"{P}-{i}") for i in range(N)]))

# чтение из локального хранилища + распределение задержек
st0 = stats(); c0 = C["total"]
s, h, b, _ = call("GET", f"/quotes/{P}-5")
check("LOCAL read", s == 200 and h.get("x-source") == "LOCAL" and json.loads(b)["id"] == f"{P}-5")
with ThreadPoolExecutor(16) as ex:
    res = list(ex.map(lambda i: call("GET", f"/quotes/{P}-{i % N}"), range(1500)))
lat = sorted(r[3] for r in res)
check("1500 local reads: all 200 and LOCAL", all(r[0] == 200 and r[1].get("x-source") == "LOCAL" for r in res))
check("latency", lat[-1] < 1.0, f"p50={statistics.median(lat)*1000:.1f}ms p99={lat[int(len(lat)*.99)]*1000:.1f}ms max={lat[-1]*1000:.0f}ms")
st1 = stats()
check("no catalog reads for local", st1["catalog_reads"] == st0["catalog_reads"] and C["total"] == c0)

# каталог: свежая цитата, отсутствие не запоминается, singleflight
C["quotes"][f"{P}-new"] = rec(f"{P}-new")
s, h, *_ = call("GET", f"/quotes/{P}-new")
check("fresh quote via CATALOG", s == 200 and h.get("x-source") == "CATALOG")
s, h, *_ = call("GET", f"/quotes/{P}-new")
check("then LOCAL", h.get("x-source") == "LOCAL")
call("GET", f"/quotes/{P}-ghost"); s, *_ = call("GET", f"/quotes/{P}-ghost")
check("404 not remembered", s == 404 and C["hits"].get(f"{P}-ghost") == 2)
C["quotes"][f"{P}-sf"] = rec(f"{P}-sf"); C["delay"] = 0.5
with ThreadPoolExecutor(40) as ex:
    res = list(ex.map(lambda _: call("GET", f"/quotes/{P}-sf"), range(40)))
C["delay"] = 0.0
check("singleflight: 40 readers -> 1 catalog hit", C["hits"].get(f"{P}-sf") == 1 and all(r[0] == 200 for r in res), C["hits"].get(f"{P}-sf"))

# PUT / DELETE
s, *_ = call("PUT", f"/catalog/{P}-p", {"author": "A" * 201, "text": "x"}); check("PUT author>200 -> 422", s == 422)
s, *_ = call("PUT", f"/catalog/{P}-p", {"author": "A", "text": ""}); check("PUT empty text -> 422", s == 422)
s, *_ = call("PUT", f"/catalog/{P}-p", {"author": "A", "text": "x" * 16385}); check("PUT text>16384 -> 422", s == 422)
s, _, b, _ = call("PUT", f"/catalog/{P}-p", {"author": "Автор", "text": "x" * 16384})
check("PUT ok", s == 200 and json.loads(b)["id"] == f"{P}-p")
s, h, *_ = call("GET", f"/quotes/{P}-p"); check("PUT then LOCAL", s == 200 and h.get("x-source") == "LOCAL")
s, _, b, _ = call("DELETE", f"/catalog/{P}-p"); check("DELETE", s == 200 and json.loads(b) == {"deleted": True})
s, *_ = call("GET", f"/quotes/{P}-p"); check("after DELETE 404", s == 404)
s, *_ = call("DELETE", f"/catalog/{P}-p"); check("DELETE again 404", s == 404)

# каталог занят (503 + Retry-After): не долбим
C["mode"] = "busy"; t0 = C["total"]
s, h, *_ = call("GET", f"/quotes/{P}-b1")
check("catalog busy -> 503 with Retry-After", s == 503 and "retry-after" in h, h.get("retry-after"))
for i in range(5): call("GET", f"/quotes/{P}-b{i+2}")
check("catalog not hammered while busy", C["total"] - t0 <= 2, C["total"] - t0)
C["mode"] = "ok"; time.sleep(3.3)
C["quotes"][f"{P}-after"] = rec(f"{P}-after")
s, *_ = call("GET", f"/quotes/{P}-after"); check("works after Retry-After", s == 200)

# каталог не отвечает вовсе
C["mode"] = "down"; t0 = C["total"]
with ThreadPoolExecutor(30) as ex:
    res = list(ex.map(lambda i: call("GET", f"/quotes/{P}-d{i}"), range(30)))
worst = max(r[3] for r in res)
check("catalog down: every reader answered < 3s", worst < 3.0 and all(r[0] == 503 for r in res), f"worst={worst:.2f}s, catalog requests={C['total']-t0}")
s, _, _, el = call("GET", f"/quotes/{P}-d99"); n1 = C["total"]
check("catalog down: fast failure, no request storm", s == 503 and el < 0.3 and n1 - t0 <= 8, f"{el*1000:.0f}ms, requests={n1-t0}")
check("health during outage", call("GET", "/health")[0] == 200)
C["mode"] = "ok"; C["quotes"][f"{P}-rec"] = rec(f"{P}-rec"); t = time.time(); code = 0
while time.time() - t < 40:
    code = call("GET", f"/quotes/{P}-rec")[0]
    if code == 200: break
    time.sleep(0.5)
check("recovers after catalog is back", code == 200, f"{time.time()-t:.1f}s")

# свежесть: правка в каталоге доезжает < 60 с
if not A.fast:
    q = f"{P}-fresh"; C["quotes"][q] = rec(q, "старый")
    call("POST", "/import", snapshot([rec(q, "старый")]))
    C["quotes"][q] = rec(q, "НОВЫЙ")
    print("...ждём 55 с для проверки свежести")
    time.sleep(55)
    s, h, b, _ = call("GET", f"/quotes/{q}")
    check("edit reaches reader within 60s", s == 200 and json.loads(b)["text"] == "НОВЫЙ", h.get("x-source"))

# большой снимок: сервис жив и отвечает во время приёма
if A.big:
    body = snapshot([rec(f"big-{i:07d}", "Ц" * 150) for i in range(340000)])
    print(f"снимок {len(body)/2**20:.0f} МБ, отправляю...")
    out = {}
    th = threading.Thread(target=lambda: out.update(r=call("POST", "/import", body, timeout=180)))
    t = time.time(); th.start(); worst = 0.0
    while th.is_alive():
        worst = max(worst, call("GET", "/health")[3]); time.sleep(0.05)
    th.join(); s, _, b, _ = out["r"]
    check("big import", s == 200 and json.loads(b)["imported"] == 340000, f"{time.time()-t:.1f}s {b.decode()}")
    check("health during import", worst < 1.0, f"worst={worst*1000:.0f}ms")
    print("stats:", stats())

print("\n" + ("ВСЁ ХОРОШО" if not fails else f"ПРОВАЛЕНО ПРОВЕРОК: {fails}"))
sys.exit(1 if fails else 0)
