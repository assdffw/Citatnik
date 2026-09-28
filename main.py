"""Витрина цитатника. Один процесс, asyncio + aiohttp, всё состояние в памяти."""
import asyncio
import math
import os
import random
import time
from collections import OrderedDict
from urllib.parse import quote

import aiohttp
from aiohttp import web

try:
    import orjson

    loads = orjson.loads
    dumps = orjson.dumps
except ImportError:  # запасной вариант без orjson
    import json

    loads = json.loads

    def dumps(o):
        return json.dumps(o, ensure_ascii=False, separators=(",", ":")).encode()


def env(name, default):
    return type(default)(os.getenv(name, default))


# ---- настройки ---------------------------------------------------------------
FRESH = env("FRESH", 50.0)                 # макс. возраст отдаваемых данных (обещано 60 с)
REFRESH_AFTER = env("REFRESH_AFTER", 35.0)  # горячие записи обновляем в фоне заранее
READER_DEADLINE = env("READER_DEADLINE", 2.5)  # обещано 3 с
CATALOG_TIMEOUT = env("CATALOG_TIMEOUT", 2.2)
CATALOG_CONCURRENCY = env("CATALOG_CONCURRENCY", 6)  # каталог общий на всю компанию
BG_MAX = env("BG_MAX", 16)                  # фоновых обновлений одновременно
FAIL_THRESHOLD = env("FAIL_THRESHOLD", 3)   # подряд неудач до "размыкания"
BACKOFF_MAX = env("BACKOFF_MAX", 15.0)
BUSY_CAP = env("BUSY_CAP", 30.0)
LOCAL_BUDGET = env("LOCAL_BUDGET", 44 * 1024 * 1024)
ENTRY_OVERHEAD = 280                        # реальные накладные расходы Python на запись
MAX_LINE = 1 << 20
SERVE_STALE = env("SERVE_STALE", 0) == 1    # отдавать устаревшее при недоступном каталоге


def jerr(status, detail, retry=None):
    h = {"Retry-After": str(max(1, math.ceil(retry)))} if retry is not None else None
    return web.json_response({"detail": detail}, status=status, headers=h)


class Unavailable(Exception):
    def __init__(self, retry=1.0):
        self.retry = retry


class Store:
    def __init__(self):
        self.local = OrderedDict()  # id -> (сырой JSON bytes, monotonic ts); порядок = LRU
        self.bytes = 0
        self.known = set()          # id цитат, которые витрина считает существующими
        self.tomb = set()           # снятые до следующего снимка
        self.inflight = {}          # singleflight обращений в каталог
        self.bg = 0
        self.ingest_puts = None     # id, появившиеся во время приёма снимка
        self.import_lock = asyncio.Lock()
        self.source = None
        self.session = None
        self.sem = None
        # защита каталога
        self.fails = 0
        self.open_until = 0.0
        self.opened_at = 0.0
        self.probing = False
        self.busy_until = 0.0
        # счётчики
        self.served_local = 0
        self.served_from_catalog = 0
        self.catalog_reads = 0
        self.evictions = 0

    # ---- локальное хранилище ----
    def drop(self, qid):
        e = self.local.pop(qid, None)
        if e:
            self.bytes -= len(e[0])

    def put(self, qid, raw, ts):
        """Обычная запись: при нехватке места вытесняем самые давние (считаем в evictions)."""
        self.drop(qid)
        need = len(raw) + ENTRY_OVERHEAD
        if need > LOCAL_BUDGET:
            return
        while self.bytes + ENTRY_OVERHEAD * len(self.local) + need > LOCAL_BUDGET and self.local:
            _, (old, _t) = self.local.popitem(last=False)
            self.bytes -= len(old)
            self.evictions += 1
        self.local[qid] = (raw, ts)
        self.bytes += len(raw)

    def put_import(self, qid, raw, ts, start):
        """Запись из снимка: вытесняем только то, что старше начала снимка (не считаем)."""
        self.drop(qid)
        need = len(raw) + ENTRY_OVERHEAD
        if need > LOCAL_BUDGET // 50:
            return
        while self.bytes + ENTRY_OVERHEAD * len(self.local) + need > LOCAL_BUDGET:
            if not self.local:
                return
            k = next(iter(self.local))
            if self.local[k][1] >= start:
                return  # место занято свежим — новое не берём
            old, _t = self.local.pop(k)
            self.bytes -= len(old)
        self.local[qid] = (raw, ts)
        self.bytes += len(raw)

    # ---- защита каталога ----
    def gate_open(self):
        """Можно ли сейчас обращаться в каталог (без побочных эффектов)."""
        if not self.source:
            return False
        now = time.monotonic()
        if now < self.busy_until:
            return False
        return not (self.fails >= FAIL_THRESHOLD and (now < self.open_until or self.probing))

    def gate(self):
        """Возвращает True, если этот запрос — пробный после размыкания."""
        if not self.source:
            raise Unavailable(1)
        now = time.monotonic()
        if now < self.busy_until:
            raise Unavailable(self.busy_until - now)
        if self.fails >= FAIL_THRESHOLD:
            if now < self.open_until or self.probing:
                raise Unavailable(max(self.open_until - now, 1.0))
            self.probing = True
            return True
        return False

    def on_ok(self):
        self.fails = 0
        self.open_until = 0.0

    def on_fail(self, t0):
        if t0 < self.opened_at:
            return  # запрос ушёл до размыкания — эта неудача уже учтена
        self.fails += 1
        if self.fails >= FAIL_THRESHOLD:
            back = min(BACKOFF_MAX, 2.0 ** (self.fails - FAIL_THRESHOLD))
            now = time.monotonic()
            self.opened_at = now
            self.open_until = now + back * (0.8 + 0.4 * random.random())

    # ---- обращения в каталог ----
    async def _run(self, qid):
        for attempt in (0, 1):
            probe = False
            t0 = time.monotonic()
            try:
                async with self.sem:
                    probe = self.gate()  # проверка после очереди: ждавшие не обходят защиту
                    t0 = time.monotonic()
                    self.catalog_reads += 1
                    async with self.session.get(
                        self.source + "/quote/" + quote(qid, safe="")
                    ) as r:
                        status = r.status
                        raw = await r.read() if status == 200 else b""
                        ra = r.headers.get("Retry-After")
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                self.on_fail(t0)
                raise Unavailable(max(self.open_until - time.monotonic(), 1.0))
            finally:
                if probe:
                    self.probing = False

            if status == 200:
                self.on_ok()
                try:
                    obj = loads(raw)
                    ok = (isinstance(obj, dict) and obj.get("id") == qid
                          and isinstance(obj.get("author"), str)
                          and isinstance(obj.get("text"), str))
                except Exception:
                    ok = False
                if not ok:
                    raise Unavailable(1)
                if qid not in self.tomb:
                    self.known.add(qid)
                    if self.ingest_puts is not None:
                        self.ingest_puts.add(qid)
                    e = self.local.get(qid)
                    if not (e and e[1] > t0):  # не затираем более свежую правку
                        self.put(qid, raw, t0)
                return raw
            if status == 404:
                self.on_ok()
                e = self.local.get(qid)
                if not (e and e[1] > t0):
                    self.drop(qid)
                    self.known.discard(qid)
                return None
            if status == 503:
                self.on_ok()  # каталог жив, просто занят
                try:
                    wait = min(max(float(ra), 0.1), BUSY_CAP)
                except (TypeError, ValueError):
                    wait = 1.0
                self.busy_until = max(self.busy_until, time.monotonic() + wait)
                if attempt == 0 and wait <= 0.8:
                    await asyncio.sleep(wait)
                    continue
                raise Unavailable(wait)
            self.on_fail(t0)  # неожиданный код
            raise Unavailable(1)
        raise Unavailable(1)

    def fetch_task(self, qid):
        t = self.inflight.get(qid)
        if t is None:
            t = asyncio.ensure_future(self._run(qid))
            self.inflight[qid] = t

            def done(f, q=qid):
                self.inflight.pop(q, None)
                if not f.cancelled():
                    f.exception()  # помечаем исключение прочитанным

            t.add_done_callback(done)
        return t

    async def catalog_get(self, qid):
        t = self.fetch_task(qid)
        try:
            return await asyncio.wait_for(asyncio.shield(t), READER_DEADLINE)
        except asyncio.TimeoutError:
            raise Unavailable(1)

    def bg_refresh(self, qid):
        if qid in self.inflight or self.bg >= BG_MAX or not self.gate_open():
            return
        self.bg += 1
        t = self.fetch_task(qid)
        t.add_done_callback(lambda _f: setattr(self, "bg", self.bg - 1))


S = Store()


# ---- ручки -------------------------------------------------------------------
async def h_health(_):
    return web.json_response({"status": "healthy"})


async def h_source(req):
    try:
        d = await req.json()
    except Exception:
        return jerr(400, "bad json")
    u = d.get("url") if isinstance(d, dict) else None
    if not isinstance(u, str) or not u.startswith(("http://", "https://")):
        return jerr(400, "url required")
    S.source = u.rstrip("/")
    S.fails, S.open_until, S.busy_until = 0, 0.0, 0.0
    return web.json_response({"source": u})


async def snapshot_lines(stream):
    buf, skip = b"", False
    async for chunk in stream.iter_chunked(1 << 18):
        if skip:
            i = chunk.find(b"\n")
            if i < 0:
                continue
            chunk, skip = chunk[i + 1:], False
        parts = (buf + chunk).split(b"\n")
        buf = parts.pop()
        if len(buf) > MAX_LINE:  # чудовищная строка — пропускаем до конца
            buf, skip = b"", True
        yield parts
        await asyncio.sleep(0)  # отдаём управление читателям и /health
    if buf and not skip:
        yield [buf]


async def h_import(req):
    async with S.import_lock:
        start = time.monotonic()
        S.ingest_puts = set()
        tomb0 = set(S.tomb)
        new_ids, imported, state = set(), 0, 0  # 0 заголовок, 1 записи, 2 закрыто
        try:
            async for parts in snapshot_lines(req.content):
                for raw in parts:
                    line = raw.strip()
                    if state == 0:
                        if not line:
                            continue
                        if line == b'{"quotes":[':
                            state = 1
                        elif line == b'{"quotes":[]}':
                            state = 2
                        else:
                            raise ValueError("bad header")
                    elif state == 1:
                        if line == b"]}":
                            state = 2
                            continue
                        if line.endswith(b","):
                            line = line[:-1].rstrip()
                        if not line:
                            continue
                        try:
                            obj = loads(line)
                            qid = obj.get("id")
                            if not (isinstance(qid, str) and qid
                                    and isinstance(obj.get("author"), str) and obj["author"]
                                    and isinstance(obj.get("text"), str) and obj["text"]):
                                continue
                        except Exception:
                            continue  # содержимое записей каталог не гарантирует
                        new_ids.add(qid)
                        imported += 1
                        if qid not in S.ingest_puts:  # свежая правка новее снимка
                            S.put_import(qid, line, start, start)
                    elif line:
                        raise ValueError("data after end")
            if state != 2:
                raise ValueError("truncated snapshot")
        except BaseException as exc:
            S.ingest_puts = None
            if isinstance(exc, asyncio.CancelledError):
                raise
            return jerr(400, "bad snapshot")

        puts, S.ingest_puts = S.ingest_puts, None
        dropped = sum(1 for i in S.known if i not in new_ids and i not in puts)
        new_ids |= puts
        S.known = new_ids
        now = time.monotonic()
        for q, (_raw, ts) in list(S.local.items()):
            if q not in new_ids or now - ts > FRESH:
                S.drop(q)  # ушли из снимка или всё равно устарели — не eviction
        S.tomb -= tomb0
        S.known -= S.tomb
        return web.json_response({"imported": imported, "dropped": dropped})


async def h_put(req):
    qid = req.match_info["quote_id"]
    try:
        body = loads(await req.read())
    except Exception:
        return jerr(400, "bad json")
    if not isinstance(body, dict):
        return jerr(400, "object expected")
    a, t = body.get("author"), body.get("text")
    if not (isinstance(a, str) and 1 <= len(a) <= 200 and isinstance(t, str)
            and 1 <= len(t) <= 16384):
        return jerr(422, "out of bounds")
    raw = dumps({"id": qid, "author": a, "text": t})
    S.tomb.discard(qid)
    S.known.add(qid)
    if S.ingest_puts is not None:
        S.ingest_puts.add(qid)
    S.put(qid, raw, time.monotonic())
    return web.Response(body=raw, content_type="application/json")


async def h_delete(req):
    qid = req.match_info["quote_id"]
    if qid in S.tomb:
        return jerr(404, "not found")
    if qid not in S.known and qid not in S.local:
        try:
            if await S.catalog_get(qid) is None:
                return jerr(404, "not found")
        except Unavailable as u:
            return jerr(503, "catalog unavailable", u.retry)
    S.tomb.add(qid)
    S.drop(qid)
    S.known.discard(qid)
    return web.json_response({"deleted": True})


def quote_response(raw, source):
    return web.Response(body=raw, content_type="application/json",
                        headers={"X-Source": source})


async def h_get(req):
    qid = req.match_info["quote_id"]
    if qid in S.tomb:
        return jerr(404, "not found")
    e = S.local.get(qid)
    if e is not None:
        age = time.monotonic() - e[1]
        if age <= FRESH:
            S.local.move_to_end(qid)
            S.served_local += 1
            if age > REFRESH_AFTER:
                S.bg_refresh(qid)  # успеем обновить до того, как запись устареет
            return quote_response(e[0], "LOCAL")
    try:
        raw = await S.catalog_get(qid)
    except Unavailable as u:
        if e is not None and SERVE_STALE:
            S.served_local += 1
            return quote_response(e[0], "LOCAL")
        return jerr(503, "catalog unavailable", u.retry)
    if raw is None or qid in S.tomb:
        return jerr(404, "not found")  # отсутствие не запоминаем
    S.served_from_catalog += 1
    return quote_response(raw, "CATALOG")


async def h_stats(_):
    return web.json_response({
        "served_local": S.served_local,
        "served_from_catalog": S.served_from_catalog,
        "catalog_reads": S.catalog_reads,
        "evictions": S.evictions,
        "local": len(S.local),
        "bytes": S.bytes,
        "catalog": len(S.known),
    })


async def on_start(app):
    S.sem = asyncio.Semaphore(CATALOG_CONCURRENCY)
    S.session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=CATALOG_TIMEOUT, sock_connect=1.0),
        connector=aiohttp.TCPConnector(limit=CATALOG_CONCURRENCY, keepalive_timeout=30,
                                       ttl_dns_cache=300),
    )


async def on_stop(app):
    await S.session.close()


def make_app():
    app = web.Application(client_max_size=1024 * 1024)
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_stop)
    app.router.add_get("/health", h_health)
    app.router.add_post("/catalog/source", h_source)
    app.router.add_post("/import", h_import)
    app.router.add_put("/catalog/{quote_id}", h_put)
    app.router.add_delete("/catalog/{quote_id}", h_delete)
    app.router.add_get("/quotes/{quote_id}", h_get)
    app.router.add_get("/stats", h_stats)
    return app


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass
    web.run_app(make_app(), host="0.0.0.0", port=8000, access_log=None, backlog=1024)
