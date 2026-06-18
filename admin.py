"""Admin-дашборд воркеров — отдельный FastAPI-процесс (раньше был вшит в server.py).

Запуск:  uvicorn admin:app --host 127.0.0.1 --port 8090   (см. Makefile.worker)
         либо  python admin.py  (возьмёт ADMIN_HOST/ADMIN_PORT из env/.env).

Читает состояние ИСКЛЮЧИТЕЛЬНО из Redis (своего процесса-воркера тут нет):
  • backlog        — XLEN потока задач,
  • consumer-группы — XINFO GROUPS,
  • метрики флота  — снапшоты, которые воркеры раз в HEARTBEAT_SEC пишут в
                     llm:worker:stats:{consumer} (с TTL — мёртвые отпадают сами),
  • последние запросы — XREVRANGE + расшифровка на лету (есть ключ).

Plaintext расшифрованных запросов отдаётся ТОЛЬКО в ответ /stats и никуда не
пишется. Держи порт за localhost/firewall. Ключи берём через keys.py (KEY_PREFIX),
поэтому видим только свой флот, а не чужой на общем Redis.
"""
import os
import json
import time

import keys  # раньше crypto: грузит .env до чтения GATEWAY_CRYPTO_KEY
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from crypto import decrypt

REDIS_URL  = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MODEL_NAME = os.getenv("MODEL_NAME", "qwen3-coder")
ADMIN_HOST = os.getenv("ADMIN_HOST", "127.0.0.1")
ADMIN_PORT = int(os.getenv("ADMIN_PORT", "8090"))

app = FastAPI(title="redis-llm-gateway · admin")
r = redis.from_url(REDIS_URL, decode_responses=True)


def _last_user(payload):
    for m in reversed(payload.get("messages", [])):
        if m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, str):
                return c
            if isinstance(c, list):           # мультимодальный content
                return " ".join(p.get("text", "") for p in c if isinstance(p, dict))
    return ""


async def _stats():
    """Срез флота из Redis: backlog + consumer-группы + воркеры + последние запросы."""
    stream = keys.req_stream()
    try:
        backlog = await r.xlen(stream)
    except Exception:
        backlog = None
    groups = []
    try:
        for g in await r.xinfo_groups(stream):
            groups.append({"name": g.get("name"), "consumers": g.get("consumers"),
                           "pending": g.get("pending"), "lag": g.get("lag")})
    except Exception:
        pass
    workers = {}                               # весь флот — из опубликованных снапшотов
    try:
        async for k in r.scan_iter(match=keys.stats_pattern()):
            v = await r.get(k)
            if v:
                try:
                    w = json.loads(v); workers[w.get("consumer")] = w
                except Exception:
                    pass
    except Exception:
        pass
    workers = sorted(workers.values(), key=lambda w: w.get("consumer", ""))
    recent = []                                # последние запросы, расшифрованные на лету
    try:
        for eid, f in await r.xrevrange(stream, count=15):
            item = {"id": f.get("id"), "stream": f.get("stream") == "1",
                    "ts": int(str(eid).split("-")[0]) // 1000}
            try:
                body = json.loads(decrypt(f["payload"]))
                item["model"] = body.get("model")
                item["nmsg"] = len(body.get("messages", []))
                item["text"] = " ".join(_last_user(body).split())[:220]
            except Exception:
                item["text"] = "‹не удалось расшифровать›"
            recent.append(item)
    except Exception:
        pass
    return {"ts": int(time.time()), "model": MODEL_NAME, "backlog": backlog,
            "groups": groups, "workers": workers, "recent": recent}


@app.get("/stats")
async def stats():
    return JSONResponse(await _stats())


@app.get("/")
@app.get("/dashboard")
@app.get("/admin")
async def dashboard():
    return HTMLResponse(_ADMIN_HTML)


_ADMIN_HTML = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<title>redis-llm-gateway · worker admin</title>
<style>
  :root{color-scheme:dark}
  *{box-sizing:border-box}
  body{margin:0;font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;background:#0d1117;color:#e6edf3}
  header{padding:16px 24px;border-bottom:1px solid #21262d;display:flex;align-items:center;gap:12px}
  h1{font-size:16px;margin:0;font-weight:600}
  .dot{width:9px;height:9px;border-radius:50%;background:#3fb950;box-shadow:0 0 8px #3fb950}
  .dot.stale{background:#d29922;box-shadow:0 0 8px #d29922}
  .muted{color:#7d8590;font-size:13px}
  main{padding:24px;max-width:1120px;margin:0 auto}
  .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:28px}
  .card{background:#161b22;border:1px solid #21262d;border-radius:10px;padding:14px 16px}
  .card .k{color:#7d8590;font-size:11px;text-transform:uppercase;letter-spacing:.05em}
  .card .v{font-size:27px;font-weight:600;margin-top:6px;font-variant-numeric:tabular-nums}
  .card .v.warn{color:#f85149}
  table{width:100%;border-collapse:collapse;margin-bottom:28px}
  th,td{text-align:left;padding:8px 10px;border-bottom:1px solid #21262d}
  th{color:#7d8590;font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.04em}
  td.num{font-variant-numeric:tabular-nums}
  h2{font-size:12px;color:#7d8590;text-transform:uppercase;letter-spacing:.05em;margin:0 0 10px}
  code{background:#161b22;padding:1px 6px;border-radius:5px}
  .empty{color:#7d8590;padding:14px;background:#161b22;border:1px dashed #30363d;border-radius:8px}
</style></head>
<body>
<header><span class="dot" id="dot"></span><h1>worker · admin</h1><span class="muted" id="meta">…</span></header>
<main>
  <div class="cards" id="cards"></div>
  <h2>Последние запросы · расшифровано</h2><div id="requests"></div>
  <h2>Воркеры (флот)</h2><div id="workers"></div>
  <h2>Consumer-группы</h2><div id="groups"></div>
</main>
<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const card=(k,v,warn)=>`<div class="card"><div class="k">${k}</div><div class="v ${warn?'warn':''}">${v}</div></div>`;
const up=s=>s<60?s+'с':s<3600?Math.floor(s/60)+'м':Math.floor(s/3600)+'ч '+Math.floor(s%3600/60)+'м';
async function tick(){
  let d;
  try{ d=await (await fetch('/stats')).json(); }
  catch(e){ $('dot').classList.add('stale'); $('meta').textContent='нет связи с воркером'; return; }
  $('dot').classList.remove('stale');
  const W=d.workers||[], sum=f=>W.reduce((a,w)=>a+(w[f]||0),0);
  $('meta').innerHTML=`модель <b>${d.model}</b> · обновлено ${new Date(d.ts*1000).toLocaleTimeString()}`;
  $('cards').innerHTML=
      card('Очередь', d.backlog??'—', (d.backlog||0)>50)
    + card('Воркеры', W.length, W.length===0)
    + card('In-flight', sum('inflight'))
    + card('Обработано', sum('processed'))
    + card('Ошибки', sum('errors'), sum('errors')>0)
    + card('RPS · 1м', W.reduce((a,w)=>a+(w.rps_1m||0),0).toFixed(1));
  const R=d.recent||[];
  $('requests').innerHTML = R.length ? `<table><tr>
      <th>время</th><th>модель</th><th>тип</th><th>сообщений</th><th>текст · расшифровано</th></tr>` +
      R.map(x=>`<tr>
        <td class="num">${new Date(x.ts*1000).toLocaleTimeString()}</td>
        <td>${esc(x.model||'—')}</td>
        <td>${x.stream?'stream':'unary'}</td>
        <td class="num">${x.nmsg??'—'}</td>
        <td>${esc(x.text||'')}</td></tr>`).join('') + `</table>`
    : `<div class="empty">Пока нет запросов.</div>`;
  $('workers').innerHTML = W.length ? `<table><tr>
      <th>consumer</th><th>upstream</th><th>uptime</th><th>in-flight</th><th>done</th><th>err</th>
      <th>rps</th><th>lat p50/p95</th><th>qwait p50/p95</th></tr>` +
      W.map(w=>`<tr>
        <td>${w.consumer}</td><td>${w.upstream}</td><td class="num">${up(w.uptime_s||0)}</td>
        <td class="num">${w.inflight}/${w.max_inflight}</td><td class="num">${w.processed}</td>
        <td class="num">${w.errors}</td><td class="num">${(w.rps_1m||0).toFixed(1)}</td>
        <td class="num">${w.lat_ms_p50}/${w.lat_ms_p95} ms</td>
        <td class="num">${w.qwait_ms_p50}/${w.qwait_ms_p95} ms</td></tr>`).join('') + `</table>`
    : `<div class="empty">Нет активных воркеров.</div>`;
  const G=d.groups||[];
  $('groups').innerHTML = G.length ? `<table><tr><th>группа</th><th>consumers</th><th>pending</th><th>lag</th></tr>` +
      G.map(g=>`<tr><td>${g.name}</td><td class="num">${g.consumers}</td>
        <td class="num">${g.pending}</td><td class="num">${g.lag??'—'}</td></tr>`).join('') + `</table>`
    : `<div class="empty">Очередь ещё не создана.</div>`;
}
tick(); setInterval(tick, 2000);
</script>
</body></html>"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=ADMIN_HOST, port=ADMIN_PORT)
