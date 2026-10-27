# main_mes.py
# HMI-утилита MES TCP/IP  — FastAPI + asyncio (plain text TCP, port 10000)
#
# Запуск:
#   pip install fastapi uvicorn
#   uvicorn main_mes:app --host 0.0.0.0 --port 8001
#
# Утилита выступает TCP-КЛИЕНТОМ к станку (роль MES).
# Протокол — текстовые команды + \n, ответы читаются до \n (для SENDL/SENDR — до "done").

import asyncio
import json
import logging
import re
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel

app = FastAPI(title="MES Client HMI Utility")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mes")

# ---------------------------------------------------------------------------
# Глобальное состояние TCP
# ---------------------------------------------------------------------------
mes_lock = asyncio.Lock()
_clients: dict[tuple[str, int], "MesTcpClient"] = {}

DEFAULT_PORT = 10000
CMD_TERM = b"\n"
LIST_TERM = b"done"          # конец списка рецептов (строка "done")
RECIPE_SEP = ";"


class MesTcpClient:
    """Текстовый TCP-клиент к станку. Один сокет на (host, port)."""

    def __init__(self, host: str, port: int, timeout: float = 3.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    @property
    def connected(self) -> bool:
        return self.writer is not None and not self.writer.is_closing()

    async def connect(self):
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), timeout=self.timeout
        )
        log.info("MES TCP connected to %s:%s", self.host, self.port)

    async def close(self):
        if self.writer is not None:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        self.reader = None
        self.writer = None

    async def _ensure(self):
        if not self.connected:
            await self.close()
            await self.connect()

    async def request(self, command: str, multiline: bool = False) -> str:
        """Отправить команду + \n, прочитать ответ.
        multiline=True — читать до строки 'done' (для SENDL/SENDR)."""
        await self._ensure()
        self.writer.write((command + "\n").encode("ascii", errors="replace"))
        await self.writer.drain()

        if not multiline:
            data = await asyncio.wait_for(
                self.reader.readuntil(CMD_TERM), timeout=self.timeout
            )
            return data.decode("utf-8", errors="replace").strip()

        # Многострочный ответ: накапливаем, пока не встретим строку "done"
        lines: list[str] = []
        while True:
            line = await asyncio.wait_for(
                self.reader.readuntil(CMD_TERM), timeout=self.timeout
            )
            text = line.decode("utf-8", errors="replace").strip()
            if text.lower() == LIST_TERM.decode():
                break
            lines.append(text)
        return "\n".join(lines)


async def get_client(host: str, port: int) -> Optional[MesTcpClient]:
    """Вернуть подключённый клиент или None. Вызывать внутри mes_lock."""
    key = (host, port)
    client = _clients.get(key)
    if client is not None and client.connected:
        return client
    if client is not None:
        try:
            await client.close()
        except Exception:
            pass
    client = MesTcpClient(host, port)
    try:
        await client.connect()
    except Exception as e:
        log.warning("MES connect %s:%s failed: %s", host, port, e)
        await client.close()
        _clients.pop(key, None)
        return None
    _clients[key] = client
    return client


def parse_recipes(raw: str) -> list[str]:
    """'text1 L;text2 L;text3 L;' -> ['text1 L','text2 L','text3 L']"""
    parts = [p.strip() for p in raw.replace("\n", "").split(RECIPE_SEP)]
    return [p for p in parts if p]


def parse_status(raw: str) -> int:
    """GETSTATUS -> 0..3.
    В таблице протокола статус — цифра: 0=stop, 1=pause, 2=run, 3=alarm.
    Если станок вдруг ответит текстом — принимаем и английские слова.
    """
    s = raw.strip().lower()
    m = re.search(r"[0-3]", s)
    if m:
        return int(m.group(0))
    # fallback по словам, если станок ответит текстом
    if any(w in s for w in ("alarm", "error")): return 3
    if any(w in s for w in ("run", "work")):    return 2
    if any(w in s for w in ("pause",)):         return 1
    return 0  # stop / неизвестно


def parse_int(raw: str) -> Optional[int]:
    m = re.search(r"-?\d+", raw)
    return int(m.group(0)) if m else None


def parse_complete(raw: str) -> bool:
    """LeftCompletes/RightCompletes -> True; *_None -> False."""
    s = raw.strip().lower()
    return "none" not in s and "complete" in s


# ---------------------------------------------------------------------------
# Pydantic-модели
# ---------------------------------------------------------------------------

class Conn(BaseModel):
    host: str
    port: int = DEFAULT_PORT


class RawCmd(Conn):
    command: str


class SetNumCmd(Conn):
    side: str          # "L" | "R"
    value: int


class SideCmd(Conn):
    side: str          # "L" | "R"


class RecipesCmd(Conn):
    left: str = ""     # имя левого рецепта ("" — не менять)
    right: str = ""    # имя правого рецепта


class ExportConfig(BaseModel):
    api_base: str = ""
    host: str = "192.168.0.100"
    port: int = DEFAULT_PORT
    rate_ms: int = 1000
    write_log: list[str] = []


# ---------------------------------------------------------------------------
# API: команды по таблице
# ---------------------------------------------------------------------------

async def _run(conn: Conn, command: str, multiline: bool = False) -> dict:
    async with mes_lock:
        c = await get_client(conn.host, conn.port)
        if c is None:
            return {"status": "error", "message": "Connection failed"}
        try:
            resp = await c.request(command, multiline=multiline)
        except Exception as e:
            log.warning("MES cmd %r failed: %s", command, e)
            # сокет мог умереть — сбросим клиента
            await c.close()
            _clients.pop((conn.host, conn.port), None)
            return {"status": "error", "message": str(e)}
    return {"status": "ok", "command": command, "response": resp}


@app.post("/api/mes/raw")
async def mes_raw(cmd: RawCmd):
    """Произвольная команда (для отладки)."""
    return await _run(cmd, cmd.command)


@app.post("/api/mes/get_recipes")
async def mes_get_recipes(cmd: SideCmd):
    """SENDL / SENDR -> список имён рецептов."""
    if cmd.side not in ("L", "R"):
        return {"status": "error", "message": "side must be L or R"}
    command = "SENDL" if cmd.side == "L" else "SENDR"
    res = await _run(cmd, command, multiline=True)
    if res["status"] == "ok":
        res["recipes"] = parse_recipes(res["response"])
    return res


@app.post("/api/mes/set_recipes")
async def mes_set_recipes(cmd: RecipesCmd):
    """'text2 L;text3 R' -> переключение рецептов."""
    if not cmd.left and not cmd.right:
        return {"status": "error", "message": "left or right required"}
    command = f"{cmd.left};{cmd.right}"
    return await _run(cmd, command)


@app.post("/api/mes/get_num")
async def mes_get_num(cmd: SideCmd):
    """GETLNUM / GETRNUM -> int."""
    if cmd.side not in ("L", "R"):
        return {"status": "error", "message": "side must be L or R"}
    command = "GETLNUM" if cmd.side == "L" else "GETRNUM"
    res = await _run(cmd, command)
    if res["status"] == "ok":
        v = parse_int(res["response"])
        res["value"] = v
        if v is None:
            res["status"] = "error"; res["message"] = "non-numeric response"
    return res


@app.post("/api/mes/get_sum")
async def mes_get_sum(cmd: SideCmd):
    """GETLSUM / GETRSUM -> int."""
    if cmd.side not in ("L", "R"):
        return {"status": "error", "message": "side must be L or R"}
    command = "GETLSUM" if cmd.side == "L" else "GETRSUM"
    res = await _run(cmd, command)
    if res["status"] == "ok":
        v = parse_int(res["response"])
        res["value"] = v
        if v is None:
            res["status"] = "error"; res["message"] = "non-numeric response"
    return res


@app.post("/api/mes/get_status")
async def mes_get_status(cmd: Conn):
    """GETSTATUS -> 0..3."""
    res = await _run(cmd, "GETSTATUS")
    if res["status"] == "ok":
        res["value"] = parse_status(res["response"])
    return res


@app.post("/api/mes/set_num")
async def mes_set_num(cmd: SetNumCmd):
    """SETLNUM;N / SETRNUM;N -> success."""
    if cmd.side not in ("L", "R"):
        return {"status": "error", "message": "side must be L or R"}
    command = f"{'SETLNUM' if cmd.side == 'L' else 'SETRNUM'};{cmd.value}"
    res = await _run(cmd, command)
    if res["status"] == "ok":
        res["ok"] = "success" in res["response"].lower()
    return res


@app.post("/api/mes/clear")
async def mes_clear(cmd: SideCmd):
    """SETCL / SETCR -> success."""
    if cmd.side not in ("L", "R"):
        return {"status": "error", "message": "side must be L or R"}
    command = "SETCL" if cmd.side == "L" else "SETCR"
    res = await _run(cmd, command)
    if res["status"] == "ok":
        res["ok"] = "success" in res["response"].lower()
    return res


@app.post("/api/mes/start_stop")
async def mes_start_stop(cmd: RawCmd):
    """SETSTART / SETSTOP -> 'recv start' / 'recv stop'."""
    if cmd.command not in ("SETSTART", "SETSTOP"):
        return {"status": "error", "message": "command must be SETSTART or SETSTOP"}
    res = await _run(cmd, cmd.command)
    if res["status"] == "ok":
        res["ok"] = "recv" in res["response"].lower()
    return res


@app.post("/api/mes/run")
async def mes_run(cmd: SideCmd):
    """SETPL / SETPR -> LeftRun / RightRun."""
    if cmd.side not in ("L", "R"):
        return {"status": "error", "message": "side must be L or R"}
    command = "SETPL" if cmd.side == "L" else "SETPR"
    res = await _run(cmd, command)
    if res["status"] == "ok":
        expect = "leftrun" if cmd.side == "L" else "rightrun"
        res["ok"] = expect in res["response"].lower().replace(" ", "")
    return res


@app.post("/api/mes/complete")
async def mes_complete(cmd: SideCmd):
    """GetCompleteL / GetCompleteR -> bool."""
    if cmd.side not in ("L", "R"):
        return {"status": "error", "message": "side must be L or R"}
    command = "GetCompleteL" if cmd.side == "L" else "GetCompleteR"
    res = await _run(cmd, command)
    if res["status"] == "ok":
        res["complete"] = parse_complete(res["response"])
    return res


# ---------------------------------------------------------------------------
# WebSocket: циклический опрос статуса
# ---------------------------------------------------------------------------

@app.websocket("/ws/mes")
async def ws_mes(ws: WebSocket):
    await ws.accept()
    task: Optional[asyncio.Task] = None
    cfg: dict = {}

    async def poll_loop():
        while True:
            host = cfg.get("host")
            port = int(cfg.get("port", DEFAULT_PORT))
            rate = max(100, int(cfg.get("rate_ms", 1000))) / 1000.0
            if not host:
                await asyncio.sleep(rate); continue

            payload: dict = {"type": "data", "data": {}, "ts": asyncio.get_event_loop().time()}
            d = payload["data"]
            ok = True

            try:
                async with mes_lock:
                    c = await get_client(host, port)
                    if c is None:
                        ok = False
                    else:
                        # последовательный опрос (один сокет, один лок)
                        d["status"]        = parse_status(await c.request("GETSTATUS"))
                        d["left_num"]      = parse_int(await c.request("GETLNUM"))
                        d["right_num"]     = parse_int(await c.request("GETRNUM"))
                        d["left_sum"]      = parse_int(await c.request("GETLSUM"))
                        d["right_sum"]     = parse_int(await c.request("GETRSUM"))
                        d["left_complete"] = parse_complete(await c.request("GetCompleteL"))
                        d["right_complete"]= parse_complete(await c.request("GetCompleteR"))
            except Exception as e:
                log.warning("MES poll failed: %s", e)
                ok = False
                async with mes_lock:
                    cli = _clients.pop((host, port), None)
                    if cli:
                        try: await cli.close()
                        except Exception: pass

            try:
                if ok:
                    await ws.send_text(json.dumps(payload))
                else:
                    await ws.send_text(json.dumps({"type": "error", "message": "MES read failed"}))
            except Exception:
                return
            await asyncio.sleep(rate)

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            action = msg.get("action")
            if action == "start":
                cfg = {
                    "host": msg.get("host"),
                    "port": int(msg.get("port", DEFAULT_PORT)),
                    "rate_ms": int(msg.get("rate_ms", 1000)),
                }
                if task is None or task.done():
                    task = asyncio.create_task(poll_loop())
            elif action == "stop":
                cfg = {}
                if task is not None:
                    task.cancel(); task = None
            elif action == "update":
                cfg.update({
                    "host": msg.get("host", cfg.get("host")),
                    "port": int(msg.get("port", cfg.get("port", DEFAULT_PORT))),
                    "rate_ms": int(msg.get("rate_ms", cfg.get("rate_ms", 1000))),
                })
    except WebSocketDisconnect:
        pass
    finally:
        if task is not None:
            task.cancel()


# ---------------------------------------------------------------------------
# Экспорт одиночного HTML и главная страница
# ---------------------------------------------------------------------------

@app.post("/api/mes/export")
def export_html(cfg: ExportConfig):
    html = _render_spa(cfg.model_dump())
    return Response(
        content=html, media_type="text/html",
        headers={"Content-Disposition": 'attachment; filename="mes_hmi_export.html"'},
    )


@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(_render_spa(None))


def _render_spa(export_cfg: Optional[dict] = None) -> str:
    cfg_json = json.dumps(export_cfg if export_cfg else None).replace("</", "<\\/")
    return """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>HMI MES Utility — MES клиент</title>
<style>
  body { font-family: Arial, sans-serif; background:#f4f4f9; margin:0; padding:20px; color:#222; }
  .panel { background:#fff; padding:15px; border-radius:8px; box-shadow:0 2px 5px rgba(0,0,0,.1); margin-bottom:20px; }
  .header-panel { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .grid { display:flex; gap:20px; align-items:flex-start; flex-wrap:wrap; }
  .col { flex:1; min-width:380px; }
  .row { display:flex; gap:8px; align-items:center; padding:6px 0; border-bottom:1px solid #eee; flex-wrap:wrap; }
  .row input[type=text], .row input[type=number] { padding:4px 6px; border:1px solid #ccc; border-radius:4px; }
  .indicator { width:18px; height:18px; border-radius:50%; background:#9e9e9e; display:inline-block; flex-shrink:0; }
  .st-green { background:#4CAF50; box-shadow:0 0 6px #4CAF50; }
  .st-yellow { background:#FFC107; box-shadow:0 0 6px #FFC107; }
  .st-red { background:#F44336; box-shadow:0 0 6px #F44336; }
  .st-gray { background:#9e9e9e; }
  .st-blue { background:#2196F3; box-shadow:0 0 6px #2196F3; }
  .val { min-width:70px; text-align:center; font-weight:bold; font-size:15px; }
  .lbl { min-width:220px; color:#555; }
  button { padding:5px 10px; cursor:pointer; border:1px solid #bbb; border-radius:4px; background:#f9f9f9; }
  button:hover:not(:disabled) { background:#e9e9e9; }
  button:disabled { opacity:0.5; cursor:not-allowed; }
  button.primary { background:#2196F3; color:#fff; border-color:#1976D2; }
  button.danger  { background:#F44336; color:#fff; border-color:#c62828; }
  button.ok      { background:#4CAF50; color:#fff; border-color:#388E3C; }
  .hint { color:#666; font-size:12px; margin-bottom:8px; }
  .topbtns { margin-left:auto; display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  .log { font-family: monospace; font-size: 12px; background:#111; color:#0f0; padding:10px; border-radius:6px; max-height:200px; overflow:auto; }
  ul.recipes { list-style:none; padding-left:0; margin:0; }
  ul.recipes li { padding:4px 6px; border-bottom:1px solid #eee; }
  ul.recipes li.active { background:#E3F2FD; font-weight:bold; }
</style>
</head>
<body>

<div class="panel header-panel">
  <h3 style="margin:0">MES клиент HMI</h3>
  <label>IP: <input id="host" type="text" value="192.168.0.100" style="width:140px"></label>
  <label>Порт: <input id="port" type="number" value="10000" style="width:80px"></label>
  <label>Опрос:
    <select id="polling-rate">
      <option value="200">200 мс</option>
      <option value="500">500 мс</option>
      <option value="1000" selected>1000 мс</option>
      <option value="2000">2000 мс</option>
    </select>
  </label>
  <button id="btn-start" onclick="startPolling()" class="primary">Запустить опрос</button>
  <button id="btn-stop" onclick="stopPolling()" disabled>Остановить</button>
  <div id="status-indicator" class="indicator st-red"></div>
  <span id="status-text" style="font-weight:bold">Отключено</span>
  <div class="topbtns">
    <button onclick="exportConfig()">Экспорт конфигурации HMI</button>
  </div>
</div>

<div class="panel">
  <h3 style="margin-top:0">Статус станка</h3>
  <div class="row">
    <span class="lbl">Статус робота (GETSTATUS)</span>
    <span id="st-robot" class="indicator st-gray"></span>
    <span id="st-robot-text" class="val">--</span>
  </div>
  <div class="row"><span class="lbl">Левый счётчик (GETLNUM)</span><span id="l-num" class="val">--</span></div>
  <div class="row"><span class="lbl">Правый счётчик (GETRNUM)</span><span id="r-num" class="val">--</span></div>
  <div class="row"><span class="lbl">Левая сумма (GETLSUM)</span><span id="l-sum" class="val">--</span></div>
  <div class="row"><span class="lbl">Правая сумма (GETRSUM)</span><span id="r-sum" class="val">--</span></div>
  <div class="row">
    <span class="lbl">LeftComplete (GetCompleteL)</span>
    <span id="l-cmpl" class="indicator st-gray"></span>
    <span id="l-cmpl-text" class="val">--</span>
  </div>
  <div class="row">
    <span class="lbl">RightComplete (GetCompleteR)</span>
    <span id="r-cmpl" class="indicator st-gray"></span>
    <span id="r-cmpl-text" class="val">--</span>
  </div>
</div>

<div class="grid">
  <div class="panel col">
    <h3 style="margin-top:0">Рецепты</h3>
    <div class="hint">SENDL/SENDR — запрос списка; выбор + «Применить» — переключение рецепта.</div>
    <div class="row" style="align-items:flex-start">
      <div style="flex:1">
        <div><b>Левые (SENDL)</b></div>
        <ul id="list-left" class="recipes"></ul>
        <button onclick="refreshRecipes('L')">Обновить SENDL</button>
      </div>
      <div style="flex:1">
        <div><b>Правые (SENDR)</b></div>
        <ul id="list-right" class="recipes"></ul>
        <button onclick="refreshRecipes('R')">Обновить SENDR</button>
      </div>
    </div>
    <div class="row" style="margin-top:10px">
      <button onclick="applyRecipes()" class="ok">Применить выбранные (L;R)</button>
    </div>
  </div>

  <div class="panel col">
    <h3 style="margin-top:0">Команды управления</h3>
    <div class="hint">Все действия отправляются на станок и логируются ниже.</div>
    <div class="row">
      <button onclick="cmd('SETSTART')" class="ok">SETSTART</button>
      <button onclick="cmd('SETSTOP')" class="danger">SETSTOP</button>
      <button onclick="cmdRun('L')">SETPL (Left)</button>
      <button onclick="cmdRun('R')">SETPR (Right)</button>
    </div>
    <div class="row">
      <button onclick="cmdClear('L')">SETCL</button>
      <button onclick="cmdClear('R')">SETCR</button>
    </div>
    <div class="row">
      <span>SETLNUM;</span>
      <input id="set-l" type="number" value="0" style="width:80px">
      <button onclick="cmdSetNum('L')">Отправить L</button>
      <span>SETRNUM;</span>
      <input id="set-r" type="number" value="0" style="width:80px">
      <button onclick="cmdSetNum('R')">Отправить R</button>
    </div>
    <div class="row">
      <span>Произвольная команда:</span>
      <input id="raw-cmd" type="text" placeholder="GETSTATUS" style="flex:1;min-width:180px">
      <button onclick="cmdRaw()">Отправить</button>
    </div>
  </div>
</div>

<div class="panel">
  <h3 style="margin-top:0">Журнал</h3>
  <div id="log" class="log"></div>
</div>

<script>
const EMBEDDED = """ + cfg_json + """;
const API_BASE = (EMBEDDED && EMBEDDED.api_base)
  ? EMBEDDED.api_base
  : (location.origin && location.origin !== "null" ? location.origin : "http://127.0.0.1:8001");

let ws = null;
let userStopped = true;
let selectedLeft = null;
let selectedRight = null;

function logLine(s) {
  const el = document.getElementById("log");
  const time = new Date().toLocaleTimeString();
  el.innerHTML = `<div>[${time}] ${s}</div>` + el.innerHTML;
  while (el.childNodes.length > 200) el.removeChild(el.lastChild);
}

function escapeHtml(str) {
  return String(str).replace(/&/g,"&amp;").replace(/"/g,"&quot;")
                    .replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

function conn() {
  return {
    host: document.getElementById("host").value.trim(),
    port: parseInt(document.getElementById("port").value) || 10000,
  };
}

function updateStatus(state) {
  const ind = document.getElementById("status-indicator");
  const txt = document.getElementById("status-text");
  ind.className = "indicator";
  if (state === "ok")      { ind.classList.add("st-green");  txt.innerText = "Соединение установлено"; }
  else if (state === "pending") { ind.classList.add("st-yellow"); txt.innerText = "Ожидание ответа..."; }
  else                     { ind.classList.add("st-red");    txt.innerText = "Ошибка связи / Отключено"; }
}

/* ====== Опрос ====== */
function startPolling() {
  userStopped = false;
  document.getElementById("btn-start").disabled = true;
  document.getElementById("btn-stop").disabled = false;
  const {host, port} = conn();
  const rate = parseInt(document.getElementById("polling-rate").value) || 1000;
  const wsUrl = API_BASE.replace(/^http/, "ws") + "/ws/mes";
  if (ws) { try { ws.close(); } catch(e){} }
  ws = new WebSocket(wsUrl);
  updateStatus("pending");
  ws.onopen = () => ws.send(JSON.stringify({action:"start", host, port, rate_ms:rate}));
  ws.onmessage = (ev) => {
    let msg; try { msg = JSON.parse(ev.data); } catch(e){ return; }
    if (msg.type === "error") { updateStatus("error"); return; }
    if (msg.type !== "data") return;
    updateStatus("ok");
    const d = msg.data;
    setStatus(d.status);
    setVal("l-num", d.left_num);
    setVal("r-num", d.right_num);
    setVal("l-sum", d.left_sum);
    setVal("r-sum", d.right_sum);
    setCmpl("l", d.left_complete);
    setCmpl("r", d.right_complete);
  };
  ws.onerror = () => updateStatus("error");
  ws.onclose = () => { if (!userStopped) updateStatus("error"); };
}

function stopPolling() {
  userStopped = true;
  if (ws) { try { ws.send(JSON.stringify({action:"stop"})); } catch(e){} ws.close(); ws = null; }
  document.getElementById("btn-start").disabled = false;
  document.getElementById("btn-stop").disabled = true;
  updateStatus("error");
}

function setStatus(v) {
  const ind = document.getElementById("st-robot");
  const txt = document.getElementById("st-robot-text");
  ind.className = "indicator";
  const map = {
    0: ["st-gray",   "0 — STOP"],
    1: ["st-yellow", "1 — PAUSE"],
    2: ["st-green",  "2 — RUN"],
    3: ["st-red",    "3 — ALARM"],
  };
  const [cls, label] = map[v] || ["st-gray", "--"];
  ind.classList.add(cls);
  txt.innerText = label;
}

function setVal(id, v) { document.getElementById(id).innerText = (v === null || v === undefined) ? "--" : v; }

function setCmpl(side, v) {
  const ind = document.getElementById(side + "-cmpl");
  const txt = document.getElementById(side + "-cmpl-text");
  ind.className = "indicator";
  if (v === true)  { ind.classList.add("st-green"); txt.innerText = "COMPLETE"; }
  else if (v === false) { ind.classList.add("st-gray"); txt.innerText = "none"; }
  else { ind.classList.add("st-gray"); txt.innerText = "--"; }
}

/* ====== Универсальный вызов API ====== */
async function api(path, body) {
  const {host, port} = conn();
  try {
    const r = await fetch(API_BASE + path, {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({host, port, ...body})
    });
    const res = await r.json();
    logLine(`${path} → ${JSON.stringify(res)}`);
    if (res.status !== "ok") updateStatus("error");
    return res;
  } catch (e) {
    logLine(`${path} → ERROR ${e}`);
    updateStatus("error");
    return {status:"error", message:String(e)};
  }
}

/* ====== Рецепты ====== */
async function refreshRecipes(side) {
  const res = await api("/api/mes/get_recipes", {side});
  const ul = document.getElementById(side === "L" ? "list-left" : "list-right");
  ul.innerHTML = "";
  if (res.status === "ok" && Array.isArray(res.recipes)) {
    res.recipes.forEach(name => {
      const li = document.createElement("li");
      li.innerText = name;
      li.onclick = () => {
        if (side === "L") selectedLeft = name; else selectedRight = name;
        [...ul.children].forEach(c => c.classList.remove("active"));
        li.classList.add("active");
      };
      ul.appendChild(li);
    });
  }
}

async function applyRecipes() {
  if (!selectedLeft && !selectedRight) {
    alert("Выберите хотя бы один рецепт");
    return;
  }
  await api("/api/mes/set_recipes", {left: selectedLeft || "", right: selectedRight || ""});
}

/* ====== Команды ====== */
async function cmd(command) {
  await api("/api/mes/start_stop", {command});
}
async function cmdRun(side) {
  await api("/api/mes/run", {side});
}
async function cmdClear(side) {
  await api("/api/mes/clear", {side});
}
async function cmdSetNum(side) {
  const v = parseInt(document.getElementById(side === "L" ? "set-l" : "set-r").value) || 0;
  await api("/api/mes/set_num", {side, value: v});
}
async function cmdRaw() {
  const command = document.getElementById("raw-cmd").value.trim();
  if (!command) return;
  await api("/api/mes/raw", {command});
}

/* ====== Экспорт ====== */
async function exportConfig() {
  const {host, port} = conn();
  const cfg = {
    api_base: API_BASE, host, port,
    rate_ms: parseInt(document.getElementById("polling-rate").value) || 1000,
    write_log: []
  };
  const r = await fetch(API_BASE + "/api/mes/export", {
    method: "POST", headers: {"Content-Type":"application/json"},
    body: JSON.stringify(cfg)
  });
  const blob = await r.blob();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "mes_hmi_export.html";
  a.click();
  URL.revokeObjectURL(a.href);
}

/* ====== Инициализация ====== */
(function init() {
  if (EMBEDDED) {
    if (EMBEDDED.host) document.getElementById("host").value = EMBEDDED.host;
    if (EMBEDDED.port) document.getElementById("port").value = EMBEDDED.port;
    if (EMBEDDED.rate_ms) document.getElementById("polling-rate").value = EMBEDDED.rate_ms;
  } else {
    try {
      const h = localStorage.getItem("mes.host"); if (h) document.getElementById("host").value = h;
      const p = localStorage.getItem("mes.port"); if (p) document.getElementById("port").value = p;
      const r = localStorage.getItem("mes.rate"); if (r) document.getElementById("polling-rate").value = r;
    } catch(e){}
  }
  // сохраняем при изменениях
  ["host","port","polling-rate"].forEach(id => {
    document.getElementById(id).addEventListener("change", () => {
      try {
        localStorage.setItem("mes.host", document.getElementById("host").value);
        localStorage.setItem("mes.port", document.getElementById("port").value);
        localStorage.setItem("mes.rate", document.getElementById("polling-rate").value);
      } catch(e){}
    });
  });
  logLine("HMI MES готов. Нажмите «Запустить опрос».");
})();
</script>
</body>
</html>
"""