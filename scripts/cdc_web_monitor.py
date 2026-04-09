#!/usr/bin/env python3
import csv
import json
import os
import socket
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

def load_properties(path: Path) -> dict:
    props = {}
    if not path.is_file():
        return props
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(os.environ.get("PIXELS_SPARK_CONFIG", ROOT_DIR / "etc" / "pixels-spark.properties"))
PROPS = load_properties(CONFIG_PATH)
STATE_DIR = Path(PROPS.get("pixels.cdc.state-dir", "/home/ubuntu/disk1/tmp/hybench_sf10_cdc_state"))
LOG_DIR = Path(PROPS.get("pixels.cdc.log-dir", "/home/ubuntu/disk1/tmp/hybench_sf10_cdc_logs"))
METRICS_DIR = Path(PROPS.get("pixels.cdc.metrics-dir", "/home/ubuntu/disk1/tmp/hybench_sf10_cdc_metrics"))
PORT = int(os.environ.get("CDC_WEB_MONITOR_PORT", "8084"))
TABLES = [
    "customer",
    "company",
    "savingaccount",
    "checkingaccount",
    "transfer",
    "checking",
    "loanapps",
    "loantrans",
]
SERVICES = [
    {"name": "HMS", "host": "127.0.0.1", "port": 9083, "link": None},
    {"name": "Trino", "host": "127.0.0.1", "port": 8080, "link": "http://127.0.0.1:8080"},
    {"name": "Pixels Metadata", "host": "127.0.0.1", "port": 18888, "link": None},
    {"name": "Pixels RPC", "host": "127.0.0.1", "port": 9091, "link": None},
    {"name": "Spark History", "host": "127.0.0.1", "port": 18080, "link": "http://127.0.0.1:18080"},
]

INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CDC Monitor</title>
  <style>
    :root {
      --bg: #f4efe7;
      --panel: rgba(255,255,255,0.82);
      --ink: #1e2b23;
      --muted: #5f6f64;
      --accent: #b14d2f;
      --good: #2d7d46;
      --bad: #b42318;
      --line: rgba(30,43,35,0.12);
    }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Helvetica Neue", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(177,77,47,0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(45,125,70,0.12), transparent 24%),
        linear-gradient(135deg, #efe8dc 0%, #f7f3eb 42%, #ece6dc 100%);
    }
    main { max-width: 1280px; margin: 0 auto; padding: 32px 20px 48px; }
    h1 { margin: 0; font-size: 34px; letter-spacing: -0.03em; }
    p { color: var(--muted); }
    .grid { display: grid; gap: 16px; }
    .services { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); margin-top: 20px; }
    .jobs { margin-top: 28px; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 18px 40px rgba(39, 52, 45, 0.06);
      backdrop-filter: blur(8px);
    }
    .card { padding: 16px 18px; }
    .label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }
    .value { margin-top: 8px; font-size: 24px; font-weight: 700; }
    .good { color: var(--good); }
    .bad { color: var(--bad); }
    .row { display: flex; justify-content: space-between; gap: 12px; align-items: center; }
    .mini { font-size: 13px; color: var(--muted); }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 12px 14px; text-align: left; border-top: 1px solid var(--line); font-size: 14px; vertical-align: top; }
    th { font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }
    .header { display: flex; justify-content: space-between; gap: 20px; align-items: end; }
    .links a { color: var(--accent); text-decoration: none; margin-left: 16px; }
    .log { max-width: 460px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    @media (max-width: 900px) {
      .header { display: block; }
      .links a { margin: 0 12px 0 0; display: inline-block; }
      table, thead, tbody, th, td, tr { display: block; }
      thead { display: none; }
      tr { border-top: 1px solid var(--line); padding: 8px 0; }
      td { border-top: none; padding: 6px 14px; }
      td::before {
        content: attr(data-label);
        display: block;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--muted);
        margin-bottom: 3px;
      }
    }
  </style>
</head>
<body>
  <main>
    <section class="header">
      <div>
        <h1>HyBench CDC Control Room</h1>
        <p>本地依赖服务、每表 Spark CDC 作业，以及系统负载的只读监控页。</p>
      </div>
      <div class="links">
        <a href="http://127.0.0.1:18080" target="_blank" rel="noreferrer">Spark History</a>
        <a href="http://127.0.0.1:8080" target="_blank" rel="noreferrer">Trino UI</a>
        <a href="/api/status" target="_blank" rel="noreferrer">Raw JSON</a>
      </div>
    </section>

    <section class="grid services" id="service-grid"></section>

    <section class="panel card jobs">
      <div class="row">
        <div>
          <div class="label">System</div>
          <div class="value" id="system-summary">Loading...</div>
        </div>
        <div class="mini" id="updated-at"></div>
      </div>
    </section>

    <section class="panel card jobs">
      <div class="row">
        <div>
          <div class="label">Spark CDC Jobs</div>
          <div class="value" id="job-count">0 running</div>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th>Table</th>
            <th>Status</th>
            <th>PID</th>
            <th>CPU%</th>
            <th>RSS MB</th>
            <th>Uptime</th>
            <th>Last Signal</th>
          </tr>
        </thead>
        <tbody id="job-body"></tbody>
      </table>
    </section>
  </main>

  <script>
    function fmtUptime(seconds) {
      if (seconds === null || seconds === undefined) return "-";
      const s = Number(seconds);
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      const sec = s % 60;
      return `${h}h ${m}m ${sec}s`;
    }

    function fmtRss(kb) {
      if (kb === null || kb === undefined) return "-";
      return (Number(kb) / 1024).toFixed(1);
    }

    async function refresh() {
      const res = await fetch("/api/status");
      const data = await res.json();

      document.getElementById("updated-at").textContent = `Updated ${data.generated_at}`;
      document.getElementById("system-summary").textContent =
        `load ${data.system.load1 ?? "-"} | mem ${data.system.mem_used_mb ?? "-"} MB used / ${data.system.mem_avail_mb ?? "-"} MB avail | disk ${data.system.disk_used_pct ?? "-"}%`;

      const serviceGrid = document.getElementById("service-grid");
      serviceGrid.innerHTML = "";
      data.services.forEach((svc) => {
        const div = document.createElement("section");
        div.className = "panel card";
        div.innerHTML = `
          <div class="label">${svc.name}</div>
          <div class="value ${svc.status === "up" ? "good" : "bad"}">${svc.status.toUpperCase()}</div>
          <div class="mini">${svc.host}:${svc.port}</div>
          <div class="mini">${svc.link ? `<a href="${svc.link}" target="_blank" rel="noreferrer">Open</a>` : ""}</div>
        `;
        serviceGrid.appendChild(div);
      });

      const running = data.jobs.filter((job) => job.status === "running").length;
      document.getElementById("job-count").textContent = `${running} running / ${data.jobs.length} total`;

      const body = document.getElementById("job-body");
      body.innerHTML = "";
      data.jobs.forEach((job) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td data-label="Table">${job.table}</td>
          <td data-label="Status" class="${job.status === "running" ? "good" : "bad"}">${job.status}</td>
          <td data-label="PID">${job.pid ?? "-"}</td>
          <td data-label="CPU%">${job.cpu ?? "-"}</td>
          <td data-label="RSS MB">${fmtRss(job.rss_kb)}</td>
          <td data-label="Uptime">${fmtUptime(job.etimes)}</td>
          <td data-label="Last Signal" class="log">${job.log_excerpt || "-"}</td>
        `;
        body.appendChild(tr);
      });
    }

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""


def port_open(host, port, timeout=0.5):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


def read_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def read_system_snapshot():
    path = METRICS_DIR / "system.csv"
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return rows[-1] if rows else {}


def read_jobs():
    jobs = []
    for table in TABLES:
        payload = read_json(METRICS_DIR / f"{table}.json")
        payload.setdefault("table", table)
        payload.setdefault("status", "unknown")
        payload.setdefault("pid", None)
        payload.setdefault("cpu", None)
        payload.setdefault("rss_kb", None)
        payload.setdefault("etimes", None)
        payload.setdefault("log_excerpt", "")
        jobs.append(payload)
    return jobs


def read_services():
    rows = []
    for service in SERVICES:
        status = "up" if port_open(service["host"], service["port"]) else "down"
        rows.append({**service, "status": status})
    return rows


def build_status():
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "services": read_services(),
        "jobs": read_jobs(),
        "system": read_system_snapshot(),
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            body = INDEX_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/api/status":
            payload = json.dumps(build_status()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt, *args):
        return


def main():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
