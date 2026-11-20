#!/usr/bin/env python3
"""
RetailPulse: Cloud-Native Hybrid Data Platform Simulator (DP-900 Capstone)
Author: Shivansh Paliwal (UID: 24BAI70045 | Section: 24AML 2A | B.E. CSE AI & ML)
Institution: Chandigarh University, Gharuan

Self-contained, zero-external-dependency Python application (compiles cleanly to
Windows .exe via PyInstaller / GitHub Actions) simulating all four Microsoft
DP-900 (Azure Data Fundamentals) domains:
  1. Core Data Concepts (Structured, Semi-Structured, Unstructured; OLTP vs OLAP)
  2. Relational Data on Azure (3NF Azure SQL Database OLTP engine via SQLite)
  3. Non-Relational Data on Azure (Cosmos DB JSON store + ADLS Gen2 Medallion files)
  4. Analytics & Real-Time Streaming (5-min Tumbling Window spoilage alerts + KPIs)
"""

import csv
import datetime
import json
import os
import random
import sqlite3
import sys
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APP_HOST = "127.0.0.1"
APP_PORT = int(os.environ.get("RETAILPULSE_PORT", "8080"))


def get_base_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


DATA_DIR = os.path.join(get_base_dir(), "retailpulse_data")
DB_PATH = os.path.join(DATA_DIR, "azure_sql_oltp.db")
COSMOS_PATH = os.path.join(DATA_DIR, "cosmos_nosql_store.json")
LAKE_DIR = os.path.join(DATA_DIR, "adls_gen2_medallion")


def init_platform() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    for layer in ("bronze", "silver", "gold"):
        os.makedirs(os.path.join(LAKE_DIR, layer), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS Stores (
            StoreID INTEGER PRIMARY KEY AUTOINCREMENT,
            StoreName TEXT NOT NULL,
            City TEXT NOT NULL,
            Region TEXT NOT NULL,
            OpenedDate TEXT NOT NULL,
            ColdZoneCount INTEGER NOT NULL CHECK (ColdZoneCount >= 0)
        );
        CREATE TABLE IF NOT EXISTS Customers (
            CustomerID INTEGER PRIMARY KEY AUTOINCREMENT,
            FullName TEXT NOT NULL,
            Email TEXT NOT NULL UNIQUE,
            LoyaltyTier TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS Suppliers (
            SupplierID INTEGER PRIMARY KEY AUTOINCREMENT,
            SupplierName TEXT NOT NULL,
            LeadTimeDays INTEGER NOT NULL,
            ContactEmail TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS Products (
            ProductID INTEGER PRIMARY KEY AUTOINCREMENT,
            SupplierID INTEGER NOT NULL REFERENCES Suppliers(SupplierID),
            SKU TEXT NOT NULL UNIQUE,
            ProductName TEXT NOT NULL,
            Category TEXT NOT NULL,
            UnitPrice REAL NOT NULL,
            TempMinC REAL,
            TempMaxC REAL
        );
        CREATE TABLE IF NOT EXISTS SalesOrders (
            OrderID INTEGER PRIMARY KEY AUTOINCREMENT,
            StoreID INTEGER NOT NULL REFERENCES Stores(StoreID),
            CustomerID INTEGER NOT NULL REFERENCES Customers(CustomerID),
            OrderTimestamp TEXT NOT NULL,
            TotalAmount REAL NOT NULL CHECK (TotalAmount >= 0)
        );
        CREATE TABLE IF NOT EXISTS OrderLineItems (
            LineItemID INTEGER PRIMARY KEY AUTOINCREMENT,
            OrderID INTEGER NOT NULL REFERENCES SalesOrders(OrderID),
            ProductID INTEGER NOT NULL REFERENCES Products(ProductID),
            Quantity INTEGER NOT NULL CHECK (Quantity > 0),
            UnitPrice REAL NOT NULL
        );
        CREATE VIEW IF NOT EXISTS vw_DailyStoreSalesSummary AS
        SELECT
            s.StoreName,
            s.Region,
            substr(o.OrderTimestamp, 1, 10) AS OrderDate,
            COUNT(o.OrderID) AS OrderCount,
            ROUND(SUM(o.TotalAmount), 2) AS GrossRevenue
        FROM SalesOrders o
        JOIN Stores s ON o.StoreID = s.StoreID
        GROUP BY s.StoreName, s.Region, substr(o.OrderTimestamp, 1, 10);
        """
    )

    if cur.execute("SELECT COUNT(*) FROM Stores").fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO Stores (StoreName, City, Region, OpenedDate, ColdZoneCount) VALUES (?, ?, ?, ?, ?)",
            [
                ("RetailPulse Chandigarh Central", "Chandigarh", "North", "2023-04-12", 4),
                ("RetailPulse Mohali Fresh", "Mohali", "North", "2024-01-18", 3),
                ("RetailPulse Gurugram Hub", "Gurugram", "NCR", "2022-09-05", 6),
            ],
        )
        cur.executemany(
            "INSERT INTO Customers (FullName, Email, LoyaltyTier) VALUES (?, ?, ?)",
            [
                ("Aarav Sharma", "aarav.sharma@example.com", "Gold"),
                ("Meera Nair", "meera.nair@example.com", "Platinum"),
                ("Rohan Verma", "rohan.verma@example.com", "Silver"),
            ],
        )
        cur.executemany(
            "INSERT INTO Suppliers (SupplierName, LeadTimeDays, ContactEmail) VALUES (?, ?, ?)",
            [
                ("Himalayan Dairy Co-op", 1, "supply@himalayandairy.example"),
                ("ArcticFresh Frozen Logistics", 2, "ops@arcticfresh.example"),
                ("GreenValley Organic Farms", 1, "orders@greenvalley.example"),
            ],
        )
        cur.executemany(
            "INSERT INTO Products (SupplierID, SKU, ProductName, Category, UnitPrice, TempMinC, TempMaxC) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (1, "DRY-1001", "A2 Fresh Milk 1L", "Dairy", 85.0, 1.0, 4.0),
                (1, "DRY-1002", "Greek Yogurt 500g", "Dairy", 140.0, 1.0, 4.0),
                (2, "FRZ-2001", "Frozen Green Peas 1kg", "Frozen", 195.0, -22.0, -15.0),
                (3, "PRD-3001", "Organic Strawberries 250g", "Produce", 160.0, 2.0, 6.0),
            ],
        )
        now = datetime.datetime.utcnow()
        for i in range(6):
            ts = (now - datetime.timedelta(hours=i * 3)).strftime("%Y-%m-%dT%H:%M:%SZ")
            store_id = (i % 3) + 1
            cust_id = (i % 3) + 1
            prod_id = (i % 4) + 1
            qty = (i % 3) + 2
            price = cur.execute("SELECT UnitPrice FROM Products WHERE ProductID=?", (prod_id,)).fetchone()[0]
            total = round(qty * price, 2)
            cur.execute(
                "INSERT INTO SalesOrders (StoreID, CustomerID, OrderTimestamp, TotalAmount) VALUES (?, ?, ?, ?)",
                (store_id, cust_id, ts, total),
            )
            oid = cur.lastrowid
            cur.execute(
                "INSERT INTO OrderLineItems (OrderID, ProductID, Quantity, UnitPrice) VALUES (?, ?, ?, ?)",
                (oid, prod_id, qty, price),
            )
    conn.commit()
    conn.close()

    if not os.path.exists(COSMOS_PATH):
        now = datetime.datetime.utcnow()
        seed_docs = {
            "ColdChainTelemetry": [
                {
                    "id": "evt-seed-001",
                    "storeSensorKey": "STR001_CHILL01",
                    "storeId": 1,
                    "zoneType": "DairyChiller",
                    "timestampUtc": (now - datetime.timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "metrics": {"temperatureC": 3.2, "humidityPct": 64.0, "doorOpenSec": 8},
                    "safeRangeC": {"min": 1.0, "max": 4.0},
                    "ttl": 7776000,
                },
                {
                    "id": "evt-seed-002",
                    "storeSensorKey": "STR002_CHILL02",
                    "storeId": 2,
                    "zoneType": "DairyChiller",
                    "timestampUtc": (now - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "metrics": {"temperatureC": 5.9, "humidityPct": 71.5, "doorOpenSec": 47},
                    "safeRangeC": {"min": 1.0, "max": 4.0},
                    "ttl": 7776000,
                },
                {
                    "id": "evt-seed-003",
                    "storeSensorKey": "STR003_FRZ01",
                    "storeId": 3,
                    "zoneType": "DeepFreezer",
                    "timestampUtc": (now - datetime.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "metrics": {"temperatureC": -12.4, "humidityPct": 58.0, "doorOpenSec": 35},
                    "safeRangeC": {"min": -22.0, "max": -15.0},
                    "ttl": 7776000,
                },
            ],
            "DynamicCatalog": [
                {"id": "cat-DRY-1001", "category": "Dairy", "sku": "DRY-1001", "attributes": {"fatPct": 3.5, "shelfLifeDays": 3, "allergens": ["Milk"]}},
                {"id": "cat-FRZ-2001", "category": "Frozen", "sku": "FRZ-2001", "attributes": {"flashFrozen": True, "shelfLifeDays": 180, "origin": "Punjab"}},
            ],
        }
        with open(COSMOS_PATH, "w", encoding="utf-8") as f:
            json.dump(seed_docs, f, indent=2)


def load_cosmos() -> dict:
    with open(COSMOS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_cosmos(data: dict) -> None:
    with open(COSMOS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def evaluate_stream_alerts(telemetry: list) -> list:
    """Simulates Azure Stream Analytics 5-minute Tumbling Window spoilage detection."""
    buckets = {}
    for evt in telemetry:
        key = (evt["storeSensorKey"], evt["storeId"], evt["zoneType"])
        buckets.setdefault(key, []).append(evt["metrics"]["temperatureC"])

    alerts = []
    for (sensor_key, store_id, zone), temps in buckets.items():
        avg_t = round(sum(temps) / len(temps), 2)
        max_t = round(max(temps), 2)
        breach = (zone == "DairyChiller" and avg_t > 4.0) or (zone == "DeepFreezer" and avg_t > -15.0)
        if breach:
            alerts.append(
                {
                    "storeSensorKey": sensor_key,
                    "storeId": store_id,
                    "zoneType": zone,
                    "avgTempC": avg_t,
                    "maxTempC": max_t,
                    "readingCount": len(temps),
                    "severity": "CRITICAL" if abs(avg_t) > 1.5 else "WARNING",
                }
            )
    return alerts


def run_medallion_elt() -> dict:
    """Simulates Azure Data Factory ELT into ADLS Gen2 Bronze -> Silver -> Gold."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    orders = [dict(r) for r in conn.execute("SELECT * FROM SalesOrders ORDER BY OrderID DESC").fetchall()]
    summary = [dict(r) for r in conn.execute("SELECT * FROM vw_DailyStoreSalesSummary").fetchall()]
    conn.close()

    cosmos = load_cosmos()
    alerts = evaluate_stream_alerts(cosmos["ColdChainTelemetry"])

    # Bronze: Raw JSON dump
    bronze_file = os.path.join(LAKE_DIR, "bronze", "raw_pos_and_telemetry.json")
    with open(bronze_file, "w", encoding="utf-8") as f:
        json.dump({"orders": orders, "telemetry": cosmos["ColdChainTelemetry"]}, f, indent=2)

    # Silver: Cleansed tabular CSV
    silver_file = os.path.join(LAKE_DIR, "silver", "cleansed_sales_orders.csv")
    if orders:
        with open(silver_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=orders[0].keys())
            writer.writeheader()
            writer.writerows(orders)

    # Gold: Curated Star-Schema KPIs & ML Feature Mart
    gold_file = os.path.join(LAKE_DIR, "gold", "gold_retail_kpis.json")
    gold_payload = {
        "generatedAtUtc": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dailyStoreSales": summary,
        "activeColdChainAlerts": alerts,
    }
    with open(gold_file, "w", encoding="utf-8") as f:
        json.dump(gold_payload, f, indent=2)

    return {
        "bronze": bronze_file,
        "silver": silver_file,
        "gold": gold_file,
        "orderCount": len(orders),
        "alertCount": len(alerts),
    }


def get_dashboard_state() -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    stores = [dict(r) for r in conn.execute("SELECT * FROM Stores").fetchall()]
    products = [dict(r) for r in conn.execute("SELECT * FROM Products").fetchall()]
    summary = [dict(r) for r in conn.execute("SELECT * FROM vw_DailyStoreSalesSummary").fetchall()]
    recent_orders = [
        dict(r)
        for r in conn.execute(
            """
            SELECT o.OrderID, s.StoreName, p.ProductName, li.Quantity, o.TotalAmount, o.OrderTimestamp
            FROM SalesOrders o
            JOIN Stores s ON o.StoreID = s.StoreID
            JOIN OrderLineItems li ON o.OrderID = li.OrderID
            JOIN Products p ON li.ProductID = p.ProductID
            ORDER BY o.OrderID DESC LIMIT 12
            """
        ).fetchall()
    ]
    total_rev = conn.execute("SELECT ROUND(COALESCE(SUM(TotalAmount), 0), 2) FROM SalesOrders").fetchone()[0]
    total_orders = conn.execute("SELECT COUNT(*) FROM SalesOrders").fetchone()[0]
    conn.close()

    cosmos = load_cosmos()
    alerts = evaluate_stream_alerts(cosmos["ColdChainTelemetry"])
    return {
        "kpis": {
            "totalRevenueINR": total_rev,
            "totalOrders": total_orders,
            "telemetryDocs": len(cosmos["ColdChainTelemetry"]),
            "activeAlerts": len(alerts),
        },
        "stores": stores,
        "products": products,
        "summary": summary,
        "recentOrders": recent_orders,
        "telemetry": cosmos["ColdChainTelemetry"][-10:][::-1],
        "alerts": alerts,
    }


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>RetailPulse DP-900 Azure Data Platform Simulator</title>
<style>
  body { font-family: 'Segoe UI', system-ui, sans-serif; margin: 0; background: #f1f5f9; color: #0f172a; }
  header { background: #1e3a5f; color: #fff; padding: 18px 28px; display: flex; justify-content: space-between; align-items: center; }
  header h1 { margin: 0; font-size: 20px; }
  header span { font-size: 13px; color: #cbd5e1; }
  .wrap { max-width: 1180px; margin: 22px auto; padding: 0 20px; }
  .kpis { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 20px; }
  .card { background: #fff; border: 1px solid #cbd5e1; border-radius: 8px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
  .kpi-val { font-size: 26px; font-weight: 700; color: #1e3a5f; margin-top: 6px; }
  .kpi-lbl { font-size: 12px; color: #475569; text-transform: uppercase; letter-spacing: 0.04em; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-bottom: 18px; }
  h2 { font-size: 15px; margin: 0 0 12px 0; color: #1e3a5f; border-bottom: 2px solid #e2e8f0; padding-bottom: 6px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 7px 8px; border-bottom: 1px solid #e2e8f0; text-align: left; }
  th { background: #f8fafc; font-weight: 600; color: #334155; }
  .badge-alert { background: #fee2e2; color: #991b1b; padding: 2px 8px; border-radius: 12px; font-weight: 600; font-size: 12px; }
  .badge-ok { background: #dcfce7; color: #166534; padding: 2px 8px; border-radius: 12px; font-weight: 600; font-size: 12px; }
  .controls { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-bottom: 12px; }
  select, input, button { padding: 7px 10px; border-radius: 6px; border: 1px solid #94a3b8; font-size: 13px; }
  button { background: #1e3a5f; color: #fff; border: none; cursor: pointer; font-weight: 600; }
  button:hover { background: #0f2942; }
  button.secondary { background: #0f766e; }
  #statusMsg { margin-top: 8px; font-size: 13px; color: #0f766e; font-weight: 600; }
</style>
</head>
<body>
<header>
  <div>
    <h1>RetailPulse &ndash; Microsoft Azure DP-900 Capstone Simulator</h1>
    <span>Shivansh Paliwal | UID: 24BAI70045 | Section: 24AML 2A | B.E. CSE (AI &amp; ML), Chandigarh University</span>
  </div>
  <button class="secondary" onclick="runELT()">Run ADF Medallion ELT (Bronze &rarr; Gold)</button>
</header>
<div class="wrap">
  <div class="kpis">
    <div class="card"><div class="kpi-lbl">Azure SQL Gross Revenue</div><div class="kpi-val" id="kRev">--</div></div>
    <div class="card"><div class="kpi-lbl">3NF OLTP Sales Orders</div><div class="kpi-val" id="kOrd">--</div></div>
    <div class="card"><div class="kpi-lbl">Cosmos DB IoT Documents</div><div class="kpi-val" id="kTel">--</div></div>
    <div class="card"><div class="kpi-lbl">Stream Analytics Alerts</div><div class="kpi-val" id="kAlt" style="color:#b91c1c;">--</div></div>
  </div>
  <div id="statusMsg"></div>

  <div class="grid">
    <div class="card">
      <h2>Domain 2: Azure SQL Database (3NF OLTP Checkout)</h2>
      <div class="controls">
        <select id="selStore"></select>
        <select id="selProd"></select>
        <input id="inpQty" type="number" min="1" value="2" style="width:64px;">
        <button onclick="submitOrder()">Record POS Order</button>
      </div>
      <table><thead><tr><th>ID</th><th>Store</th><th>Product</th><th>Qty</th><th>Amount</th></tr></thead><tbody id="tblOrders"></tbody></table>
    </div>

    <div class="card">
      <h2>Domain 3 &amp; 4: Event Hubs + Stream Analytics Spoilage Alerts</h2>
      <div class="controls">
        <button onclick="emitTelemetry(false)">Emit Normal Sensor Event</button>
        <button style="background:#b91c1c;" onclick="emitTelemetry(true)">Simulate Chiller Breach</button>
      </div>
      <table><thead><tr><th>Sensor Key</th><th>Zone</th><th>Avg &deg;C</th><th>Max &deg;C</th><th>Status</th></tr></thead><tbody id="tblAlerts"></tbody></table>
    </div>
  </div>

  <div class="card">
    <h2>Domain 4: Power BI Semantic Model Preview (vw_DailyStoreSalesSummary)</h2>
    <table><thead><tr><th>Store Name</th><th>Region</th><th>Order Date</th><th>Orders</th><th>Gross Revenue (INR)</th></tr></thead><tbody id="tblSummary"></tbody></table>
  </div>
</div>

<script>
async function refresh() {
  const res = await fetch('/api/state');
  const d = await res.json();
  document.getElementById('kRev').textContent = 'INR ' + d.kpis.totalRevenueINR.toLocaleString();
  document.getElementById('kOrd').textContent = d.kpis.totalOrders;
  document.getElementById('kTel').textContent = d.kpis.telemetryDocs;
  document.getElementById('kAlt').textContent = d.kpis.activeAlerts;

  const ss = document.getElementById('selStore');
  if (!ss.options.length) d.stores.forEach(s => ss.add(new Option(s.StoreName, s.StoreID)));
  const sp = document.getElementById('selProd');
  if (!sp.options.length) d.products.forEach(p => sp.add(new Option(p.ProductName + ' (INR ' + p.UnitPrice + ')', p.ProductID)));

  document.getElementById('tblOrders').innerHTML = d.recentOrders.map(o =>
    `<tr><td>#${o.OrderID}</td><td>${o.StoreName}</td><td>${o.ProductName}</td><td>${o.Quantity}</td><td>INR ${o.TotalAmount}</td></tr>`
  ).join('');

  document.getElementById('tblAlerts').innerHTML = d.alerts.length
    ? d.alerts.map(a => `<tr><td>${a.storeSensorKey}</td><td>${a.zoneType}</td><td>${a.avgTempC}&deg;C</td><td>${a.maxTempC}&deg;C</td><td><span class="badge-alert">${a.severity}</span></td></tr>`).join('')
    : `<tr><td colspan="5"><span class="badge-ok">All Cold-Chain Zones Nominal</span></td></tr>`;

  document.getElementById('tblSummary').innerHTML = d.summary.map(s =>
    `<tr><td>${s.StoreName}</td><td>${s.Region}</td><td>${s.OrderDate}</td><td>${s.OrderCount}</td><td>INR ${s.GrossRevenue}</td></tr>`
  ).join('');
}

async function submitOrder() {
  const payload = {
    storeId: parseInt(document.getElementById('selStore').value),
    productId: parseInt(document.getElementById('selProd').value),
    quantity: parseInt(document.getElementById('inpQty').value)
  };
  await fetch('/api/order', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
  document.getElementById('statusMsg').textContent = 'Committed ACID 3NF POS order to Azure SQL simulator.';
  refresh();
}

async function emitTelemetry(breach) {
  await fetch('/api/telemetry', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ breach }) });
  document.getElementById('statusMsg').textContent = breach ? 'Injected cold-chain breach event into Cosmos DB & Stream Analytics.' : 'Injected nominal IoT sensor reading.';
  refresh();
}

async function runELT() {
  const res = await fetch('/api/elt', { method: 'POST' });
  const r = await res.json();
  document.getElementById('statusMsg').textContent = `Medallion ELT complete: exported Bronze/Silver/Gold files (${r.orderCount} orders, ${r.alertCount} alerts) to ADLS Gen2 folder.`;
}

refresh();
</script>
</body>
</html>
"""


class RetailPulseHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return  # Keep console clean

    def _send_json(self, obj: dict, status: int = 200) -> None:
        raw = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/" or path == "/index.html":
            raw = DASHBOARD_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        elif path == "/api/state":
            self._send_json(get_dashboard_state())
        else:
            self._send_json({"error": "Not found"}, 404)

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}

        if path == "/api/order":
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            price = cur.execute("SELECT UnitPrice FROM Products WHERE ProductID=?", (body["productId"],)).fetchone()[0]
            qty = max(1, int(body.get("quantity", 1)))
            total = round(price * qty, 2)
            ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            cur.execute(
                "INSERT INTO SalesOrders (StoreID, CustomerID, OrderTimestamp, TotalAmount) VALUES (?, 1, ?, ?)",
                (int(body["storeId"]), ts, total),
            )
            oid = cur.lastrowid
            cur.execute(
                "INSERT INTO OrderLineItems (OrderID, ProductID, Quantity, UnitPrice) VALUES (?, ?, ?, ?)",
                (oid, int(body["productId"]), qty, price),
            )
            conn.commit()
            conn.close()
            self._send_json({"status": "ok", "orderId": oid, "totalAmount": total})

        elif path == "/api/telemetry":
            breach = bool(body.get("breach", False))
            cosmos = load_cosmos()
            temp = round(random.uniform(5.2, 8.4), 1) if breach else round(random.uniform(1.5, 3.6), 1)
            doc = {
                "id": f"evt-{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
                "storeSensorKey": "STR002_CHILL02" if breach else "STR001_CHILL01",
                "storeId": 2 if breach else 1,
                "zoneType": "DairyChiller",
                "timestampUtc": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "metrics": {"temperatureC": temp, "humidityPct": round(random.uniform(60, 74), 1), "doorOpenSec": 45 if breach else 6},
                "safeRangeC": {"min": 1.0, "max": 4.0},
                "ttl": 7776000,
            }
            cosmos["ColdChainTelemetry"].append(doc)
            save_cosmos(cosmos)
            self._send_json({"status": "ok", "event": doc})

        elif path == "/api/elt":
            res = run_medallion_elt()
            self._send_json(res)
        else:
            self._send_json({"error": "Not found"}, 404)


def main():
    init_platform()
    if "--self-test" in sys.argv:
        elt = run_medallion_elt()
        state = get_dashboard_state()
        print("SELF-TEST OK:", json.dumps({"kpis": state["kpis"], "eltOrders": elt["orderCount"], "eltAlerts": elt["alertCount"]}))
        return

    server = ThreadingHTTPServer((APP_HOST, APP_PORT), RetailPulseHandler)
    url = f"http://{APP_HOST}:{APP_PORT}"
    print("=" * 64)
    print("  RetailPulse - Microsoft Azure DP-900 Capstone Simulator")
    print("  Author: Shivansh Paliwal (UID: 24BAI70045 | Section: 24AML 2A)")
    print(f"  Dashboard URL: {url}")
    print("  Press Ctrl+C to stop.")
    print("=" * 64)
    threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down RetailPulse...")
        server.server_close()


if __name__ == "__main__":
    main()
