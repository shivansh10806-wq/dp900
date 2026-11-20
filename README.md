# RetailPulse: Cloud-Native Hybrid Data Platform (Microsoft DP-900 Capstone)

**Author:** Shivansh Paliwal  
**UID:** 24BAI70045 | **Section:** 24AML 2A | **Program:** B.E. CSE (AI & ML)  
**Institution:** Chandigarh University, Gharuan  

## Overview
**RetailPulse** is a real-world capstone implementation aligned with all four domains of the **Microsoft Certified: Azure Data Fundamentals (DP-900)** certification:
1. **Core Data Concepts:** Structured, semi-structured, and unstructured data classification with strict OLTP/OLAP separation.
2. **Relational Data on Azure:** Normalized 3NF POS & inventory schema (`sql/retailpulse_3nf_schema.sql`) and live local SQLite OLTP engine.
3. **Non-Relational Storage & NoSQL:** Partitioned JSON document store (`nosql/telemetry_sample.json`) simulating Azure Cosmos DB + ADLS Gen2 Medallion folder export (`Bronze → Silver → Gold`).
4. **Analytics & Real-Time Streaming:** 5-minute Tumbling Window cold-chain spoilage alert engine (`streaming/cold_chain_alerts.saql`) and live Power BI KPI semantic view.

## Deploy to GitHub & Automatically Build `RetailPulse_DP900.exe`
This repository includes a pre-configured **GitHub Actions** workflow (`.github/workflows/build-windows-exe.yml`) that compiles the standalone Windows `.exe` automatically on GitHub's Windows runners and publishes it under **Releases**:

```bash
git init
git add .
git commit -m "Initial commit: RetailPulse DP-900 Capstone"
git branch -M main
git remote add origin https://github.com/<your-username>/RetailPulse-DP900-Capstone.git
git push -u origin main
```

Once pushed:
1. Open the **Actions** tab in your GitHub repository to watch `Build Windows EXE for RetailPulse DP-900` run (~1 minute).
2. Download `RetailPulse_DP900.exe` directly from the **Releases** page (or from the workflow Artifacts).

## Build `RetailPulse_DP900.exe` Locally on Windows
Double-click `build_exe.bat` (requires Python 3.10+), or run:
```bat
pip install pyinstaller
pyinstaller --onefile --console --name RetailPulse_DP900 retailpulse_app.py
```
The standalone executable will be generated at `dist\RetailPulse_DP900.exe`.

## Run Directly from Python (Windows / macOS / Linux)
```bash
python retailpulse_app.py
```
Your browser will automatically open `http://127.0.0.1:8080`.
