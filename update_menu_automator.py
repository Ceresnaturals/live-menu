#!/usr/bin/env python3
import requests
import base64
import json
import os
import sys
import subprocess
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
import pandas as pd

print("RUNNING REPO SCRIPT:", __file__)

# ============================================================
#  METRC API AUTH
# ============================================================
ENV_FILE = "/etc/ceres/metrc.env"
env = {}
with open(ENV_FILE, "r", encoding="utf-8") as f:
    for line in f:
        if "=" in line and not line.startswith("#"):
            k, v = line.strip().split("=", 1)
            env[k] = v

VENDOR_KEY     = env["METRC_VENDOR_KEY"]
USER_KEY       = env["METRC_USER_KEY"]
LICENSE_NUMBER = env["METRC_LICENSE"]

auth = base64.b64encode(f"{VENDOR_KEY}:{USER_KEY}".encode()).decode()
HEADERS = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}

# ============================================================
#  CONSTANTS
# ============================================================
START = "2020-01-01T00:00:00Z"
END   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

ROOMS = {"vault - finished goods", "low inventory"}

REPO_DIR    = Path("/home/ceres/live-menu")
OUTPUT_PATH = REPO_DIR / "menu.json"

RCLONE_REMOTE = "ceres_sharepoint:METRC API Depot/Product Information.xlsx"
LOCAL_EXCEL   = Path("/tmp/Product Information.xlsx")

# ============================================================
#  HELPERS
# ============================================================

def file_hash(path: Path):
    if not path.exists():
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _to_money(v):
    if v is None:
        return None
    s = str(v).strip().replace("$", "").replace(",", "")
    try:
        return float(Decimal(s))
    except:
        return None


def build_excel_map(path: Path):
    xl = pd.ExcelFile(path, engine="openpyxl")

    # ---------- PRODUCTS TABLE ----------
    sheet = xl.sheet_names[0]
    df = xl.parse(sheet, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    df = df[df["Product"].notna() & (df["Product"].str.strip() != "")]

    product_map = {}

    for _, row in df.iterrows():
        name = str(row["Product"]).strip()

        bulk_raw = row.get("BulkPricing")
        bulk = None
        if bulk_raw and not pd.isna(bulk_raw):
            try:
                bulk = json.loads(str(bulk_raw))
            except:
                bulk = None

        product_map[name] = {
            "price": _to_money(row.get("Price", "")),
            "type":  (None if pd.isna(row.get("Type")) else str(row.get("Type")).strip()),
            "bulk_pricing": bulk
        }

    # ---------- BULK RULES TABLE ----------
    bulk_rules = []

    if "BulkRules" in xl.sheet_names:
        br_df = xl.parse("BulkRules", dtype=str)
        br_df.columns = [c.strip() for c in br_df.columns]

        for _, row in br_df.iterrows():
            pg = row.get("ProductGroup")
            mq = row.get("MinQty")
            pr = row.get("Price")

            if pg and mq and pr:
                bulk_rules.append({
                    "ProductGroup": str(pg).strip(),
                    "MinQty": int(mq),
                    "Price": _to_money(pr)
                })

    return product_map, bulk_rules

# ============================================================
#  STEP 0 — DOWNLOAD EXCEL
# ============================================================
subprocess.run(
    ["rclone", "copyto", RCLONE_REMOTE, str(LOCAL_EXCEL), "--checksum", "--quiet"],
    check=False
)

excel_map, bulk_rules = build_excel_map(LOCAL_EXCEL)

# ============================================================
#  STEP 1 — GET PACKAGES
# ============================================================
packages_map = {}
page = 1

while True:
    resp = requests.get(
        "https://api-md.metrc.com/packages/v2/active",
        headers=HEADERS,
        params={
            "licenseNumber": LICENSE_NUMBER,
            "pageNumber": page,
            "pageSize": 20,
            "lastModifiedStart": START,
            "lastModifiedEnd": END
        },
        timeout=30
    )
    resp.raise_for_status()
    data = resp.json()

    for pkg in data.get("Data", []):
        if pkg.get("LocationName", "").lower() in ROOMS:

            item_name = None
            if isinstance(pkg.get("Item"), dict):
                item_name = pkg["Item"].get("Name")
            if not item_name:
                item_name = pkg.get("ItemName")

            packages_map[pkg["Id"]] = {
                "Id": pkg["Id"],
                "Label": pkg.get("Label"),
                "ItemName": str(item_name).strip(),
                "Quantity": pkg.get("Quantity"),
                "Type": excel_map.get(str(item_name).strip(), {}).get("type"),
                "Price": excel_map.get(str(item_name).strip(), {}).get("price")
            }

    if page >= data.get("TotalPages", 1):
        break
    page += 1

# ============================================================
#  STEP 2 — PULL LAB RESULTS (THC / CBD / TERPENES)
# ============================================================
ANALYTES = ["thc","thca","cbd","cbda","cbg","cbc","cbn","limonene","myrcene","pinene","linalool","caryophyllene"]

lab_by_pkg = {pid: [] for pid in packages_map}

for pid in packages_map:
    lr = requests.get(
        "https://api-md.metrc.com/labtests/v2/results",
        headers=HEADERS,
        params={"licenseNumber": LICENSE_NUMBER, "packageId": pid},
        timeout=30
    )

    if lr.status_code == 200:
        for rec in lr.json().get("Data", []):
            name = (rec.get("TestTypeName") or "").lower()
            if any(a in name for a in ANALYTES):
                lab_by_pkg[pid].append({
                    "TestTypeName": rec.get("TestTypeName"),
                    "TestResultLevel": rec.get("TestResultLevel")
                })

# Ensure stable lab ordering
for pid in lab_by_pkg:
    lab_by_pkg[pid] = sorted(lab_by_pkg[pid], key=lambda x: x["TestTypeName"] or "")

# ============================================================
#  BUILD FINAL JSON
# ============================================================
final = []

for pkg in sorted(packages_map.values(), key=lambda x: x["Id"]):
    final.append({
        "Id": pkg["Id"],
        "Label": pkg["Label"],
        "ItemName": pkg["ItemName"],
        "Quantity": pkg["Quantity"],
        "Type": pkg["Type"],
        "Price": pkg["Price"],
        "LabResults": lab_by_pkg.get(pkg["Id"], [])
    })

payload = {
    "items": final,
    "bulkRules": bulk_rules
}

new_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

# ============================================================
#  CHANGE DETECTION
# ============================================================
old_hash = file_hash(OUTPUT_PATH)
new_hash = hashlib.sha256(new_json.encode("utf-8")).hexdigest()

if old_hash == new_hash:
    print("NO INVENTORY CHANGE — exiting.")
    sys.exit(0)

print("CHANGE DETECTED — updating repo.")

# ============================================================
#  GIT SYNC + WRITE + PUSH
# ============================================================
os.chdir(REPO_DIR)

if os.system("git pull --rebase origin main") != 0:
    print("GIT PULL FAILED")
    sys.exit(1)

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write(new_json)

os.system('git add menu.json')
os.system(f'git commit -m "Auto-update @ {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"')
os.system("git push origin main")

print("SUCCESS:")
