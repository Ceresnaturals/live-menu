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

TRACKED_ROOMS = {
    "vault - finished goods",
    "vault - bulk",
    "vault - bulk wip",
    "on hold"
}

MENU_ROOMS = {
    "vault - finished goods",
    "low inventory"
}

WATCHED_ROOMS = TRACKED_ROOMS | MENU_ROOMS

REPO_DIR    = Path("/home/ceres/live-menu")
OUTPUT_PATH = REPO_DIR / "menu_v2_collector_temp.json"

RCLONE_REMOTE = "ceres_sharepoint:METRC API Depot/Product Information.xlsx"
LOCAL_EXCEL   = Path("/tmp/Product Information.xlsx")

# LAB CACHE
LAB_CACHE_PATH = Path("/home/ceres/cache/lab_results_library_v2.json")
LAB_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

WATCHED_INVENTORY_PATH = Path("/home/ceres/cache/watched_inventory_v2.json")
WATCHED_INVENTORY_PATH.parent.mkdir(parents=True, exist_ok=True)

# ============================================================
#  SNAPSHOT FOR CHANGE DETECTION
# ============================================================
SNAPSHOT_PATH = Path("/home/ceres/cache/tracked_inventory_snapshot_v2.json")
SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)


def load_snapshot():
    if SNAPSHOT_PATH.exists():
        try:
            with open(SNAPSHOT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {"packages": {}}


def save_snapshot(snapshot):
    with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)


def save_watched_inventory(watched_packages_map):
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "packages": {str(k): v for k, v in watched_packages_map.items()}
    }
    with open(WATCHED_INVENTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# ============================================================
#  LAB HASH
# ============================================================

def compute_lab_hash(lab_results):
    if not lab_results:
        return None

    normalized = []
    for r in lab_results:
        normalized.append({
            "name": r.get("TestTypeName"),
            "value": r.get("TestResultLevel")
        })

    normalized = sorted(normalized, key=lambda x: x["name"] or "")

    return hashlib.md5(json.dumps(normalized, sort_keys=True).encode()).hexdigest()

# ============================================================
#  HELPERS
# ============================================================

def load_lab_cache():
    if LAB_CACHE_PATH.exists():
        try:
            with open(LAB_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {}


def save_lab_cache(cache):
    with open(LAB_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)


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

    sheet = "ProductInfo" if "ProductInfo" in xl.sheet_names else xl.sheet_names[0]
    df = xl.parse(sheet, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    df = df[df["Product"].notna() & (df["Product"].str.strip() != "")]

    product_map = {}
    bulk_rules = []

    for _, row in df.iterrows():
        name = str(row["Product"]).strip()

        product_map[name] = {
            "price": _to_money(row.get("Price", "")),
            "type":  (None if pd.isna(row.get("Type")) else str(row.get("Type")).strip())
        }

        bulk_raw = row.get("BulkPricing")

        if bulk_raw and not pd.isna(bulk_raw):
            try:
                tiers = json.loads(str(bulk_raw))
                if isinstance(tiers, list):
                    for tier in tiers:
                        if tier.get("minQty") and tier.get("price"):
                            bulk_rules.append({
                                "Scope": "Item",
                                "Key": name,
                                "MinQty": int(tier["minQty"]),
                                "Price": float(tier["price"])
                            })
            except:
                pass

    if "BulkPrice" in xl.sheet_names:
        br_df = xl.parse("BulkPrice", dtype=str)
        br_df.columns = [c.strip() for c in br_df.columns]

        for _, row in br_df.iterrows():
            if row.get("ProductGroup") and row.get("MinQty") and row.get("Price"):
                bulk_rules.append({
                    "Scope": "Group",
                    "Key": str(row["ProductGroup"]).strip(),
                    "MinQty": int(row["MinQty"]),
                    "Price": _to_money(row["Price"])
                })

    return product_map, bulk_rules

# ============================================================
#  STEP 0 — DOWNLOAD EXCEL
# ============================================================
subprocess.run([
    "rclone", "copyto", RCLONE_REMOTE, str(LOCAL_EXCEL), "--checksum", "--quiet"
], check=False)

excel_map, bulk_rules = build_excel_map(LOCAL_EXCEL)

# ============================================================
#  STEP 1 — GET PACKAGES
# ============================================================
watched_packages_map = {}
tracked_packages_map = {}
menu_packages_map = {}
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
        if (pkg.get("LocationName") or "").lower() in WATCHED_ROOMS:
            item_name = None
            if isinstance(pkg.get("Item"), dict):
                item_name = pkg["Item"].get("Name")
            if not item_name:
                item_name = pkg.get("ItemName")

            pkg_obj = {
                "Id": pkg["Id"],
                "Label": pkg.get("Label"),
                "ItemName": str(item_name).strip(),
                "Quantity": pkg.get("Quantity"),
                "LocationName": (
                    pkg.get("LocationName")
                    or pkg.get("CurrentLocationName")
                    or (pkg.get("Location") or {}).get("Name")
                    or pkg.get("Location")
                ),
                "Type": excel_map.get(str(item_name).strip(), {}).get("type"),
                "Price": excel_map.get(str(item_name).strip(), {}).get("price")
            }

            room_name = (pkg.get("LocationName") or "").lower()

            watched_packages_map[pkg["Id"]] = pkg_obj

            if room_name in TRACKED_ROOMS:
                tracked_packages_map[pkg["Id"]] = pkg_obj

            if room_name in MENU_ROOMS:
                menu_packages_map[pkg["Id"]] = pkg_obj

    if page >= data.get("TotalPages", 1):
        break
    page += 1

save_watched_inventory(watched_packages_map)

# ============================================================
#  STEP 2 — LAB RESULTS WITH CACHE 
# ============================================================
ANALYTES = ["thc","thca","cbd","cbda","cbg","cbc","cbn","limonene","myrcene","pinene","linalool","caryophyllene"]

lab_cache = load_lab_cache()
snapshot = load_snapshot()
old_packages = snapshot.get("packages", {})

lab_by_pkg = {}

for pid in tracked_packages_map:
    pid_str = str(pid)

    if pid_str in lab_cache:
        lab_by_pkg[pid] = lab_cache[pid_str]
        continue

    # only fetch if missing (new or uncached)
    lr = requests.get(
        "https://api-md.metrc.com/labtests/v2/results",
        headers=HEADERS,
        params={
            "licenseNumber": LICENSE_NUMBER,
            "packageId": pid
        },
        timeout=30
    )

    results = []

    if lr.status_code == 200:
        for rec in lr.json().get("Data", []):
            name = (rec.get("TestTypeName") or "").lower()
            if any(a in name for a in ANALYTES):
                results.append({
                    "TestTypeName": rec.get("TestTypeName"),
                    "TestResultLevel": rec.get("TestResultLevel")
                })

    results = sorted(results, key=lambda x: x["TestTypeName"] or "")

    lab_by_pkg[pid] = results
    lab_cache[pid_str] = results

# clean cache
current_pkg_ids = {str(pid) for pid in tracked_packages_map}
lab_cache = {pid: labs for pid, labs in lab_cache.items() if pid in current_pkg_ids}
save_lab_cache(lab_cache)

# ============================================================
#  BUILD FINAL JSON
# ============================================================
final = []

for pkg in sorted(menu_packages_map.values(), key=lambda x: x["Id"]):    
    final.append({
        "Id": pkg["Id"],
        "Label": pkg["Label"],
        "ItemName": pkg["ItemName"],
        "Quantity": pkg["Quantity"],
        "LocationName": pkg.get("LocationName"),
        "Type": pkg["Type"],
        "Price": pkg["Price"],
        "LabResults": lab_by_pkg.get(pkg["Id"], [])
    })

payload = {"items": final, "bulkRules": bulk_rules}
new_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

# ============================================================
#  CHANGE DETECTION
# ============================================================
new_packages = {}
changes_detected = False
now = datetime.now(timezone.utc).isoformat()

for pkg in tracked_packages_map.values():
    label = str(pkg["Id"])

    lab_hash = compute_lab_hash(lab_by_pkg.get(pkg["Id"], []))

    new_packages[label] = {
        "item": pkg["ItemName"],
        "qty": pkg["Quantity"],
        "lab_hash": lab_hash,
        "last_seen": now
    }

    if label not in old_packages:
        changes_detected = True
    else:
        old = old_packages[label]
        if old.get("qty") != pkg["Quantity"]:
            changes_detected = True
        if lab_hash and old.get("lab_hash") != lab_hash:
            changes_detected = True

for label in old_packages:
    if label not in new_packages:
        changes_detected = True

if not changes_detected:
    print("NO CHANGES DETECTED — collector internal files refreshed; publisher can rebuild menu_v2.json")

# ============================================================
#  WRITE TEMP COLLECTOR OUTPUT ONLY
# ============================================================
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write(new_json)

print(f"WROTE TEMP COLLECTOR OUTPUT: {OUTPUT_PATH}")
# ============================================================
#  SAVE SNAPSHOT 
# ============================================================
snapshot["packages"] = new_packages
save_snapshot(snapshot)

print("SUCCESS:")
