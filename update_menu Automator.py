#!/usr/bin/env python3
import requests
import base64
import json
import os
import sys
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal, InvalidOperation
import pandas as pd

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

ANALYTES = [a.lower() for a in [
    "CBD","CBDa","CBN","THC","THCa","CBDV","CBG","CBGa","Δ8-THC","THCV","CBC",
    "alpha-Pinene","Camphene","Sabinene","beta-Pinene","beta-Myrcene","3-Carene",
    "alpha-Terpinene","p-Cymene","d-Limonene","Eucalyptol","o-cymene","gamma-Terpinene",
    "Sabinene hydrate","Terpinolene","Enochone","Linalool","Fenchol","Isopulegol",
    "Camphor","Isoborneol","Borneol","Menthol","Terpineol","Nerol","Pulegone",
    "Geraniol","Geraniol acetate","alpha-Cedrene","beta-Caryophyllene","alpha-Humulene",
    "Valencene","cis-Nerolidol","trans-Nerolidol","Caryophyllene oxide","Guaio1",
    "Cedrol","alpha-Bisabolol"
]]

REPO_DIR    = Path("/home/ceres/live-menu")
OUTPUT_PATH = REPO_DIR / "menu.json"

# EXCEL SOURCE
RCLONE_REMOTE = "ceres_sharepoint:METRC API Depot/Product Information.xlsx"
LOCAL_EXCEL   = Path("/tmp/Product Information.xlsx")


# ============================================================
#  HELPERS
# ============================================================

def _to_money(v):
    if v is None: return None
    s = str(v).strip().replace("$", "").replace(",", "")
    try:
        return float(Decimal(s))
    except:
        return None


def build_excel_map(path: Path):
    xl = pd.ExcelFile(path, engine="openpyxl")
    sheet = xl.sheet_names[0]
    df = xl.parse(sheet, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    df = df[df["Product"].notna() & (df["Product"].str.strip() != "")]

    mapping = {}
    for _, row in df.iterrows():
        name = str(row["Product"]).strip()
        mapping[name] = {
            "price": _to_money(row.get("Price", "")),
            "type":  (None if pd.isna(row.get("Type")) else str(row.get("Type")).strip())
        }
    return mapping


# ============================================================
#  STEP 0 — DOWNLOAD EXCEL
# ============================================================
try:
    subprocess.run(
        ["rclone", "copyto", RCLONE_REMOTE, str(LOCAL_EXCEL), "--checksum", "--quiet"],
        check=True
    )
    print("STEP 0 SUCCESS: Excel pulled from SharePoint.")
except:
    print("STEP 0 WARNING: Failed to pull Excel, using cached version.")
    if not LOCAL_EXCEL.exists():
        print("STEP 0 ERROR: No Excel file available.")
        sys.exit(1)

excel_map = build_excel_map(LOCAL_EXCEL)


# ============================================================
#  STEP 1 — GET PACKAGES FROM METRC
# ============================================================
print("STEP 1: Pulling packages…")
packages_map = {}
page, page_size = 1, 20

try:
    while True:
        resp = requests.get(
            "https://api-md.metrc.com/packages/v2/active",
            headers=HEADERS,
            params={
                "licenseNumber": LICENSE_NUMBER,
                "pageNumber": page,
                "pageSize": page_size,
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

                    # FIXED: always have a usable timestamp
                    "DateReceived": pkg.get("ReceivedDateTime") or pkg.get("ReceivedDate"),
                    "PackageDate": pkg.get("PackagedDate") or pkg.get("PackageDate"),
                    "CreatedAt": pkg.get("LastModified")
                }

        if page >= data.get("TotalPages", 1):
            break
        page += 1

    print(f"STEP 1 SUCCESS: Found {len(packages_map)} packages.")

except Exception as e:
    print("STEP 1 ERROR:", e)
    sys.exit(1)


# ============================================================
#  STEP 2 — PULL LAB RESULTS
# ============================================================
print("STEP 2: Pulling lab results…")

lab_by_pkg = {pid: [] for pid in packages_map}
total_raw = 0

try:
    for pid in list(packages_map):
        lr = requests.get(
            "https://api-md.metrc.com/labtests/v2/results",
            headers=HEADERS,
            params={"licenseNumber": LICENSE_NUMBER, "packageId": pid},
            timeout=30
        )

        if lr.status_code == 200:
            data = lr.json().get("Data", [])
            total_raw += len(data)

            for rec in data:
                if any(a in (rec.get("TestTypeName", "").lower()) for a in ANALYTES):
                 lab_by_pkg[pid].append({
                    "TestTypeName": rec.get("TestTypeName"),
                    "TestResultLevel": rec.get("TestResultLevel")
                })

        elif lr.status_code != 404:
            lr.raise_for_status()

    print(f"STEP 2 SUCCESS: Pulled {total_raw} lab rows.")

except Exception as e:
    print("STEP 2 ERROR:", e)
    sys.exit(1)

# ============================================================
#  STEP 3 — BUILD FINAL JSON
# ============================================================
print("STEP 3: Building final JSON…")

final = []
for pkg in packages_map.values():
    excel = excel_map.get(pkg["ItemName"], {})

    final.append({
        "Id": pkg["Id"],
        "Label": pkg["Label"],
        "ItemName": pkg["ItemName"],
        "Quantity": pkg["Quantity"],

        # REQUIRED FOR WEBSITE — must always exist (null is OK)
        "DateReceived": pkg.get("DateReceived"),
        "PackageDate": pkg.get("PackageDate") or pkg.get("PackagedDate"),
        "CreatedAt": pkg.get("CreatedAt"),

        # REQUIRED FOR WEBSITE TABLE
        "Type": excel.get("type"),
        "Price": excel.get("price"),

        # LAB DATA
        "LabResults": lab_by_pkg.get(pkg["Id"], [])
    })

# ============================================================
#  STEP 3.25 — SYNC WITH REMOTE (CRITICAL)
# ============================================================
print("STEP 3.25: Syncing with GitHub…")

os.chdir(REPO_DIR)

pull_code = os.system("git pull --rebase origin main")
if pull_code != 0:
    print("STEP 3.25 ERROR: git pull failed")
    sys.exit(1)

# ============================================================
#  WRITE FILE + GIT PUSH
# ============================================================
print("STEP 3.5: Writing file…")

REPO_DIR.mkdir(exist_ok=True, parents=True)

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(final, f, ensure_ascii=False, separators=(",", ":"))

print(f"STEP 3 SUCCESS: {len(final)} items written to {OUTPUT_PATH}")

print("STEP 4: Pushing to GitHub…")

os.chdir(REPO_DIR)
os.system('git add "menu.json"')
commit_msg = f"Auto-update @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
os.system(f'git commit -m "{commit_msg}" || true')
push_code = os.system("git push origin main")
if push_code != 0:
    print("STEP 4 ERROR: git push failed")
    sys.exit(1)

if push_code == 0:
    print("STEP 4 SUCCESS: Changes pushed.")
else:
    print("STEP 4 WARNING: Push exit code:", push_code)
