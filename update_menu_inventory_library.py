#!/usr/bin/env python3
import requests
import base64
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

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

LAB_CACHE_PATH = Path("/home/ceres/cache/lab_results_library_v2.json")
LAB_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

WATCHED_INVENTORY_PATH = Path("/home/ceres/cache/watched_inventory_v2.json")
WATCHED_INVENTORY_PATH.parent.mkdir(parents=True, exist_ok=True)

SNAPSHOT_PATH = Path("/home/ceres/cache/tracked_inventory_snapshot_v2.json")
SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ============================================================
# HELPERS
# ============================================================

def load_snapshot():
    if SNAPSHOT_PATH.exists():
        try:
            return json.loads(SNAPSHOT_PATH.read_text())
        except:
            pass
    return {"packages": {}}


def save_snapshot(snapshot):
    SNAPSHOT_PATH.write_text(json.dumps(snapshot, indent=2))


def save_watched_inventory(data):
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "packages": {str(k): v for k, v in data.items()}
    }
    WATCHED_INVENTORY_PATH.write_text(json.dumps(payload, indent=2))


def load_lab_cache():
    if LAB_CACHE_PATH.exists():
        try:
            return json.loads(LAB_CACHE_PATH.read_text())
        except:
            pass
    return {}


def save_lab_cache(cache):
    LAB_CACHE_PATH.write_text(json.dumps(cache))


def compute_lab_hash(lab_results):
    if not lab_results:
        return None
    return hashlib.md5(json.dumps(lab_results, sort_keys=True).encode()).hexdigest()

# ============================================================
#  STEP 1 — GET PACKAGES
# ============================================================
watched_packages_map = {}
tracked_packages_map = {}
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
        room = str(pkg.get("LocationName") or "").lower()

        if room in WATCHED_ROOMS:
            item_name = pkg.get("ItemName") or (pkg.get("Item") or {}).get("Name")

            obj = {
                "Id": pkg["Id"],
                "Label": pkg.get("Label"),
                "ItemName": str(item_name).strip(),
                "Quantity": pkg.get("Quantity"),
                "LocationName": pkg.get("LocationName")
            }

            watched_packages_map[pkg["Id"]] = obj

            if room in TRACKED_ROOMS:
                tracked_packages_map[pkg["Id"]] = obj

    if page >= data.get("TotalPages", 1):
        break
    page += 1

save_watched_inventory(watched_packages_map)

# ============================================================
# LAB CACHE
# ============================================================
lab_cache = load_lab_cache()
snapshot = load_snapshot()
old = snapshot.get("packages", {})

lab_by_pkg = {}

for pid in tracked_packages_map:
    pid_str = str(pid)

    if pid_str in lab_cache:
        lab_by_pkg[pid] = lab_cache[pid_str]
        continue

    r = requests.get(
        "https://api-md.metrc.com/labtests/v2/results",
        headers=HEADERS,
        params={"licenseNumber": LICENSE_NUMBER, "packageId": pid},
        timeout=30
    )

    results = r.json().get("Data", []) if r.status_code == 200 else []
    lab_by_pkg[pid] = results
    lab_cache[pid_str] = results

save_lab_cache(lab_cache)

# ============================================================
# SNAPSHOT
# ============================================================
new = {}
now = datetime.now(timezone.utc).isoformat()

for pkg in tracked_packages_map.values():
    pid = str(pkg["Id"])
    lab_hash = compute_lab_hash(lab_by_pkg.get(pkg["Id"], []))

    new[pid] = {
        "item": pkg["ItemName"],
        "qty": pkg["Quantity"],
        "lab_hash": lab_hash,
        "last_seen": now
    }

snapshot["packages"] = new
save_snapshot(snapshot)

print("UPDATED INVENTORY CACHE")
print("UPDATED LAB CACHE")
print("UPDATED SNAPSHOT")
