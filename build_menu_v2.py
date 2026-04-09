#!/usr/bin/env python3
import json
import subprocess
from pathlib import Path
from decimal import Decimal
from datetime import datetime
import os
import pandas as pd

WATCHED_INVENTORY_PATH = Path("/home/ceres/cache/watched_inventory_v2.json")
LAB_CACHE_PATH = Path("/home/ceres/cache/lab_results_library_v2.json")
OUTPUT_PATH = Path("/home/ceres/live-menu/menu_v2.json")

MENU_ROOMS = {
    "vault - finished goods",
    "low inventory"
}

RCLONE_REMOTE = "ceres_sharepoint:METRC API Depot/Product Information.xlsx"
LOCAL_EXCEL = Path("/tmp/Product Information.xlsx")


def _to_money(v):
    if v is None:
        return None
    s = str(v).strip().replace("$", "").replace(",", "")
    if s == "":
        return None
    try:
        return float(Decimal(s))
    except Exception:
        return None


# ==============================
# PRODUCT GROUP MAPPING
# ==============================
def get_product_group(product_name):
    name = product_name.lower()

    if "double diamonds" in name:
        return "Double Diamonds"
    if "diamond cut" in name:
        return "Diamond Cut"
    if "zen drops" in name:
        return "Zen Drops"
    if "cart" in name and "2g" in name:
        return "2g Cart"
    if "cart" in name:
        return "1g Cart"
    if "vape" in name and "2g" in name:
        return "2g Vape"
    if "vape" in name:
        return "1g Vape"
    if "pre-roll" in name:
        return "Pre-Roll"
    if "relic" in name:
        return "Relic"

    return None


# ==============================
# BUILD MAPS + BULK LOOKUPS
# ==============================
def build_excel_map(path: Path):
    xl = pd.ExcelFile(path, engine="openpyxl")

    sheet = "ProductInfo" if "ProductInfo" in xl.sheet_names else xl.sheet_names[0]
    df = xl.parse(sheet, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    df = df[df["Product"].notna() & (df["Product"].str.strip() != "")]

    product_map = {}
    item_rules = {}
    group_rules = {}

    # ==========================
    # PRODUCT + ITEM RULES
    # ==========================
    for _, row in df.iterrows():
        name = str(row["Product"]).strip()

        product_map[name] = {
            "price": _to_money(row.get("Price", "")),
            "type": None if pd.isna(row.get("Type")) else str(row.get("Type")).strip()
        }

        # ITEM-LEVEL BULK JSON
        bulk_raw = row.get("BulkPricing")
        if bulk_raw and not pd.isna(bulk_raw):
            try:
                tiers = json.loads(str(bulk_raw))
                if isinstance(tiers, list) and tiers:
                    clean = []
                    for t in tiers:
                        if t.get("minQty") and t.get("price"):
                            clean.append({
                                "minQty": int(t["minQty"]),
                                "price": float(t["price"])
                            })

                    if clean:
                        item_rules[name] = sorted(clean, key=lambda x: x["minQty"])

            except Exception as e:
                print(f"Bulk pricing parse error for {name}: {e}")

    # ==========================
    # GROUP RULES
    # ==========================
    if "BulkPrice" in xl.sheet_names:
        br_df = xl.parse("BulkPrice", dtype=str)
        br_df.columns = [c.strip() for c in br_df.columns]

        br_df = br_df.dropna(subset=["ProductGroup", "MinQty", "Price"])

        for group, gdf in br_df.groupby("ProductGroup"):
            rules = []
            for _, r in gdf.iterrows():
                rules.append({
                    "minQty": int(r["MinQty"]),
                    "price": _to_money(r["Price"])
                })

            group_rules[str(group).strip()] = sorted(rules, key=lambda x: x["minQty"])

    return product_map, item_rules, group_rules


def main():
    if not WATCHED_INVENTORY_PATH.exists():
        raise FileNotFoundError(f"Missing watched inventory file: {WATCHED_INVENTORY_PATH}")

    if not LAB_CACHE_PATH.exists():
        raise FileNotFoundError(f"Missing lab cache file: {LAB_CACHE_PATH}")

    subprocess.run(
        ["rclone", "copyto", RCLONE_REMOTE, str(LOCAL_EXCEL), "--checksum", "--quiet"],
        check=False
    )

    product_map, item_rules, group_rules = build_excel_map(LOCAL_EXCEL)

    with open(WATCHED_INVENTORY_PATH, "r", encoding="utf-8") as f:
        watched_payload = json.load(f)

    with open(LAB_CACHE_PATH, "r", encoding="utf-8") as f:
        lab_cache = json.load(f)

    watched_packages = watched_payload.get("packages", {})

    final = []

    for pkg_id_str, pkg in watched_packages.items():
        room_name = str(pkg.get("LocationName") or "").strip().lower()
        if room_name not in MENU_ROOMS:
            continue

        item_name = str(pkg.get("ItemName") or "").strip()
        excel_row = product_map.get(item_name, {})

        bulk_rules = []

        # ==========================
        # PRIORITY: ITEM RULES
        # ==========================
        if item_name in item_rules:
            bulk_rules = item_rules[item_name]

        # ==========================
        # FALLBACK: GROUP RULES
        # ==========================
        else:
            group = get_product_group(item_name)
            if group and group in group_rules:
                bulk_rules = group_rules[group]

        final.append({
            "Id": pkg.get("Id"),
            "Label": pkg.get("Label"),
            "ItemName": item_name,
            "Quantity": pkg.get("Quantity"),
            "LocationName": pkg.get("LocationName"),
            "Type": excel_row.get("type"),
            "Price": excel_row.get("price"),
            "LabResults": lab_cache.get(str(pkg.get("Id")), []),
            "bulkRules": bulk_rules if bulk_rules else None
        })

    final = sorted(final, key=lambda x: (str(x.get("ItemName") or ""), x.get("Id") or 0))

    payload = {
        "items": final
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print("WATCHED PACKAGES:", len(watched_packages))
    print("MENU ITEMS:", len(final))
    print("WROTE:", OUTPUT_PATH)

# ============================================================
#  GIT SYNC + COMMIT + PUSH
# ============================================================

REPO_DIR = Path("/home/ceres/live-menu")

os.chdir(REPO_DIR)

# ensure we are up to date (avoid conflicts)
subprocess.run(["git", "fetch", "origin"], check=False)
subprocess.run(["git", "reset", "--hard", "origin/main"], check=False)

# add file
subprocess.run(["git", "add", "menu_v2.json"], check=False)

# commit (won’t fail if nothing changed)
subprocess.run(
    ["git", "commit", "-m", f"menu_v2 update @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
    check=False
)

# push
subprocess.run(["git", "push", "origin", "main"], check=False)

print("PUSHED TO GITHUB")

if __name__ == "__main__":
    main()
