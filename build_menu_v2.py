#!/usr/bin/env python3
import json
import subprocess
from pathlib import Path
from decimal import Decimal

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
            "type": None if pd.isna(row.get("Type")) else str(row.get("Type")).strip()
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
            except Exception:
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


def main():
    if not WATCHED_INVENTORY_PATH.exists():
        raise FileNotFoundError(f"Missing watched inventory file: {WATCHED_INVENTORY_PATH}")

    if not LAB_CACHE_PATH.exists():
        raise FileNotFoundError(f"Missing lab cache file: {LAB_CACHE_PATH}")

    subprocess.run(
        ["rclone", "copyto", RCLONE_REMOTE, str(LOCAL_EXCEL), "--checksum", "--quiet"],
        check=False
    )

    product_map, bulk_rules = build_excel_map(LOCAL_EXCEL)

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

        final.append({
            "Id": pkg.get("Id"),
            "Label": pkg.get("Label"),
            "ItemName": item_name,
            "Quantity": pkg.get("Quantity"),
            "LocationName": pkg.get("LocationName"),
            "Type": excel_row.get("type"),
            "Price": excel_row.get("price"),
            "LabResults": lab_cache.get(str(pkg.get("Id")), [])
        })

    final = sorted(final, key=lambda x: (str(x.get("ItemName") or ""), x.get("Id") or 0))

    payload = {
        "items": final,
        "bulkRules": bulk_rules
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print("WATCHED PACKAGES:", len(watched_packages))
    print("MENU ITEMS:", len(final))
    print("WROTE:", OUTPUT_PATH)


if __name__ == "__main__":
    main() 
    