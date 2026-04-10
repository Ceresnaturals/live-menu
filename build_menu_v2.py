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
INVENTORY_RESULTS_OUTPUT_PATH = Path("/home/ceres/live-menu/inventory_test_results_v2.json")

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
    if "infused pr" in name or "infused pre-roll" in name:
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
    raw_df = xl.parse(sheet, dtype=str, header=None).fillna("")

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
    # BulkPrice lives as a separate table on the same worksheet.
    # ==========================
    bulk_header = None
    bulk_start_col = None

    for row_idx in range(len(raw_df)):
        row_values = [
            "" if pd.isna(value) else str(value).strip()
            for value in raw_df.iloc[row_idx].tolist()
        ]

        for col_idx in range(max(0, len(row_values) - 2)):
            if row_values[col_idx:col_idx + 3] == ["ProductGroup", "MinQty", "Price"]:
                bulk_header = row_idx
                bulk_start_col = col_idx
                break

        if bulk_header is not None:
            break

    if bulk_header is not None and bulk_start_col is not None:
        for row_idx in range(bulk_header + 1, len(raw_df)):
            group = str(raw_df.iat[row_idx, bulk_start_col]).strip()
            min_qty_raw = raw_df.iat[row_idx, bulk_start_col + 1]
            price_raw = raw_df.iat[row_idx, bulk_start_col + 2]

            if not group and pd.isna(min_qty_raw) and pd.isna(price_raw):
                continue

            min_qty = _to_money(min_qty_raw)
            price = _to_money(price_raw)

            if not group or min_qty is None or price is None:
                continue

            group_rules.setdefault(group, []).append({
                "minQty": int(min_qty),
                "price": price
            })

    for group, rules in group_rules.items():
        group_rules[group] = sorted(rules, key=lambda x: x["minQty"])

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
    watched_generated_at = watched_payload.get("generated_at")

    final = []
    inventory_results = []

    for pkg_id_str, pkg in watched_packages.items():
        item_name = str(pkg.get("ItemName") or "").strip()
        excel_row = product_map.get(item_name, {})
        lab_results = lab_cache.get(str(pkg.get("Id")), [])
        room_name = str(pkg.get("LocationName") or "").strip()

        inventory_results.append({
            "Id": pkg.get("Id"),
            "Label": pkg.get("Label"),
            "ItemName": item_name,
            "Quantity": pkg.get("Quantity"),
            "LocationName": room_name,
            "Type": excel_row.get("type"),
            "Price": excel_row.get("price"),
            "LabResults": lab_results
        })

        room_name = room_name.lower()
        if room_name not in MENU_ROOMS:
            continue

        bulk_rules = []
        bulk_rule_scope = None
        bulk_rule_group = None

        # ==========================
        # PRIORITY: ITEM RULES
        # ==========================
        if item_name in item_rules:
            bulk_rules = item_rules[item_name]
            bulk_rule_scope = "item"

        # ==========================
        # FALLBACK: GROUP RULES
        # ==========================
        else:
            group = get_product_group(item_name)
            if group and group in group_rules:
                bulk_rules = group_rules[group]
                bulk_rule_scope = "group"
                bulk_rule_group = group

        final.append({
            "Id": pkg.get("Id"),
            "Label": pkg.get("Label"),
            "ItemName": item_name,
            "Quantity": pkg.get("Quantity"),
            "LocationName": pkg.get("LocationName"),
            "Type": excel_row.get("type"),
            "Price": excel_row.get("price"),
            "LabResults": lab_results,
            "bulkRules": bulk_rules if bulk_rules else None,
            "bulkRuleScope": bulk_rule_scope,
            "bulkRuleGroup": bulk_rule_group
        })

    final = sorted(final, key=lambda x: (str(x.get("ItemName") or ""), x.get("Id") or 0))
    inventory_results = sorted(
        inventory_results,
        key=lambda x: (
            str(x.get("LocationName") or ""),
            str(x.get("ItemName") or ""),
            x.get("Id") or 0
        )
    )

    payload = {
        "items": final
    }

    inventory_payload = {
        "generatedAt": watched_generated_at,
        "items": inventory_results
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    with open(INVENTORY_RESULTS_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(inventory_payload, f, ensure_ascii=False, separators=(",", ":"))

    print("WATCHED PACKAGES:", len(watched_packages))
    print("MENU ITEMS:", len(final))
    print("WROTE:", OUTPUT_PATH)
    print("WROTE:", INVENTORY_RESULTS_OUTPUT_PATH)

def sync_to_github():
    repo_dir = Path("/home/ceres/live-menu")
    os.chdir(repo_dir)

    print("SYNCING TO GITHUB...")

    subprocess.run(["git", "fetch", "origin"], check=False, capture_output=True, text=True)
    subprocess.run(
        ["git", "add", "menu_v2.json", "inventory_test_results_v2.json"],
        check=False,
        capture_output=True,
        text=True
    )

    diff_result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        check=False,
        capture_output=True,
        text=True
    )

    if diff_result.returncode == 0:
        print("NO GITHUB CHANGES TO PUSH")
        return

    commit_result = subprocess.run(
        ["git", "commit", "-m", f"menu_v2 update @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
        check=False,
        capture_output=True,
        text=True
    )

    if commit_result.returncode != 0:
        print("GIT COMMIT FAILED")
        if commit_result.stderr.strip():
            print(commit_result.stderr.strip())
        elif commit_result.stdout.strip():
            print(commit_result.stdout.strip())
        return

    push_result = subprocess.run(
        ["git", "push", "origin", "main"],
        check=False,
        capture_output=True,
        text=True
    )

    if push_result.returncode == 0:
        print("PUSHED TO GITHUB")
    else:
        print("GIT PUSH FAILED")
        if push_result.stderr.strip():
            print(push_result.stderr.strip())
        elif push_result.stdout.strip():
            print(push_result.stdout.strip())

if __name__ == "__main__":
    main()
    sync_to_github()
