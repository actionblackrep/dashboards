"""Re-filter existing data/backup/*.csv against the current branches API.
Drops rows whose IdFilial is presale or whose brand is ACTION_SPORT_CLUB.
"""
import os
import requests
import pandas as pd

BRANCHES_URL = os.environ.get("BRANCHES_URL", "https://action-branches-api.vercel.app/api/branches")
BRANCHES_API_KEY = os.environ["BRANCHES_API_KEY"]
BACKUP_DIR = os.environ.get("BACKUP_DIR", "data/backup")

FILES = ["filtered_data.csv", "filtered_data_mx.csv", "filtered_data_br.csv"]
OUT_COLS = ["display_name", "ValorBaixa", "DtLancamento", "IdFilial"]

def _truthy(v):
    if v is None: return False
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return int(v) == 1
    return str(v).strip().lower() in ("1", "true")


def fetch_branches():
    r = requests.get(BRANCHES_URL, headers={"x-api-key": BRANCHES_API_KEY}, timeout=60)
    r.raise_for_status()
    js = r.json()
    items = js if isinstance(js, list) else js.get("data") or js.get("branches") or []
    mapping = {}
    p = d = bsp = 0
    for b in items:
        if _truthy(b.get("is_presale")):
            p += 1; continue
        if _truthy(b.get("is_deleted")):
            d += 1; continue
        if str(b.get("brand", "")).strip().upper() == "ACTION_SPORT_CLUB":
            bsp += 1; continue
        pid = b.get("partner_id")
        name = b.get("display_name")
        if pid is None or not name:
            continue
        try:
            mapping[int(pid)] = str(name).strip()
        except (TypeError, ValueError):
            continue
    print(f"branches kept={len(mapping)} presale_skip={p} deleted_skip={d} action_sport_skip={bsp}")
    return mapping


def main():
    if not os.path.isdir(BACKUP_DIR):
        print(f"No {BACKUP_DIR} folder, nothing to clean.")
        return
    branches = fetch_branches()
    for fname in FILES:
        path = os.path.join(BACKUP_DIR, fname)
        if not os.path.exists(path):
            print(f"SKIP missing {path}")
            continue
        df = pd.read_csv(path)
        before = len(df)
        df["IdFilial"] = pd.to_numeric(df["IdFilial"], errors="coerce").astype("Int64")
        df["display_name"] = df["IdFilial"].map(branches)
        df = df.dropna(subset=["display_name"])
        df[OUT_COLS].to_csv(path, index=False)
        print(f"{fname}: {before} -> {len(df)} rows")


if __name__ == "__main__":
    main()
