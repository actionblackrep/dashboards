"""Re-filter existing data/backup/*.csv against the current branches API.
No EVO calls, no historical re-fetch. Drops rows whose IdFilial is presale
or whose brand is ACTION_SPORT_CLUB. Also refreshes display_name.
"""
import os
import requests
import pandas as pd

BRANCHES_URL = os.environ.get("BRANCHES_URL", "https://action-branches-api.vercel.app/api/branches")
BRANCHES_API_KEY = os.environ["BRANCHES_API_KEY"]
BACKUP_DIR = os.environ.get("BACKUP_DIR", "data/backup")

FILES = ["filtered_data.csv", "filtered_data_mx.csv", "filtered_data_br.csv"]
OUT_COLS = ["display_name", "ValorBaixa", "DtLancamento", "IdFilial"]


def _is_presale(v):
    if v is None: return False
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return int(v) == 1
    return str(v).strip().lower() in ("1", "true", "yes", "y", "t")


def fetch_branches():
    r = requests.get(BRANCHES_URL, headers={"x-api-key": BRANCHES_API_KEY}, timeout=60)
    r.raise_for_status()
    js = r.json()
    items = js if isinstance(js, list) else js.get("data") or js.get("branches") or []
    mapping = {}
    p = b_count = 0
    for b in items:
        if _is_presale(b.get("Presale", b.get("presale"))):
            p += 1
            continue
        brand = str(b.get("brand", b.get("Brand", "")) or "").strip().upper()
        if brand == "ACTION_SPORT_CLUB":
            b_count += 1
            continue
        pid = b.get("partner_id") or b.get("partnerId") or b.get("id")
        name = b.get("display_name") or b.get("displayName") or b.get("name")
        if pid is None or not name:
            continue
        try:
            mapping[int(pid)] = str(name).strip()
        except (TypeError, ValueError):
            continue
    print(f"branches kept={len(mapping)} presale_skip={p} action_sport_skip={b_count}")
    return mapping


def main():
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
