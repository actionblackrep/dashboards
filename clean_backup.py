"""Re-filter existing data/backup/*.csv against the current branches API.
Drops rows whose IdFilial is presale or whose brand is ACTION_SPORT_CLUB.
"""
import os
import requests
import pandas as pd

BRANCHES_URL = os.environ.get("BRANCHES_URL", "https://action-branches-api.vercel.app/api/branches")
BRANCHES_API_KEY = os.environ["BRANCHES_API_KEY"]
BACKUP_DIR = os.environ.get("BACKUP_DIR", "data/backup")

FILES = [
    ("filtered_data.csv", "CO"),
    ("filtered_data_mx.csv", "MX"),
    ("filtered_data_br.csv", "BR"),
]
OUT_COLS = ["display_name", "ValorBaixa", "DtLancamento", "IdFilial"]

def _truthy(v):
    if v is None: return False
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return int(v) == 1
    return str(v).strip().lower() in ("1", "true")


def fetch_branches_by_country():
    r = requests.get(BRANCHES_URL, headers={"x-api-key": BRANCHES_API_KEY}, timeout=60)
    r.raise_for_status()
    js = r.json()
    items = js if isinstance(js, list) else js.get("data") or js.get("branches") or []
    by_country = {}
    p = d = bsp = 0
    for b in items:
        if _truthy(b.get("is_presale")): p += 1; continue
        if _truthy(b.get("is_deleted")): d += 1; continue
        if str(b.get("brand", "")).strip().upper() == "ACTION_SPORT_CLUB":
            bsp += 1; continue
        pid = b.get("partner_id")
        name = b.get("display_name")
        cc = (b.get("country_code") or "").strip().upper()
        if pid is None or not name or not cc:
            continue
        try:
            by_country.setdefault(cc, {})[int(pid)] = str(name).strip()
        except (TypeError, ValueError):
            continue
    print(f"branches by country: " + ", ".join(f"{c}={len(m)}" for c, m in sorted(by_country.items())))
    return by_country


def main():
    if not os.path.isdir(BACKUP_DIR):
        print(f"No {BACKUP_DIR} folder, nothing to clean.")
        return
    branches_by_country = fetch_branches_by_country()
    for fname, cc in FILES:
        path = os.path.join(BACKUP_DIR, fname)
        if not os.path.exists(path):
            print(f"SKIP missing {path}")
            continue
        mapping = branches_by_country.get(cc, {})
        df = pd.read_csv(path)
        before = len(df)
        df["IdFilial"] = pd.to_numeric(df["IdFilial"], errors="coerce").astype("Int64")
        df["display_name"] = df["IdFilial"].map(mapping)
        df = df.dropna(subset=["display_name"])
        df[OUT_COLS].to_csv(path, index=False)
        print(f"{fname} [{cc}]: {before} -> {len(df)} rows")


if __name__ == "__main__":
    main()
