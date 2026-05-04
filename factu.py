import os
import time
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://evo-integracao.w12app.com.br/api/v1/receivables/summary-excel"
BRANCHES_URL = os.environ.get("BRANCHES_URL", "https://action-branches-api.vercel.app/api/branches")
BRANCHES_API_KEY = os.environ["BRANCHES_API_KEY"]
DATA_DIR = os.environ.get("DATA_DIR", "data")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "300"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "3"))

try:
    import python_calamine  # noqa
    READ_ENGINE = "calamine"
except Exception:
    READ_ENGINE = "openpyxl"

CREDENTIALS = [
    {"username": os.environ["EVO_CO_USER"], "password": os.environ["EVO_CO_PASS"], "filename": "filtered_data.csv"},
    {"username": os.environ["EVO_MX_USER"], "password": os.environ["EVO_MX_PASS"], "filename": "filtered_data_mx.csv"},
    {"username": os.environ["EVO_BR_USER"], "password": os.environ["EVO_BR_PASS"], "filename": "filtered_data_br.csv"},
]
RAW_COLS = ["Filial", "ValorBaixa", "DtLancamento", "IdFilial"]
OUT_COLS = ["display_name", "ValorBaixa", "DtLancamento", "IdFilial"]


def _truthy(v):
    if v is None: return False
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return int(v) == 1
    return str(v).strip().lower() in ("1", "true")


def fetch_branches():
    """Return dict {partner_id (int): display_name} excluding presale, deleted, and ACTION_SPORT_CLUB."""
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


def monthly_ranges(start_date, end_date):
    s = datetime.strptime(start_date, "%Y-%m-%d")
    e = datetime.strptime(end_date, "%Y-%m-%d")
    out = []
    while s <= e:
        nxt = (s.replace(day=28) + timedelta(days=4)).replace(day=1)
        re = min(nxt - timedelta(days=1), e)
        out.append((s.strftime("%Y-%m-%d"), re.strftime("%Y-%m-%d")))
        s = re + timedelta(days=1)
    return out


def fetch_chunk(task):
    creds, start, end = task
    user = creds["username"]
    t0 = time.time()
    try:
        r = requests.get(
            f"{BASE_URL}?dtLancamentoCaixaDe={start}&dtLancamentoCaixaAte={end}&exibirSaldoDevedor=false",
            auth=(user, creds["password"]),
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        df = pd.read_excel(BytesIO(r.content), engine=READ_ENGINE)
        print(f"OK [{user}] {start}->{end} {len(df)} rows {time.time()-t0:.1f}s")
        return creds["filename"], df
    except Exception as ex:
        print(f"FAIL [{user}] {start}->{end} {ex}")
        return creds["filename"], None


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    today = datetime.now()
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = f"{today.year - 1}-12-01"
    end_dt = pd.to_datetime(end_date)
    ranges = monthly_ranges(start_date, end_date)
    print(f"Window: {start_date} -> {end_date} ({len(ranges)} chunks/country, engine={READ_ENGINE}, workers={MAX_WORKERS})")

    branches = fetch_branches()

    tasks = [(c, s, e) for c in CREDENTIALS for s, e in ranges]
    by_file = {c["filename"]: [] for c in CREDENTIALS}

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for fname, df in ex.map(fetch_chunk, tasks):
            if df is not None:
                by_file[fname].append(df)
    print(f"Total fetch time: {time.time()-t0:.1f}s")

    for fname, frames in by_file.items():
        if not frames:
            print(f"NO DATA {fname}")
            continue
        df = pd.concat(frames, ignore_index=True)[RAW_COLS]
        df["DtLancamento"] = pd.to_datetime(df["DtLancamento"], format="%d/%m/%Y", errors="coerce")
        df = df[df["DtLancamento"] <= end_dt]
        df["DtLancamento"] = df["DtLancamento"].dt.strftime("%Y-%m-%d")
        df["IdFilial"] = pd.to_numeric(df["IdFilial"], errors="coerce").astype("Int64")
        before = len(df)
        df["display_name"] = df["IdFilial"].map(branches)
        df = df.dropna(subset=["display_name"])
        print(f"{fname}: {before} -> {len(df)} rows after branches join")
        out = os.path.join(DATA_DIR, fname)
        df[OUT_COLS].to_csv(out, index=False)
        print(f"WROTE {out} ({len(df)} rows)")

    with open(os.path.join(DATA_DIR, "last_update.txt"), "w") as f:
        f.write(datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))


if __name__ == "__main__":
    main()
