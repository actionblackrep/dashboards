import os
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://evo-integracao.w12app.com.br/api/v1/receivables/summary-excel"
DATA_DIR = os.environ.get("DATA_DIR", "data")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "600"))

CREDENTIALS = [
    {"username": os.environ["EVO_CO_USER"], "password": os.environ["EVO_CO_PASS"], "filename": "filtered_data.csv"},
    {"username": os.environ["EVO_MX_USER"], "password": os.environ["EVO_MX_PASS"], "filename": "filtered_data_mx.csv"},
    {"username": os.environ["EVO_BR_USER"], "password": os.environ["EVO_BR_PASS"], "filename": "filtered_data_br.csv"},
]

COLS = ["Filial", "ValorBaixa", "DtLancamento", "IdFilial"]


def download_one(username, password, start, end):
    url = f"{BASE_URL}?dtLancamentoCaixaDe={start}&dtLancamentoCaixaAte={end}&exibirSaldoDevedor=false"
    r = requests.get(url, auth=(username, password), timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return pd.read_excel(BytesIO(r.content))


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


def fetch_country(creds, start_date, end_date):
    """Single shot first; fall back to monthly chunks on timeout/error."""
    user = creds["username"]
    try:
        print(f"[{user}] single shot {start_date} -> {end_date}")
        df = download_one(user, creds["password"], start_date, end_date)
        print(f"[{user}] OK ({len(df)} rows)")
        return df
    except Exception as ex:
        print(f"[{user}] single shot failed: {ex}. Falling back to monthly.")
    frames = []
    for s, e in monthly_ranges(start_date, end_date):
        try:
            df = download_one(user, creds["password"], s, e)
            frames.append(df)
            print(f"[{user}] OK chunk {s}->{e} ({len(df)})")
        except Exception as ex:
            print(f"[{user}] FAIL chunk {s}->{e}: {ex}")
    return pd.concat(frames, ignore_index=True) if frames else None


def process(creds, start_date, end_date, end_dt):
    df = fetch_country(creds, start_date, end_date)
    if df is None:
        return creds, None
    df = df[COLS]
    df["DtLancamento"] = pd.to_datetime(df["DtLancamento"], format="%d/%m/%Y", errors="coerce")
    df = df[df["DtLancamento"] <= end_dt]
    df["DtLancamento"] = df["DtLancamento"].dt.strftime("%Y-%m-%d")
    return creds, df


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    today = datetime.now()
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = f"{today.year - 1}-12-01"
    end_dt = pd.to_datetime(end_date)
    print(f"Window: {start_date} -> {end_date}")

    with ThreadPoolExecutor(max_workers=len(CREDENTIALS)) as ex:
        results = list(ex.map(lambda c: process(c, start_date, end_date, end_dt), CREDENTIALS))

    for creds, df in results:
        if df is None:
            print(f"NO DATA {creds['username']}")
            continue
        out = os.path.join(DATA_DIR, creds["filename"])
        df.to_csv(out, index=False)
        print(f"WROTE {out} ({len(df)} rows)")

    with open(os.path.join(DATA_DIR, "last_update.txt"), "w") as f:
        f.write(datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))


if __name__ == "__main__":
    main()
