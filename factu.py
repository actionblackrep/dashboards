import os
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

BASE_URL = "https://evo-integracao.w12app.com.br/api/v1/receivables/summary-excel"
DATA_DIR = os.environ.get("DATA_DIR", "data")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "180"))

CREDENTIALS = [
    {"username": os.environ["EVO_CO_USER"], "password": os.environ["EVO_CO_PASS"], "filename": "filtered_data.csv"},
    {"username": os.environ["EVO_MX_USER"], "password": os.environ["EVO_MX_PASS"], "filename": "filtered_data_mx.csv"},
    {"username": os.environ["EVO_BR_USER"], "password": os.environ["EVO_BR_PASS"], "filename": "filtered_data_br.csv"},
]


def download_data(username, password, start_date, end_date):
    url = f"{BASE_URL}?dtLancamentoCaixaDe={start_date}&dtLancamentoCaixaAte={end_date}&exibirSaldoDevedor=false"
    for attempt in (1, 2):
        try:
            r = requests.get(url, auth=(username, password), timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                print(f"OK {username} {start_date}->{end_date}")
                return pd.read_excel(BytesIO(r.content))
            print(f"ERR {r.status_code} {username} {start_date}->{end_date}")
            return None
        except requests.exceptions.Timeout:
            print(f"TIMEOUT attempt {attempt} {username} {start_date}->{end_date}")
    return None


def generate_date_ranges(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges = []
    while start <= end:
        nxt = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        range_end = min(nxt - timedelta(days=1), end)
        ranges.append((start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")))
        start = range_end + timedelta(days=1)
    return ranges


def fetch_for_credential(creds, date_ranges):
    frames = []
    for s, e in date_ranges:
        df = download_data(creds["username"], creds["password"], s, e)
        if df is not None:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else None


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    today = datetime.now()
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    # Daily window: Dec 1 of previous year -> yesterday
    start_date = f"{today.year - 1}-12-01"
    date_ranges = generate_date_ranges(start_date, end_date)
    end_dt = pd.to_datetime(end_date)
    print(f"Window: {start_date} -> {end_date} ({len(date_ranges)} chunks)")

    for creds in CREDENTIALS:
        print(f"\n== {creds['username']} ==")
        df = fetch_for_credential(creds, date_ranges)
        if df is None:
            print(f"NO DATA {creds['username']}")
            continue
        cols = ["Filial", "ValorBaixa", "DtLancamento", "IdFilial"]
        df = df[cols]
        df["DtLancamento"] = pd.to_datetime(df["DtLancamento"], format="%d/%m/%Y", errors="coerce")
        df = df[df["DtLancamento"] <= end_dt]
        df["DtLancamento"] = df["DtLancamento"].dt.strftime("%Y-%m-%d")
        out = os.path.join(DATA_DIR, creds["filename"])
        df.to_csv(out, index=False)
        print(f"WROTE {out} ({len(df)} rows)")

    with open(os.path.join(DATA_DIR, "last_update.txt"), "w") as f:
        f.write(datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))


if __name__ == "__main__":
    main()
