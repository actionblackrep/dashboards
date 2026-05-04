import os
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

BASE_URL = "https://evo-integracao.w12app.com.br/api/v1/receivables/summary-excel"
DATA_DIR = os.environ.get("DATA_DIR", "data")

CREDENTIALS = [
    {"username": os.environ["EVO_CO_USER"], "password": os.environ["EVO_CO_PASS"], "filename": "filtered_data.csv",    "country": "CO"},
    {"username": os.environ["EVO_MX_USER"], "password": os.environ["EVO_MX_PASS"], "filename": "filtered_data_mx.csv", "country": "MX"},
    {"username": os.environ["EVO_BR_USER"], "password": os.environ["EVO_BR_PASS"], "filename": "filtered_data_br.csv", "country": "BR"},
]


def download_data(username, password, start_date, end_date):
    url = f"{BASE_URL}?dtLancamentoCaixaDe={start_date}&dtLancamentoCaixaAte={end_date}&exibirSaldoDevedor=false"
    try:
        r = requests.get(url, auth=(username, password), timeout=300)
        if r.status_code == 200:
            print(f"OK {username} {start_date}->{end_date}")
            return pd.read_excel(BytesIO(r.content))
        print(f"ERR {r.status_code} {username} {start_date}->{end_date}")
        return None
    except requests.exceptions.Timeout:
        print(f"TIMEOUT {username} {start_date}->{end_date}")
        return None


def generate_date_ranges(start_date, end_date, frequency="monthly"):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges = []
    while start <= end:
        if frequency == "monthly":
            nxt = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
            range_end = min(nxt - timedelta(days=1), end)
        else:
            break
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
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    date_ranges = generate_date_ranges("2025-01-01", end_date, frequency="monthly")
    end_dt = pd.to_datetime(end_date)

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

    # marker for the dashboard
    with open(os.path.join(DATA_DIR, "last_update.txt"), "w") as f:
        f.write(datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))


if __name__ == "__main__":
    main()
