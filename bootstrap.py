"""One-shot historical backup. Run manually once.

Fetches everything from 2025-01-01 up to (current_year-1)-11-30 and writes to
data/backup/. The daily script never touches this folder.

Usage:
  EVO_CO_USER=... EVO_CO_PASS=... \\
  EVO_MX_USER=... EVO_MX_PASS=... \\
  EVO_BR_USER=... EVO_BR_PASS=... \\
  python bootstrap.py
"""
import os
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

BASE_URL = "https://evo-integracao.w12app.com.br/api/v1/receivables/summary-excel"
BACKUP_DIR = os.environ.get("BACKUP_DIR", "data/backup")
HISTORY_START = os.environ.get("HISTORY_START", "2025-01-01")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "300"))

CREDENTIALS = [
    {"username": os.environ["EVO_CO_USER"], "password": os.environ["EVO_CO_PASS"], "filename": "filtered_data.csv"},
    {"username": os.environ["EVO_MX_USER"], "password": os.environ["EVO_MX_PASS"], "filename": "filtered_data_mx.csv"},
    {"username": os.environ["EVO_BR_USER"], "password": os.environ["EVO_BR_PASS"], "filename": "filtered_data_br.csv"},
]


def download_data(username, password, start_date, end_date):
    url = f"{BASE_URL}?dtLancamentoCaixaDe={start_date}&dtLancamentoCaixaAte={end_date}&exibirSaldoDevedor=false"
    try:
        r = requests.get(url, auth=(username, password), timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            print(f"OK {username} {start_date}->{end_date}")
            return pd.read_excel(BytesIO(r.content))
        print(f"ERR {r.status_code} {username} {start_date}->{end_date}")
    except requests.exceptions.Timeout:
        print(f"TIMEOUT {username} {start_date}->{end_date}")
    return None


def date_ranges(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    out = []
    while start <= end:
        nxt = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        re = min(nxt - timedelta(days=1), end)
        out.append((start.strftime("%Y-%m-%d"), re.strftime("%Y-%m-%d")))
        start = re + timedelta(days=1)
    return out


def main():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    today = datetime.now()
    end_date = f"{today.year - 1}-11-30"
    if datetime.strptime(end_date, "%Y-%m-%d") < datetime.strptime(HISTORY_START, "%Y-%m-%d"):
        print("Nothing to backup yet.")
        return
    ranges = date_ranges(HISTORY_START, end_date)
    end_dt = pd.to_datetime(end_date)
    print(f"Backup window: {HISTORY_START} -> {end_date} ({len(ranges)} chunks)")

    for creds in CREDENTIALS:
        print(f"\n== {creds['username']} ==")
        frames = []
        for s, e in ranges:
            df = download_data(creds["username"], creds["password"], s, e)
            if df is not None:
                frames.append(df)
        if not frames:
            print(f"NO DATA {creds['username']}")
            continue
        df = pd.concat(frames, ignore_index=True)
        cols = ["Filial", "ValorBaixa", "DtLancamento", "IdFilial"]
        df = df[cols]
        df["DtLancamento"] = pd.to_datetime(df["DtLancamento"], format="%d/%m/%Y", errors="coerce")
        df = df[df["DtLancamento"] <= end_dt]
        df["DtLancamento"] = df["DtLancamento"].dt.strftime("%Y-%m-%d")
        out = os.path.join(BACKUP_DIR, creds["filename"])
        df.to_csv(out, index=False)
        print(f"WROTE {out} ({len(df)} rows)")


if __name__ == "__main__":
    main()
