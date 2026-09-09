"""One-shot historical backup. Run via the bootstrap workflow.

Fetches everything from 2025-01-01 up to (current_year-1)-11-30 and writes to
data/backup/. The daily script never touches this folder.
"""
import os
import time
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://evo-integracao.w12app.com.br/api/v1/receivables/summary-excel"
# Sedes master: financialsab /api/admin, read-only key (see ../API_SEDES_READONLY.md)
SEDES_API_URL = os.environ.get("SEDES_API_URL", "https://financialsab.vercel.app/api/admin")
SEDES_API_KEY = os.environ["SEDES_API_KEY"]
BACKUP_DIR = os.environ.get("BACKUP_DIR", "data/backup")
HISTORY_START = os.environ.get("HISTORY_START", "2025-01-01")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "300"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "3"))

try:
    import python_calamine  # noqa
    READ_ENGINE = "calamine"
except Exception:
    READ_ENGINE = "openpyxl"

CREDENTIALS = [
    {"username": os.environ["EVO_CO_USER"], "password": os.environ["EVO_CO_PASS"], "filename": "filtered_data.csv",    "country": "CO"},
    {"username": os.environ["EVO_MX_USER"], "password": os.environ["EVO_MX_PASS"], "filename": "filtered_data_mx.csv", "country": "MX"},
    {"username": os.environ["EVO_BR_USER"], "password": os.environ["EVO_BR_PASS"], "filename": "filtered_data_br.csv", "country": "BR"},
]
RAW_COLS = ["Filial", "ValorBaixa", "DtLancamento", "IdFilial"]
OUT_COLS = ["display_name", "ValorBaixa", "DtLancamento", "IdFilial"]


def _truthy(v):
    if v is None: return False
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return int(v) == 1
    return str(v).strip().lower() in ("1", "true")


def fetch_sedes():
    """GET /api/admin with the read-only key. Returns the raw list of sede rows."""
    r = requests.get(SEDES_API_URL, headers={"X-API-Key": SEDES_API_KEY}, timeout=60)
    r.raise_for_status()
    return r.json().get("sedes") or []


def is_operativa(b):
    """Business rules, API_SEDES_READONLY.md section 5.
    vigente = not desaparecida and not (is_deleted and estado != activa)
    fase operativa = not is_presale and estado == activa. ACTION_SPORT_CLUB excluded."""
    estado = str(b.get("estado") or "").strip().lower()
    if _truthy(b.get("desaparecida")): return False
    if _truthy(b.get("is_deleted")) and estado != "activa": return False
    if _truthy(b.get("is_presale")): return False
    if estado != "activa": return False
    if str(b.get("brand", "")).strip().upper() == "ACTION_SPORT_CLUB": return False
    return True


def branches_by_country_from(items):
    """{country: {partner_id (EVO IdFilial): display_name}} for operativa sedes."""
    by_country = {}
    skipped = 0
    for b in items:
        if not is_operativa(b): skipped += 1; continue
        pid = b.get("partner_id")
        name = b.get("display_name") or b.get("name")
        cc = (b.get("country") or "").strip().upper()
        if pid is None or not name or not cc:
            continue
        try:
            by_country.setdefault(cc, {})[int(pid)] = str(name).strip()
        except (TypeError, ValueError):
            continue
    summary = ", ".join(f"{c}={len(m)}" for c, m in sorted(by_country.items()))
    print(f"sedes operativas by country: {summary}; skipped={skipped}")
    return by_country


def fetch_branches_by_country():
    return branches_by_country_from(fetch_sedes())


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
    os.makedirs(BACKUP_DIR, exist_ok=True)
    today = datetime.now()
    end_date = f"{today.year - 1}-11-30"
    if datetime.strptime(end_date, "%Y-%m-%d") < datetime.strptime(HISTORY_START, "%Y-%m-%d"):
        print("Nothing to backup yet.")
        return
    ranges = monthly_ranges(HISTORY_START, end_date)
    end_dt = pd.to_datetime(end_date)
    print(f"Backup window: {HISTORY_START} -> {end_date} ({len(ranges)} chunks/country)")

    branches_by_country = fetch_branches_by_country()

    tasks = [(c, s, e) for c in CREDENTIALS for s, e in ranges]
    by_file = {c["filename"]: [] for c in CREDENTIALS}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for fname, df in ex.map(fetch_chunk, tasks):
            if df is not None:
                by_file[fname].append(df)

    for creds in CREDENTIALS:
        fname = creds["filename"]
        cc = creds["country"]
        frames = by_file.get(fname, [])
        if not frames:
            print(f"NO DATA {fname}")
            continue
        mapping = branches_by_country.get(cc, {})
        df = pd.concat(frames, ignore_index=True)[RAW_COLS]
        df["DtLancamento"] = pd.to_datetime(df["DtLancamento"], format="%d/%m/%Y", errors="coerce")
        df = df[df["DtLancamento"] <= end_dt]
        df["DtLancamento"] = df["DtLancamento"].dt.strftime("%Y-%m-%d")
        df["IdFilial"] = pd.to_numeric(df["IdFilial"], errors="coerce").astype("Int64")
        before = len(df)
        df["display_name"] = df["IdFilial"].map(mapping)
        df = df.dropna(subset=["display_name"])
        print(f"{fname} [{cc}]: {before} -> {len(df)} rows")
        out = os.path.join(BACKUP_DIR, fname)
        df[OUT_COLS].to_csv(out, index=False)
        print(f"WROTE {out} ({len(df)} rows)")


if __name__ == "__main__":
    main()
