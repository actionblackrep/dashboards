"""Re-filter existing data/backup/*.csv against the current API Master Sedes.
Drops rows whose IdFilial is presale or whose brand is ACTION_SPORT_CLUB.
"""
import os
import requests
import pandas as pd

# API Master Sedes: unica fuente de verdad de sedes y de su estado activa/
# inactiva (lo pone el admin en /admin). Branches puede borrar una sede por
# otros efectos y eso NO la saca de aqui. Ver ../MASTER_SEDES_API.md.
MASTER_SEDES_URL = os.environ.get("MASTER_SEDES_URL", "https://financialsab.vercel.app/api/admin")
MASTER_SEDES_KEY = (os.environ.get("MASTER_SEDES_KEY") or os.environ.get("SEDES_API_KEY") or "").strip()
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


def fetch_sedes():
    """GET API Master Sedes con la key de solo lectura. Devuelve las filas crudas."""
    if not MASTER_SEDES_KEY:
        raise SystemExit(
            "MASTER_SEDES_KEY vacia. Crea el secret MASTER_SEDES_KEY en el repo "
            "(Settings > Secrets and variables > Actions) con la key de solo "
            "lectura de la API Master Sedes. Ver MASTER_SEDES_API.md.")
    r = requests.get(MASTER_SEDES_URL, headers={"X-API-Key": MASTER_SEDES_KEY}, timeout=60)
    if r.status_code == 401:
        raise SystemExit(
            f"401 de la API Master Sedes: la key es invalida o esta vencida "
            f"(largo={len(MASTER_SEDES_KEY)}). Revisa el secret MASTER_SEDES_KEY.")
    r.raise_for_status()
    return r.json().get("sedes") or []


def is_operativa(b):
    """Business rules, MASTER_SEDES_API.md section 5.
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
