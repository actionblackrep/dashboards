"""One-off seed: load historical MX/BR snapshots from the manual Excel files
(UsuariosMX.xlsx / UsuariosBR.xlsx, sheet "Operacion") into
data/evo_snapshots.json. Excel rows are only used for days that have no EVO
row for that country, so the EVO job (evo_snapshot.py) always wins going forward.

Usage: python seed_from_excel.py UsuariosMX.xlsx:MX UsuariosBR.xlsx:BR
"""
import json
import os
import sys
from datetime import datetime

import pandas as pd

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
OUT = os.path.join(DATA_DIR, "evo_snapshots.json")


def load_excel(path, country):
    x = pd.ExcelFile(path)
    op = x.parse("Operacion")
    ids = {}
    if "MasterEvo" in x.sheet_names:
        m = x.parse("MasterEvo")
        ids = {str(r["Filial"]).strip().upper(): int(r["IdFilial"]) for _, r in m.iterrows() if pd.notna(r.get("IdFilial"))}
    rows = []
    for _, r in op.iterrows():
        f = r.get("Fecha")
        if pd.isna(f) or pd.isna(r.get("Clientes activos")) or pd.isna(r.get("Deudores")):
            continue
        sede = str(r["Sede/club"]).strip()
        rows.append({
            "Fecha": pd.Timestamp(f).strftime("%Y-%m-%dT%H:%M:%S.000+00:00"),
            "country": country,
            "idBranch": ids.get(sede.upper()),
            "Sede/club": sede,
            "Clientes activos": int(r["Clientes activos"]),
            "Deudores": int(r["Deudores"]),
            "Suspensos": None if pd.isna(r.get("Suspensos")) else int(r["Suspensos"]),
            "source": "excel",
        })
    return rows


def main(args):
    existing = json.load(open(OUT, encoding="utf-8")) if os.path.exists(OUT) else []
    evo_days = {(r["country"], r["Fecha"][:10]) for r in existing if r.get("source") != "excel"}
    keep = [r for r in existing if r.get("source") != "excel"]  # re-seed excel rows from scratch
    added = 0
    for a in args:
        path, country = a.rsplit(":", 1)
        rows = load_excel(path, country.upper())
        rows = [r for r in rows if (r["country"], r["Fecha"][:10]) not in evo_days]
        keep.extend(rows)
        added += len(rows)
        print(f"[{country}] {len(rows)} excel rows from {path}")
    keep.sort(key=lambda r: (r["Fecha"], r["country"], str(r["idBranch"])))
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(keep, f, ensure_ascii=False, indent=0)
    print(f"WROTE {OUT}: {len(keep)} rows ({added} from excel)")


if __name__ == "__main__":
    main(sys.argv[1:])
