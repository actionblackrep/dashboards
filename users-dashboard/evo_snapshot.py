"""Daily EVO snapshot (Mexico + Brasil): Activos y Deudores por sede.

Runs in GitHub Actions (no serverless time limit). Appends one row per
(country, branch, UTC day) to data/evo_snapshots.json with the same shape as
the Colombia "Operacion" sheet, so api/snapshot.js can consume all countries
with the same logic. See resources/CONNECT_EVO_MX.md / CONNECT_EVO_BR.md.
"""
import io
import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests

EVO_BASE = os.environ.get("EVO_BASE", "https://evo-integracao-api.w12app.com.br")
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
OUT = os.path.join(DATA_DIR, "evo_snapshots.json")
PAGE = 100
PAUSE = float(os.environ.get("EVO_PAUSE", "1.6"))  # 40 req/min limit
MAX_HISTORY_DAYS = int(os.environ.get("MAX_HISTORY_DAYS", "800"))

# Dedicated credentials from resources/CONNECT_EVO_MX.md / CONNECT_EVO_BR.md.
# Do NOT reuse the factu.py secrets (EVO_*_PASS): different EVO tokens carry
# different permissions and can return different data.
COUNTRIES = [
    {"country": "MX", "user": os.environ.get("EVO_USERS_MX_USER"), "token": os.environ.get("EVO_USERS_MX_TOKEN")},
    {"country": "BR", "user": os.environ.get("EVO_USERS_BR_USER"), "token": os.environ.get("EVO_USERS_BR_TOKEN")},
]


def get(auth, path, params=None, binary=False, tries=6):
    for i in range(tries):
        r = requests.get(EVO_BASE + path, auth=auth, params=params, timeout=120)
        if r.status_code == 429:
            print(f"  429 rate limit, waiting 20s ({path})")
            time.sleep(20)
            continue
        if r.status_code >= 500:
            print(f"  {r.status_code} upstream error, retry {i+1}")
            time.sleep(5 * (i + 1))
            continue
        r.raise_for_status()
        return r.content if binary else r.json()
    raise RuntimeError(f"giving up on {path}")


def branches(auth):
    cfg = get(auth, "/api/v1/configuration")
    out = []
    for b in cfg:
        bid = int(b.get("idBranch", -1))
        if bid <= 0 or bid >= 999:  # 0 = admin, 999 = treinamento, 1000 = modelo
            continue
        out.append({"idBranch": bid, "name": str(b.get("name", "")).strip(), "openingDate": b.get("openingDate")})
    return out


def active_members(auth):
    raw = get(auth, "/api/v2/members/active-members", binary=True)
    df = pd.read_excel(io.BytesIO(raw))
    if df.empty:
        return {}
    return df.groupby("IdBranch")["IdMember"].nunique().to_dict()


def debtors(auth, id_branch):
    """distinct memberId with debtStatus == open and memberStatus == Active."""
    members = set()
    frozen = set()
    skip, total = 0, None
    while True:
        j = get(auth, "/api/v1/receivables/debtors", {"idBranch": id_branch, "take": PAGE, "skip": skip})
        rows = j.get("results") or []
        total = j.get("total") if total is None else total
        for r in rows:
            if str(r.get("debtStatus", "")).lower() == "open":
                st = str(r.get("memberStatus", ""))
                if st == "Active":
                    members.add(r.get("memberId"))
                elif st == "Freeze":
                    frozen.add(r.get("memberId"))
        skip += PAGE
        if len(rows) < PAGE or (total is not None and skip >= total):
            break
        time.sleep(PAUSE)
    return len(members), total or 0


def load_existing():
    if not os.path.exists(OUT):
        return []
    try:
        with open(OUT, encoding="utf-8") as f:
            return json.load(f)
    except Exception as ex:
        print(f"WARN: could not read {OUT}: {ex}")
        return []


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    now = datetime.now(timezone.utc)
    day = now.strftime("%Y-%m-%d")
    stamp = now.isoformat(timespec="milliseconds").replace("+00:00", "+00:00")
    rows = load_existing()
    new_rows = []
    failures = 0

    for c in COUNTRIES:
        cc = c["country"]
        if not c["user"] or not c["token"]:
            print(f"[{cc}] skipped: missing credentials")
            continue
        auth = (c["user"], c["token"])
        t0 = time.time()
        try:
            brs = branches(auth)
            act = active_members(auth)
            print(f"[{cc}] {len(brs)} branches, activos total={sum(act.values())}")
            time.sleep(PAUSE)
            for b in brs:
                bid = b["idBranch"]
                activos = int(act.get(bid, 0))
                deud, total_receipts = debtors(auth, bid)
                new_rows.append({
                    "Fecha": stamp,
                    "country": cc,
                    "idBranch": bid,
                    "Sede/club": b["name"],
                    "Clientes activos": activos,
                    "Deudores": int(deud),
                    "Suspensos": None,
                    "openingDate": b["openingDate"],
                    "source": "evo",
                })
                print(f"[{cc}] {b['name']:40} activos={activos:5} deudores={deud:4} (recibos={total_receipts})")
                time.sleep(PAUSE)
            print(f"[{cc}] done in {time.time()-t0:.0f}s")
        except Exception as ex:
            failures += 1
            print(f"[{cc}] FAILED: {ex}")

    if not new_rows:
        print("no new rows")
        sys.exit(1 if failures else 0)

    # one snapshot per (country, idBranch, UTC day): replace same-day rows
    done_today = {(r["country"], r["idBranch"]) for r in new_rows}
    kept = [r for r in rows if not (r.get("Fecha", "")[:10] == day and (r.get("country"), r.get("idBranch")) in done_today)]
    kept.extend(new_rows)
    cutoff = (now.timestamp() - MAX_HISTORY_DAYS * 86400)
    kept = [r for r in kept if datetime.fromisoformat(r["Fecha"]).timestamp() >= cutoff]
    kept.sort(key=lambda r: (r["Fecha"], r["country"], r["idBranch"]))

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(kept, f, ensure_ascii=False, indent=0)
    with open(os.path.join(DATA_DIR, "last_update.txt"), "w") as f:
        f.write(now.strftime("%Y-%m-%d %H:%M UTC"))
    print(f"WROTE {OUT}: {len(kept)} rows ({len(new_rows)} new)")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
