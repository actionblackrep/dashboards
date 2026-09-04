// Shared logic for all countries. Sede validity/metadata (estado, m2,
// punto_equilibrio, apertura) always comes from financialsab /api/admin
// (resources/CONNECT_ADMIN_DATA.md). Snapshot logic replicates the PBIX.

const UPSTREAM = process.env.UPSTREAM_BASE || "https://action-branches-api.vercel.app";
const API_KEY = process.env.BRANCHES_API_KEY;
const ADMIN_BASE = process.env.ADMIN_BASE || "https://financialsab.vercel.app";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;
const ES_API_KEY = process.env.ES_USERS_API_KEY;
const EVO_SNAPSHOTS_URL = process.env.EVO_SNAPSHOTS_URL ||
  "https://raw.githubusercontent.com/actionblackrep/dashboards/main/users-dashboard/data/evo_snapshots.json";
const EVO_SNAPSHOTS_FILE = process.env.EVO_SNAPSHOTS_FILE; // local testing only
const ZOMBIE_DAYS = parseInt(process.env.ZOMBIE_DAYS || "7", 10);
const CACHE_TTL_MS = 10 * 60 * 1000;
const BOGOTA_OFFSET_MS = -5 * 3600 * 1000;

// Colombia only: PBIX master_sedes fallback (Mt2 / PUNTO EQUILIBRIO / display name)
const MASTER_CO = {
"ALTO DE PALMAS": {
"name": "Alto De Palmas",
"mt2": 675.01,
"pe": 894
},
"ALTOS DEL PRADO": {
"name": "Altos Del Prado",
"mt2": 503.01,
"pe": 930
},
"ATLANTIS": {
"name": "Atlantis",
"mt2": 576.01,
"pe": 1028
},
"ATRIO": {
"name": "Atrio",
"mt2": 600.01,
"pe": 455
},
"AVES MARIA": {
"name": "Aves Maria",
"mt2": 855.01,
"pe": 1338
},
"BELEN": {
"name": "Belen",
"mt2": 1600.01,
"pe": 968
},
"C.C MULTIPLAZA - LA FELICIDAD": {
"name": "CC Multiplaza",
"mt2": 650.65,
"pe": 450
},
"C.C PARQUE LA COLINA": {
"name": "CC Colina",
"mt2": 543.65,
"pe": 653
},
"CAMPANELLA PLAZA": {
"name": "Campanella Plaza",
"mt2": 966.01,
"pe": 708
},
"CAMPESTRE": {
"name": "Campestre",
"mt2": 585.01,
"pe": 1256
},
"CC ARKADIA": {
"name": "CC Arkadia",
"mt2": 700.01,
"pe": 570
},
"CC FLORIDA": {
"name": "CC Florida",
"mt2": 712.01,
"pe": 792
},
"CEDRO BOLIVAR": {
"name": "Carulla Cedro Bolivar",
"mt2": 359.01,
"pe": 634
},
"CITY PLAZA": {
"name": "City Plaza",
"mt2": 723.01,
"pe": 1243
},
"CIUDADELA SABANETA": {
"name": "Ciudadela Sabaneta",
"mt2": 657.01,
"pe": 835
},
"COLINA": {
"name": "Colina",
"mt2": 928.01,
"pe": 1042
},
"CONNECTA 26": {
"name": "Connecta 26",
"mt2": 1267.01,
"pe": 1059
},
"CONNECTA 80": {
"name": "Connecta 80",
"mt2": 583.93,
"pe": 844
},
"CORPORATE CENTER": {
"name": "Corporate Center",
"mt2": 894.81,
"pe": 721
},
"CORTEZZA": {
"name": "Cortezza",
"mt2": 501.01,
"pe": 800
},
"EL EDEN": {
"name": "El Eden",
"mt2": 554.01,
"pe": 841
},
"EPIC PEREIRA": {
"name": "Epic Pereira",
"mt2": 709.19,
"pe": 797
},
"EXITO CHAPINERO": {
"name": "Exito Chapinero",
"mt2": 696.48,
"pe": 450
},
"EXITO POBLADO": {
"name": "Exito Poblado",
"mt2": 615.01,
"pe": 939
},
"EXITO SAN DIEGO": {
"name": "Exito San Diego",
"mt2": 595.75,
"pe": 600
},
"FIC 48 CD DEL RIO": {
"name": "Fic 48 Cd Del Rio",
"mt2": 840.01,
"pe": 684
},
"INFERIOR": {
"name": "Feeling",
"mt2": 438.01,
"pe": 829
},
"INTERMEDIA": {
"name": "Intermedia",
"mt2": 665.01,
"pe": 971
},
"LA 33": {
"name": "La 33",
"mt2": 845.01,
"pe": 1549
},
"LAS PALMAS BIO 26": {
"name": "Las Palmas Bio 26",
"mt2": 1247.01,
"pe": 705
},
"LAURELES": {
"name": "Laureles",
"mt2": 644.01,
"pe": 981
},
"LOS COLORES": {
"name": "Los Colores",
"mt2": 579.01,
"pe": 851
},
"LOS MOLINOS": {
"name": "Los Molinos",
"mt2": 1030.56,
"pe": 802
},
"MALL GRAN VIA": {
"name": "Mall Gran Via",
"mt2": 840.01,
"pe": 622
},
"MALL LA RESERVA": {
"name": "Mall La Reserva",
"mt2": 290.01,
"pe": 300
},
"MALLPLAZA NQS": {
"name": "Mall NQS",
"mt2": 600.01,
"pe": 435
},
"NIQUIA": {
"name": "Niquia",
"mt2": 700.01,
"pe": 1063
},
"OUTLET": {
"name": "Outlet",
"mt2": 740.01,
"pe": 642
},
"OUTLET ARAUCO SOPO": {
"name": "CC Parque Sopo",
"mt2": 583.39,
"pe": 450
},
"OVIEDO": {
"name": "Oviedo",
"mt2": 678.01,
"pe": 949
},
"PARK1433": {
"name": "1433 Park",
"mt2": 682.58,
"pe": 710
},
"PARQUE ARBOLEDA": {
"name": "CC Parque Arboleda",
"mt2": 840.01,
"pe": 450
},
"PARQUE FABRICATO": {
"name": "CC Plaza Fabricato",
"mt2": 890.21,
"pe": 450
},
"PEPE SIERRA": {
"name": "Pepe Sierra",
"mt2": 713.01,
"pe": 1102
},
"PLAZA CAMPESTRE": {
"name": "Plaza Campestre",
"mt2": 679.01,
"pe": 786
},
"PLAZA RIO GRANDE": {
"name": "Plaza Rio Grande",
"mt2": 579.01,
"pe": 871
},
"SABANETA": {
"name": "Sabaneta",
"mt2": 1265.01,
"pe": 2035
},
"SAN BERNARDO": {
"name": "San Bernardo",
"mt2": 1171.01,
"pe": 1289
},
"SAN LUCAS": {
"name": "San Lucas",
"mt2": 580.12,
"pe": 841
},
"SANTAFE BOGOTA": {
"name": "Santafe Bogota",
"mt2": 548.01,
"pe": 1103
},
"TESORO": {
"name": "Tesoro",
"mt2": 418.01,
"pe": 919
},
"UNICENTRO": {
"name": "Unicentro Cali",
"mt2": 855.01,
"pe": 1067
},
"VIVA BARRANQUILLA": {
"name": "Viva Barranquilla",
"mt2": 750.01,
"pe": 809
},
"VIVA ENVIGADO": {
"name": "Viva Envigado",
"mt2": 604.01,
"pe": 809
},
"VIVA PALMAS": {
"name": "Viva Alto Las Palmas",
"mt2": 506.01,
"pe": 809
}
};

const COUNTRIES = {
  CO: { label: "COLOMBIA", zombieDays: ZOMBIE_DAYS },
  MX: { label: "MEXICO", zombieDays: ZOMBIE_DAYS },
  BR: { label: "BRASIL", zombieDays: ZOMBIE_DAYS },
  ES: { label: "ESPAÑA", zombieDays: 45 }, // manual monthly cuts
};

// EVO branch name -> admin display_name (after normalization) when they differ
const ALIAS = {
  MX: {
    "SANTA FE": "EUROTEN SANTA FE",
    "PUNTO SAO PAULO": "SAO PAULO",
    "PASEO INTERLOMAS": "INTERLOMAS",
    "FORUM CUERNAVACA": "CUERNAVACA",
    "PARQUE DURAZNO": "PARQUE DURAZNOS",
    "LA ISLA MERIDA": "ISLA MERIDA",
  },
  BR: {},
  ES: {},
  CO: {},
};

function norm(s) {
  return String(s || "")
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toUpperCase()
    .trim()
    .replace(/\s+/g, " ");
}
// Strip EVO / Excel prefixes: "ACTION BLACK (BR) - BOTAFOGO" -> "BOTAFOGO", "Action Ventas" -> "VENTAS"
function sedeKey(name, country) {
  let k = norm(name)
    .replace(/^ACTION BLACK\s*(\((MX|BR|CO|ES)\))?\s*-?\s*/i, "")
    .replace(/^ACTION\s+/, "")
    .replace(/^-\s*/, "")
    .trim();
  const a = ALIAS[country] || {};
  return a[k] || k;
}

function truthy(v) {
  if (v == null) return false;
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v === 1;
  return ["1", "true"].includes(String(v).trim().toLowerCase());
}

// ---------- admin sedes (cookie auth, 12 h) ----------
let adminSession = { cookie: null, at: 0 };
let adminCache = { at: 0, rows: null };

async function adminLogin() {
  if (!ADMIN_PASSWORD) throw new Error("ADMIN_PASSWORD not configured");
  const r = await fetch(ADMIN_BASE + "/api/admin", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "login", password: ADMIN_PASSWORD }),
  });
  if (!r.ok) throw new Error(`Admin login -> ${r.status}`);
  const raw = typeof r.headers.getSetCookie === "function"
    ? r.headers.getSetCookie()
    : [r.headers.get("set-cookie")].filter(Boolean);
  const cookie = raw.map((s) => s.split(";")[0]).join("; ");
  if (!cookie) throw new Error("Admin login: no session cookie returned");
  adminSession = { cookie, at: Date.now() };
}

async function fetchAdminSedes() {
  const now = Date.now();
  if (adminCache.rows && now - adminCache.at < CACHE_TTL_MS) return adminCache.rows;
  if (!adminSession.cookie || now - adminSession.at > 11 * 3600 * 1000) await adminLogin();
  let r = await fetch(ADMIN_BASE + "/api/admin", { headers: { Cookie: adminSession.cookie } });
  if (r.status === 401) {
    await adminLogin();
    r = await fetch(ADMIN_BASE + "/api/admin", { headers: { Cookie: adminSession.cookie } });
  }
  if (!r.ok) throw new Error(`Admin /api/admin -> ${r.status}`);
  const j = await r.json();
  adminCache = { at: now, rows: Array.isArray(j.sedes) ? j.sedes : [] };
  return adminCache.rows;
}

// Schema-proof: only require what we compute on. Validity = estado "activa"
// (fallback to is_presale/is_deleted while the field is absent).
async function validSedesFor(country) {
  const rows = await fetchAdminSedes();
  const estadoOf = (b) => norm(b.estado ?? b.Estado ?? b.ESTADO ?? "");
  const hasEstado = rows.some((b) => estadoOf(b) !== "");
  const out = new Map(); // key -> { display, m2, pe, apertura }
  for (const b of rows) {
    if (String(b.country ?? "").trim().toUpperCase() !== country) continue;
    if (truthy(b.desaparecida)) continue;
    if (String(b.brand ?? "").trim().toUpperCase() === "ACTION_SPORT_CLUB") continue;
    if (hasEstado) {
      if (estadoOf(b).startsWith("INACTIV")) continue;
    } else if (truthy(b.is_presale) || truthy(b.is_deleted)) continue;
    const display = b.display_name ?? b.name;
    if (!display) continue;
    out.set(sedeKey(display, country), {
      display: String(display).trim(),
      m2: typeof b.m2 === "number" && b.m2 > 0 ? b.m2 : null,
      pe: typeof b.punto_equilibrio === "number" && b.punto_equilibrio > 0 ? b.punto_equilibrio : null,
      apertura: typeof b.apertura === "string" && b.apertura ? b.apertura : null,
    });
  }
  return out;
}

// ---------- record sources -> Map(sedeKey -> [{ts, activos, deudores, suspensos}]) ----------
const srcCache = {};
function cached(key, loader) {
  const c = srcCache[key];
  if (c && Date.now() - c.at < CACHE_TTL_MS) return c.p;
  const p = loader().catch((e) => { delete srcCache[key]; throw e; });
  srcCache[key] = { at: Date.now(), p };
  return p;
}

function pushRec(bySede, key, ts, activos, deudores, suspensos) {
  if (Number.isNaN(ts) || activos == null || deudores == null) return;
  if (!bySede.has(key)) bySede.set(key, []);
  bySede.get(key).push({ ts, activos: Number(activos), deudores: Number(deudores),
    suspensos: suspensos == null ? null : Number(suspensos) });
}
function finish(bySede) {
  for (const arr of bySede.values()) arr.sort((a, b) => a.ts - b.ts);
  return bySede;
}

async function loadCO() {
  return cached("CO", async () => {
    if (!API_KEY) throw new Error("BRANCHES_API_KEY not configured");
    const r = await fetch(UPSTREAM + "/api/usuarios/Operacion", { headers: { "X-API-Key": API_KEY } });
    if (!r.ok) throw new Error(`Upstream Operacion -> ${r.status}`);
    const j = await r.json();
    const raw = Array.isArray(j) ? j : j.data || [];
    const bySede = new Map();
    for (const x of raw)
      pushRec(bySede, sedeKey(x["Sede/club"], "CO"), Date.parse(x["Fecha"]), x["Clientes activos"], x["Deudores"], x["Suspensos"]);
    return finish(bySede);
  });
}

async function loadEvoAll() {
  return cached("EVO", async () => {
    let raw;
    if (EVO_SNAPSHOTS_FILE) {
      raw = JSON.parse(require("fs").readFileSync(EVO_SNAPSHOTS_FILE, "utf8"));
    } else {
      const r = await fetch(EVO_SNAPSHOTS_URL + "?t=" + Math.floor(Date.now() / 300000), { cache: "no-store" });
      if (r.status === 404) return [];
      if (!r.ok) throw new Error(`EVO snapshots -> ${r.status}`);
      raw = await r.json();
    }
    return Array.isArray(raw) ? raw : [];
  });
}
async function loadEVO(country) {
  const raw = await loadEvoAll();
  const bySede = new Map();
  for (const x of raw) {
    if (String(x.country || "").toUpperCase() !== country) continue;
    pushRec(bySede, sedeKey(x["Sede/club"], country), Date.parse(x["Fecha"]), x["Clientes activos"], x["Deudores"], x["Suspensos"]);
  }
  return finish(bySede);
}

async function loadES() {
  return cached("ES", async () => {
    if (!ES_API_KEY) throw new Error("ES_USERS_API_KEY not configured");
    const r = await fetch(ADMIN_BASE + "/api/usuarios-es", { headers: { "X-API-Key": ES_API_KEY } });
    if (!r.ok) throw new Error(`usuarios-es -> ${r.status}`);
    const j = await r.json();
    const bySede = new Map();
    for (const x of j.registros || []) {
      const activos = x.activos;
      let deudores = x.deudores;
      if (deudores == null && activos != null && x.pagantes != null) deudores = activos - x.pagantes;
      pushRec(bySede, sedeKey(x.sede, "ES"), Date.parse(String(x.fecha).slice(0, 10) + "T12:00:00Z"), activos, deudores, null);
    }
    return finish(bySede);
  });
}

async function loadRecords(country) {
  if (country === "CO") return loadCO();
  if (country === "ES") return loadES();
  return loadEVO(country);
}

// ---------- snapshot (PBIX logic) ----------
function utcDay(ts) { return Math.floor(ts / 86400000); }
function utcYearMonth(ts) { const d = new Date(ts); return d.getUTCFullYear() * 12 + d.getUTCMonth(); }

function latestAtOrBefore(arr, endMs) {
  let lo = 0, hi = arr.length - 1, ans = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid].ts <= endMs) { ans = mid; lo = mid + 1; } else hi = mid - 1;
  }
  return ans;
}

// records -> filter ts <= targetEnd -> latest per sede -> drop zombies -> sum
function computeSnapshot(bySede, validSedes, targetEndMs, opts) {
  const zombieDays = (opts && opts.zombieDays) || ZOMBIE_DAYS;
  const master = (opts && opts.master) || {};
  const unmatched = [];

  const chosen = new Map();
  for (const [sede, arr] of bySede) {
    if (!validSedes.has(sede)) { unmatched.push(sede); continue; }
    const i = latestAtOrBefore(arr, targetEndMs);
    if (i >= 0) chosen.set(sede, i);
  }
  if (chosen.size === 0) return { snapshot: null, unmatched };

  let globalMax = 0;
  for (const [sede, i] of chosen) { const ts = bySede.get(sede)[i].ts; if (ts > globalMax) globalMax = ts; }
  const cutoff = globalMax - zombieDays * 86400000;

  const rows = [];
  let tA = 0, tD = 0, zombies = 0;
  for (const [sede, i] of chosen) {
    const arr = bySede.get(sede);
    const cur = arr[i];
    if (cur.ts < cutoff) { zombies++; continue; }

    const info = validSedes.get(sede);
    if (info.apertura) {
      const ap = Date.parse(info.apertura + "T00:00:00Z");
      if (!Number.isNaN(ap) && ap > targetEndMs) continue; // not opened yet at that date
    }

    const pagantes = cur.activos - cur.deudores;
    const curYm = utcYearMonth(cur.ts);
    let prevMonthSnap = null;
    for (let k = i - 1; k >= 0; k--) {
      const ym = utcYearMonth(arr[k].ts);
      if (ym === curYm - 1) { prevMonthSnap = arr[k]; break; }
      if (ym < curYm - 1) break;
    }

    const m = master[sede];
    const pctDeudores = cur.activos > 0 ? cur.deudores / cur.activos : null;
    const crecActivos = prevMonthSnap && prevMonthSnap.activos ? cur.activos / prevMonthSnap.activos - 1 : null;
    const prevPag = prevMonthSnap ? prevMonthSnap.activos - prevMonthSnap.deudores : null;
    const crecPagantes = prevPag ? pagantes / prevPag - 1 : null;
    const peTarget = info.pe || (m && m.pe) || null;
    const mt2 = info.m2 || (m && m.mt2) || null;

    tA += cur.activos; tD += cur.deudores;
    rows.push({
      sede: (m && m.name) || info.display || sede,
      fecha: new Date(cur.ts).toISOString(),
      activos: cur.activos,
      deudores: cur.deudores,
      pagantes,
      pctDeudores,
      crecActivos,
      crecPagantes,
      puntoEq: peTarget ? pagantes / peTarget : null,
      congelados: cur.suspensos,
      um2: mt2 ? pagantes / mt2 : null,
    });
  }
  rows.sort((a, b) =>
    (b.pagantes + (b.pctDeudores || 0) + (b.puntoEq || 0)) -
    (a.pagantes + (a.pctDeudores || 0) + (a.puntoEq || 0)));
  rows.forEach((r, i) => { r.id = i + 1; });

  return {
    snapshot: {
      totals: { activos: tA, deudores: tD, pagantes: tA - tD },
      sedes: rows,
      snapshotTimestamp: new Date(globalMax).toISOString(),
      sedeCount: rows.length,
      zombiesExcluded: zombies,
    },
    unmatched,
  };
}

function bogotaDateStr(ms) { return new Date(ms + BOGOTA_OFFSET_MS).toISOString().slice(0, 10); }
function utcDateStr(ms) { return new Date(ms).toISOString().slice(0, 10); }
function endOfDay(dateStr) { return Date.parse(dateStr + "T23:59:59.999Z"); }

async function snapshotFor(country, dateStr) {
  const cfg = COUNTRIES[country];
  if (!cfg) throw new Error("unknown country " + country);
  const [bySede, validSedes] = await Promise.all([loadRecords(country), validSedesFor(country)]);
  const { snapshot, unmatched } = computeSnapshot(bySede, validSedes, endOfDay(dateStr), {
    zombieDays: cfg.zombieDays, master: country === "CO" ? MASTER_CO : {},
  });
  let minTs = Infinity, maxTs = 0;
  for (const arr of bySede.values()) {
    if (arr[0].ts < minTs) minTs = arr[0].ts;
    if (arr[arr.length - 1].ts > maxTs) maxTs = arr[arr.length - 1].ts;
  }
  return {
    country, label: cfg.label, date: dateStr, snapshot,
    meta: {
      minDate: maxTs ? utcDateStr(minTs) : null,
      maxDate: maxTs ? utcDateStr(maxTs) : null,
      unmatchedSedes: unmatched,
      validSedes: validSedes.size,
    },
  };
}

module.exports = { COUNTRIES, snapshotFor, bogotaDateStr, utcDateStr, endOfDay };
