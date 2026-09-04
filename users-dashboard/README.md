# Users Dashboard (Colombia / Mexico / Brasil / España)

Replica el visual "Summary" del PBIX: por cada sede se toma el ultimo snapshot
con Fecha <= fecha seleccionada, Pagantes = Activos - Deudores, % Deudores,
% Crecimiento mes (vs ultimo snapshot del mes anterior), Punto equilibrio,
Congelados, U_M2, con el mismo formato condicional. Vista TV (sin scroll,
auto-fit) y rotacion cada 120 s: DASHBOARD (CO) -> USUARIOS (MX/ES/BR) -> PREVENTAS (Power BI).

## Fuentes
| Pais | Usuarios (activos/deudores) | Sedes (estado, m2, PE, apertura) |
|---|---|---|
| CO | `UPSTREAM_BASE /api/usuarios/Operacion` (X-API-Key) | financialsab `/api/admin` |
| MX, BR | `users-dashboard/data/evo_snapshots.json` en GitHub, generado a diario por `evo_snapshot.py` (GitHub Action `users-refresh`, EVO API directa; ver `resources/CONNECT_EVO_*.md`) | financialsab `/api/admin` |
| ES | financialsab `/api/usuarios-es` (X-API-Key, cortes mensuales manuales; ver `resources/API_USUARIOS_ES.md`) | financialsab `/api/admin` |

Validez de sede = `estado` "activa" del admin (se ignoran inactivas, desaparecidas y
ACTION_SPORT_CLUB) y `apertura` <= fecha consultada. Nombres EVO/Excel se normalizan
(`lib/core.js` `sedeKey` + `ALIAS`) para cruzarlos con `display_name` del admin.

## Estructura
- `lib/core.js` - logica compartida (admin, fuentes, computeSnapshot).
- `api/snapshot.js` - `GET /api/snapshot?country=CO|MX|BR|ES&date=YYYY-MM-DD`.
- `public/index.html` - frontend.
- `evo_snapshot.py` + `requirements-job.txt` - job diario EVO (MX + BR).
- `.github/workflows/users-refresh.yml` - Action (cron 00:30 UTC + manual).

## Variables de entorno (Vercel)
- `BRANCHES_API_KEY`, `ADMIN_PASSWORD`, `ES_USERS_API_KEY` (requeridas)
- `ADMIN_BASE`, `UPSTREAM_BASE`, `EVO_SNAPSHOTS_URL`, `ZOMBIE_DAYS` (opcionales)

## Secrets GitHub (repo actionblackrep/dashboards)
`EVO_USERS_MX_USER`, `EVO_USERS_MX_TOKEN`, `EVO_USERS_BR_USER`, `EVO_USERS_BR_TOKEN`
(tokens de `resources/CONNECT_EVO_*.md`; no reutilizar los de factu.py).

## Deploy
```
npx vercel --prod
```
