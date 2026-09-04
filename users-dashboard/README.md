# COL Users Dashboard

Dashboard de usuarios (solo Colombia) que replica el visual "Summary" del PBIX:
por cada sede se toma el ultimo snapshot con timestamp <= fecha seleccionada
(America/Bogota), se excluyen sedes zombie/invalidas y Pagantes = Activos - Deudores.
Tabla identica al PBIX (ID, Sede, Usuarios pagantes, Deudores, % Deudores,
% Crecimiento mes Activos, Punto equilibrio, Congelados, Nuevos, U_M2, Bajas)
con su mismo formato condicional.

## Fuentes de datos
- Usuarios: `UPSTREAM_BASE /api/usuarios/Operacion` (X-API-Key).
- Sedes: `ADMIN_BASE /api/admin` (login con cookie, ver
  `resources/CONNECT_ADMIN_DATA.md`). Filtra CO y excluye ACTION_SPORT_CLUB
  y desaparecida. La validez viene de la columna `estado`: se toman las
  "Activa" y se ignoran las inactivas. Mientras /api/admin no exponga ese
  campo (su GET proyecta columnas fijas, caveat 4 del doc), se usa el
  fallback is_presale/is_deleted; al aparecer `estado` en la respuesta el
  filtro cambia solo. El m2 vivo del admin alimenta U_M2 (fallback: Mt2 del
  master_sedes del PBIX).
- Punto Equilibrio y nombres de sede: master_sedes extraido del PBIX
  (embebido en `api/snapshot.js`).

`Planes vendidos` y `Bajas` llegan null desde el upstream, por eso
Nuevos mes/dia y Bajas se muestran en blanco.

## Estructura
- `api/snapshot.js` - `GET /api/snapshot?date=YYYY-MM-DD` (sin `date` = ayer
  en Bogota). Credenciales solo en el servidor.
- `public/index.html` - frontend estilo PBIX, pestañas AYER / HISTORICO.

## Variables de entorno (Vercel)
- `BRANCHES_API_KEY` (requerida)
- `ADMIN_PASSWORD` (requerida)
- `ADMIN_BASE` (opcional, default https://financialsab.vercel.app)
- `UPSTREAM_BASE` (opcional, default https://action-branches-api.vercel.app)
- `ZOMBIE_DAYS` (opcional, default 7)

## Deploy
```
npx vercel --prod
```
