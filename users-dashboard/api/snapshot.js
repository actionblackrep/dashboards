// GET /api/snapshot?country=CO|MX|BR|ES&date=YYYY-MM-DD
// No date -> yesterday in America/Bogota. See lib/core.js for the logic.
const { COUNTRIES, snapshotFor, bogotaDateStr } = require("../lib/core.js");

module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "s-maxage=300, stale-while-revalidate=600");
  try {
    const q = req.query || {};
    const country = String(q.country || "CO").toUpperCase();
    if (!COUNTRIES[country]) return res.status(400).json({ ok: false, error: "country must be CO|MX|BR|ES" });

    let dateStr = String(q.date || "");
    let mode = "historical";
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      dateStr = bogotaDateStr(Date.now() - 86400000);
      mode = "yesterday";
    }
    const out = await snapshotFor(country, dateStr);
    res.status(200).json({ ok: true, mode, timezone: "America/Bogota", ...out,
      meta: { ...out.meta, lastRefresh: new Date().toISOString() } });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
};
