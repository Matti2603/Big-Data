// Läuft nur beim ersten Start (leeres Volume). Legt zwei Benutzer mit
// getrennten Rechten an: die ETL-Strecke schreibt, die Webanwendung liest nur.
const mtg = db.getSiblingDB("mtg");

mtg.createUser({
  user: "mtg_etl",
  pwd: process.env.MONGO_ETL_PASSWORD,
  roles: [{ role: "readWrite", db: "mtg" }],
});

mtg.createUser({
  user: "mtg_web",
  pwd: process.env.MONGO_WEB_PASSWORD,
  roles: [{ role: "read", db: "mtg" }],
});
