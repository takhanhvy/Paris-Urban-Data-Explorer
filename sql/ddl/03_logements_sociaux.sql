-- ============================================================
-- Logements sociaux finances a Paris
-- Source : API opendata.paris.fr — 4 174 enregistrements
-- Ingestion : ingestion/api/feeder_logements_sociaux.py
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.logements_sociaux (
    id                  SERIAL PRIMARY KEY,
    id_livraison        VARCHAR(50) UNIQUE,
    arrondissement      SMALLINT REFERENCES ude.arrondissements(arrondissement),
    adresse_programme   TEXT,
    code_postal         CHAR(5),
    annee               SMALLINT,

    -- Nombre de logements par type de financement
    nb_logmt_total      INTEGER,
    nb_plai             INTEGER,    -- Pret Locatif Aide d'Integration (tres social)
    nb_plus             INTEGER,    -- Pret Locatif a Usage Social
    nb_pluscd           INTEGER,    -- PLUS Construction-Demolition
    nb_pls              INTEGER,    -- Pret Locatif Social (intermediaire)

    -- Infos programme
    mode_real           VARCHAR(100),   -- mode de realisation
    nature_programme    VARCHAR(200),
    commentaires        TEXT,

    -- Coordonnees geographiques
    longitude           NUMERIC(10,7),
    latitude            NUMERIC(10,7),
    geom                GEOMETRY(POINT, 4326),  -- WGS84

    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_logsoc_arrondissement ON ude.logements_sociaux(arrondissement);
CREATE INDEX IF NOT EXISTS idx_logsoc_annee          ON ude.logements_sociaux(annee);
CREATE INDEX IF NOT EXISTS idx_logsoc_geom           ON ude.logements_sociaux USING GIST(geom);

COMMENT ON TABLE ude.logements_sociaux IS
    'Logements sociaux finances a Paris — source API opendata.paris.fr, 4 174 enregistrements';
COMMENT ON COLUMN ude.logements_sociaux.nb_plai IS
    'PLAI : financement le plus social, loyers les plus bas';
