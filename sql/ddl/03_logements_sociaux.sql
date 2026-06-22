-- ============================================================
-- Logements sociaux financés à Paris
-- Source : API opendata.paris.fr, 4 174 enregistrements
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.logements_sociaux (
    id                  SERIAL PRIMARY KEY,
    id_livraison        VARCHAR(50) UNIQUE,
    adresse_programme   TEXT,
    code_postal         CHAR(5),
    arrondissement      SMALLINT REFERENCES ude.arrondissements(arrondissement),
    annee               SMALLINT,
    nb_logmt_total      SMALLINT,
    nb_plai             SMALLINT,      -- Prêt Locatif Aidé d'Intégration (très social)
    nb_plus             SMALLINT,      -- Prêt Locatif à Usage Social
    nb_pluscd           SMALLINT,      -- PLUS Construction Démolition
    nb_pls              SMALLINT,      -- Prêt Locatif Social
    mode_real           VARCHAR(100),  -- mode de réalisation
    nature_programme    VARCHAR(100),
    commentaires        TEXT,
    longitude           DOUBLE PRECISION,
    latitude            DOUBLE PRECISION,
    geom                GEOMETRY(POINT, 4326),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_logsoc_arrondissement ON ude.logements_sociaux(arrondissement);
CREATE INDEX IF NOT EXISTS idx_logsoc_annee          ON ude.logements_sociaux(annee);
CREATE INDEX IF NOT EXISTS idx_logsoc_geom           ON ude.logements_sociaux USING GIST(geom);

COMMENT ON TABLE ude.logements_sociaux IS 'Logements sociaux financés à Paris — source opendata.paris.fr';
COMMENT ON COLUMN ude.logements_sociaux.nb_plai IS 'PLAI : logements pour les ménages les plus en difficulté';
COMMENT ON COLUMN ude.logements_sociaux.nb_pls  IS 'PLS : logements sociaux intermédiaires';
