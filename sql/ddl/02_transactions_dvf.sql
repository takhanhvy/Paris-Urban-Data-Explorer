-- ============================================================
-- Transactions immobilières — DVF (Demandes de Valeurs Foncières)
-- Source : CSV 2020–2025, ingéré via ingestion/files/feeder_dvf.py
-- Volume : plusieurs millions de lignes
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.transactions_dvf (
    id                      BIGSERIAL PRIMARY KEY,
    date_mutation           DATE NOT NULL,
    annee                   SMALLINT GENERATED ALWAYS AS (EXTRACT(YEAR FROM date_mutation)::SMALLINT) STORED,
    nature_mutation         VARCHAR(50),               -- 'Vente', 'Adjudication', etc.
    valeur_fonciere         NUMERIC(15,2),
    code_postal             CHAR(5),
    code_commune            CHAR(5),
    arrondissement          SMALLINT REFERENCES ude.arrondissements(arrondissement),
    type_local              VARCHAR(50),               -- 'Appartement', 'Maison', etc.
    surface_reelle_bati     NUMERIC(10,2),             -- m²
    nombre_pieces_princ     SMALLINT,
    prix_m2                 NUMERIC(12,2),             -- calculé : valeur_fonciere / surface
    longitude               DOUBLE PRECISION,
    latitude                DOUBLE PRECISION,
    geom                    GEOMETRY(POINT, 4326),     -- point WGS84 (PostGIS)
    source_fichier          VARCHAR(50),               -- nom du fichier CSV source
    loaded_at               TIMESTAMP DEFAULT NOW()
);

-- Index performance
CREATE INDEX IF NOT EXISTS idx_dvf_arrondissement ON ude.transactions_dvf(arrondissement);
CREATE INDEX IF NOT EXISTS idx_dvf_annee          ON ude.transactions_dvf(annee);
CREATE INDEX IF NOT EXISTS idx_dvf_type_local     ON ude.transactions_dvf(type_local);
CREATE INDEX IF NOT EXISTS idx_dvf_geom           ON ude.transactions_dvf USING GIST(geom);

-- Index composite pour les requêtes dashboard (arrondissement × année)
CREATE INDEX IF NOT EXISTS idx_dvf_arr_annee
    ON ude.transactions_dvf(arrondissement, annee)
    WHERE surface_reelle_bati > 0 AND valeur_fonciere > 0;

COMMENT ON TABLE ude.transactions_dvf IS 'Transactions immobilières DVF 2020–2025 filtrées sur Paris';
COMMENT ON COLUMN ude.transactions_dvf.prix_m2 IS 'Calculé lors du pipeline Silver : valeur_fonciere / surface_reelle_bati';
