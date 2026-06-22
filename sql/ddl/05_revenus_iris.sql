-- ============================================================
-- Revenus et niveau de vie par IRIS (INSEE FiLoSoFi 2018)
-- Source : XLSX BASE_TD_FILO_DEC_IRIS_2018.xlsx
-- Granularité : IRIS (infra-communal) → jointure sur arrondissement
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.revenus_iris (
    id              SERIAL PRIMARY KEY,
    code_iris       VARCHAR(9) NOT NULL UNIQUE,    -- ex. '751010101'
    nom_iris        VARCHAR(150),
    code_commune    CHAR(5) NOT NULL,              -- '75101' à '75120'
    nom_commune     VARCHAR(50),
    arrondissement  SMALLINT REFERENCES ude.arrondissements(arrondissement),
    annee           SMALLINT DEFAULT 2018,

    -- Indicateurs de revenu (en euros par unité de consommation)
    revenu_median       NUMERIC(10,2),             -- DEC_MED18
    q1                  NUMERIC(10,2),             -- DEC_Q118 — 1er quartile
    q3                  NUMERIC(10,2),             -- DEC_Q318 — 3e quartile
    d1                  NUMERIC(10,2),             -- DEC_D118 — 1er décile
    d9                  NUMERIC(10,2),             -- DEC_D918 — 9e décile
    indice_gini         NUMERIC(6,4),              -- DEC_GI18 (0–1)
    rapport_interdecile NUMERIC(6,2),              -- DEC_RD18 (D9/D1)
    part_revenus_activ  NUMERIC(6,2),              -- DEC_PACT18 (%)
    part_chomage        NUMERIC(6,2),              -- DEC_PCHO18 (%)
    part_retraites      NUMERIC(6,2),              -- DEC_PPEN18 (%)
    rapport_s80s20      NUMERIC(6,2),              -- DEC_S80S2018

    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_revenus_arrondissement ON ude.revenus_iris(arrondissement);
CREATE INDEX IF NOT EXISTS idx_revenus_commune        ON ude.revenus_iris(code_commune);

COMMENT ON TABLE ude.revenus_iris IS 'Revenus FiLoSoFi 2018 par IRIS parisien — source INSEE XLSX';
COMMENT ON COLUMN ude.revenus_iris.indice_gini IS 'Indice de Gini : 0 = égalité parfaite, 1 = inégalité maximale';
