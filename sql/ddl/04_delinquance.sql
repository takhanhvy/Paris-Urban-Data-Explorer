-- ============================================================
-- Délinquance par arrondissement parisien
-- Source : Parquet INSEE, filtré sur codes INSEE 75101–75120
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.delinquance (
    id                      SERIAL PRIMARY KEY,
    annee                   SMALLINT NOT NULL,
    arrondissement          SMALLINT REFERENCES ude.arrondissements(arrondissement),
    code_insee              CHAR(5) NOT NULL,
    indicateur              VARCHAR(200),              -- type de faits (ex. 'Cambriolages de logement')
    nombre                  INTEGER,                  -- nombre de faits constatés
    taux_pour_mille         NUMERIC(8,3),             -- pour 1000 habitants
    unite_de_compte         VARCHAR(50),              -- 'faits', 'victimes', etc.
    est_diffuse             BOOLEAN,
    insee_log               INTEGER,                  -- nb logements de référence
    insee_pop               INTEGER,                  -- population de référence
    loaded_at               TIMESTAMP DEFAULT NOW(),

    UNIQUE (annee, arrondissement, indicateur)
);

CREATE INDEX IF NOT EXISTS idx_delinquance_arrondissement ON ude.delinquance(arrondissement);
CREATE INDEX IF NOT EXISTS idx_delinquance_annee          ON ude.delinquance(annee);
CREATE INDEX IF NOT EXISTS idx_delinquance_indicateur     ON ude.delinquance(indicateur);

COMMENT ON TABLE ude.delinquance IS 'Faits de délinquance par arrondissement parisien — source INSEE Parquet';
