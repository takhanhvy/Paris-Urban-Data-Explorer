-- ============================================================
-- Delinquance par arrondissement parisien
-- Source : Parquet INSEE (France entiere, filtre 75101-75120)
-- Ingestion : ingestion/files/feeder_delinquance.py
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.delinquance (
    id                      SERIAL PRIMARY KEY,
    arrondissement          SMALLINT REFERENCES ude.arrondissements(arrondissement),
    annee                   SMALLINT NOT NULL,
    code_geo                CHAR(5),            -- code INSEE (ex: 75101)
    indicateur              VARCHAR(200),        -- type de crime/delit
    nombre                  INTEGER,             -- nombre de faits
    taux_pour_mille         NUMERIC(8,3),        -- taux pour 1000 habitants
    unite_de_compte         VARCHAR(50),
    est_diffuse             BOOLEAN,
    insee_log               INTEGER,             -- nombre de logements (base calcul taux)
    insee_pop               INTEGER,             -- population (base calcul taux)
    complement_info_taux    TEXT,
    complement_info_nombre  TEXT,
    created_at              TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_delinq_arrondissement ON ude.delinquance(arrondissement);
CREATE INDEX IF NOT EXISTS idx_delinq_annee          ON ude.delinquance(annee);
CREATE INDEX IF NOT EXISTS idx_delinq_indicateur     ON ude.delinquance(indicateur);

COMMENT ON TABLE ude.delinquance IS
    'Statistiques de delinquance par arrondissement parisien — source INSEE, filtree sur 75101-75120';
COMMENT ON COLUMN ude.delinquance.taux_pour_mille IS
    'Taux de delinquance pour 1 000 habitants — base de l indicateur composite du dashboard';
