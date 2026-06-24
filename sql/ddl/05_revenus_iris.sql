-- ============================================================
-- Revenus et niveau de vie par IRIS (INSEE FiLoSoFi 2018)
-- Source : BASE_TD_FILO_DEC_IRIS_2018.xlsx -- feuille IRIS_DEC
-- 12 395 lignes France entiere -> 870 IRIS parisiens apres filtrage
-- Ingestion : ingestion/files/feeder_revenus.py
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.revenus_iris (
    id              SERIAL PRIMARY KEY,
    iris            CHAR(9) NOT NULL UNIQUE,
    lib_iris        VARCHAR(200),
    arrondissement  SMALLINT REFERENCES ude.arrondissements(arrondissement),

    dec_med18       NUMERIC(10,2),
    dec_q118        NUMERIC(10,2),
    dec_q318        NUMERIC(10,2),
    dec_gi18        NUMERIC(6,4),
    dec_rd18        NUMERIC(6,2),

    dec_d118        NUMERIC(10,2),
    dec_d218        NUMERIC(10,2),
    dec_d318        NUMERIC(10,2),
    dec_d418        NUMERIC(10,2),
    dec_d618        NUMERIC(10,2),
    dec_d718        NUMERIC(10,2),
    dec_d818        NUMERIC(10,2),
    dec_d918        NUMERIC(10,2),

    dec_pact18      NUMERIC(5,2),
    dec_pcho18      NUMERIC(5,2),
    dec_ppen18      NUMERIC(5,2),
    dec_s80s2018    NUMERIC(6,2),

    annee_ref       SMALLINT DEFAULT 2018,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Ajouter les colonnes si la table existait avec un ancien schema
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS iris            CHAR(9);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS lib_iris        VARCHAR(200);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS arrondissement  SMALLINT;
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_med18       NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_q118        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_q318        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_gi18        NUMERIC(6,4);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_rd18        NUMERIC(6,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_d118        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_d218        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_d318        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_d418        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_d618        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_d718        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_d818        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_d918        NUMERIC(10,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_pact18      NUMERIC(5,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_pcho18      NUMERIC(5,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_ppen18      NUMERIC(5,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS dec_s80s2018    NUMERIC(6,2);
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS annee_ref       SMALLINT DEFAULT 2018;
ALTER TABLE ude.revenus_iris ADD COLUMN IF NOT EXISTS created_at      TIMESTAMP DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_revenus_arrondissement ON ude.revenus_iris(arrondissement);
CREATE INDEX IF NOT EXISTS idx_revenus_iris_code      ON ude.revenus_iris(iris);

-- COMMENTs conditionnels via DO block (evite erreur si colonne absente)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ude' AND table_name = 'revenus_iris' AND column_name = 'dec_gi18'
    ) THEN
        COMMENT ON COLUMN ude.revenus_iris.dec_gi18 IS
            'Indice de Gini : 0 = egalite parfaite, 1 = inegalite totale';
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'ude' AND table_name = 'revenus_iris' AND column_name = 'dec_med18'
    ) THEN
        COMMENT ON COLUMN ude.revenus_iris.dec_med18 IS
            'Revenu median par unite de consommation (UC) en euros annuels';
    END IF;
END $$;

COMMENT ON TABLE ude.revenus_iris IS
    'Revenus FiLoSoFi 2018 par IRIS -- 870 IRIS parisiens, agregation vers arrondissement pour le dashboard';
