-- ============================================================
-- Table Gold : indicateurs agrégés par arrondissement × année
-- Alimentée par pipelines/gold/ — prête pour l'API et le dashboard
-- C'est la table de faits principale du star schema
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.indicateurs_gold (
    id                          SERIAL PRIMARY KEY,
    arrondissement              SMALLINT NOT NULL REFERENCES ude.arrondissements(arrondissement),
    annee                       SMALLINT NOT NULL,

    -- Indicateurs transactions DVF
    nb_transactions             INTEGER,
    prix_m2_median              NUMERIC(10,2),
    prix_m2_moyen               NUMERIC(10,2),
    prix_m2_q1                  NUMERIC(10,2),
    prix_m2_q3                  NUMERIC(10,2),
    surface_mediane             NUMERIC(8,2),
    part_appartements           NUMERIC(5,2),    -- % des transactions

    -- Indicateurs logements sociaux
    nb_logements_sociaux        INTEGER,
    nb_plai                     INTEGER,
    nb_plus_pluscd              INTEGER,
    nb_pls                      INTEGER,
    taux_logements_sociaux      NUMERIC(6,2),    -- % estimé (vs parc total)

    -- Indicateurs délinquance
    taux_cambriolages_pmille    NUMERIC(8,3),
    taux_violences_pmille       NUMERIC(8,3),
    taux_delinquance_global     NUMERIC(8,3),

    -- Indicateurs revenus (agrégé depuis IRIS)
    revenu_median_arr           NUMERIC(10,2),
    indice_gini_arr             NUMERIC(6,4),
    rapport_interdecile_arr     NUMERIC(6,2),

    -- Indicateur composite (score 0–100 calculé par business rules)
    score_attractivite          NUMERIC(5,2),

    computed_at                 TIMESTAMP DEFAULT NOW(),

    UNIQUE (arrondissement, annee)
);

CREATE INDEX IF NOT EXISTS idx_gold_arr    ON ude.indicateurs_gold(arrondissement);
CREATE INDEX IF NOT EXISTS idx_gold_annee  ON ude.indicateurs_gold(annee);

COMMENT ON TABLE ude.indicateurs_gold IS 'Table de faits Gold : indicateurs agrégés par arrondissement × année, prêts pour l API';
COMMENT ON COLUMN ude.indicateurs_gold.score_attractivite IS 'Score composite 0–100 combinant prix, logements sociaux, délinquance, revenus';
