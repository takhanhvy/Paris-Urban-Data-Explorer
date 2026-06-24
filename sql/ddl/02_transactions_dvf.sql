-- ============================================================
-- Transactions immobilieres DVF (Demandes de Valeurs Foncieres)
-- Source : CSV 2020-2025, ingere via ingestion/files/feeder_dvf.py
-- Volume : plusieurs millions de lignes (Paris, 20 arrondissements)
-- Cle de jointure : arrondissement -> ude.arrondissements
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.transactions_dvf (
    id                      BIGSERIAL PRIMARY KEY,
    arrondissement          SMALLINT REFERENCES ude.arrondissements(arrondissement),
    annee                   SMALLINT NOT NULL,
    date_mutation           DATE,
    nature_mutation         VARCHAR(50),        -- 'Vente', 'Adjudication', etc.
    valeur_fonciere         NUMERIC(14,2),      -- prix total en euros
    code_postal             CHAR(5),
    type_local              VARCHAR(50),        -- 'Appartement', 'Maison', etc.
    surface_reelle_bati     NUMERIC(8,2),       -- m2
    nombre_pieces_principales SMALLINT,
    code_commune            CHAR(5),
    code_type_local         CHAR(1),
    -- Prix au m2 calcule a l'ingestion
    prix_m2                 NUMERIC(10,2),
    created_at              TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dvf_arrondissement ON ude.transactions_dvf(arrondissement);
CREATE INDEX IF NOT EXISTS idx_dvf_annee          ON ude.transactions_dvf(annee);
CREATE INDEX IF NOT EXISTS idx_dvf_type_local     ON ude.transactions_dvf(type_local);
CREATE INDEX IF NOT EXISTS idx_dvf_date           ON ude.transactions_dvf(date_mutation);

COMMENT ON TABLE ude.transactions_dvf IS
    'Transactions immobilieres DVF 2020-2025 — table de faits principale du modele relationnel';
