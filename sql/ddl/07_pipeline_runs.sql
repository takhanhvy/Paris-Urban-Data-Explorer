-- ============================================================
-- Monitoring des pipelines (C2.4 — mesure de performance)
-- Alimentée par le décorateur @log_pipeline_run dans src/common/
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,    -- ex. 'silver_dvf', 'feeder_logements_sociaux'
    source          VARCHAR(50),              -- 'dvf', 'logements_sociaux', 'delinquance', etc.
    layer           VARCHAR(10),              -- 'bronze', 'silver', 'gold'
    run_date        DATE NOT NULL DEFAULT CURRENT_DATE,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    duration_s      NUMERIC(10,3),            -- secondes
    nb_lignes_in    BIGINT,                   -- lignes lues en entrée
    nb_lignes_out   BIGINT,                   -- lignes écrites en sortie
    volume_mb       NUMERIC(10,3),            -- volume traité en Mo
    statut          VARCHAR(20) DEFAULT 'running'  -- 'running', 'success', 'failed'
                    CHECK (statut IN ('running', 'success', 'failed', 'skipped')),
    erreur          TEXT,                     -- message d'erreur si failed
    metadata        JSONB                     -- infos additionnelles libres
);

CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON ude.pipeline_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_runs_date     ON ude.pipeline_runs(run_date);
CREATE INDEX IF NOT EXISTS idx_runs_statut   ON ude.pipeline_runs(statut);

COMMENT ON TABLE ude.pipeline_runs IS 'Log de performance de chaque exécution de pipeline — exploité pour C2.4';
