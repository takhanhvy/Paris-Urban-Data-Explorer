-- ============================================================
-- Table de référence : arrondissements de Paris
-- Clé de jointure centrale pour toutes les autres tables
-- ============================================================

CREATE TABLE IF NOT EXISTS ude.arrondissements (
    arrondissement      SMALLINT PRIMARY KEY,          -- 1 à 20
    code_insee          CHAR(5) NOT NULL UNIQUE,       -- '75101' à '75120'
    nom                 VARCHAR(50) NOT NULL,           -- ex. 'Paris 1er Arrondissement'
    code_postal         CHAR(5) NOT NULL,               -- '75001' à '75020'
    superficie_km2      NUMERIC(6,3),
    population_2020     INTEGER,
    densite_population  INTEGER,                        -- hab/km², calculé par Spark processor.py
    geom                GEOMETRY(MULTIPOLYGON, 4326),  -- polygone WGS84 (PostGIS)
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Index géospatial pour les requêtes de type "transactions dans cet arrondissement"
CREATE INDEX IF NOT EXISTS idx_arrondissements_geom
    ON ude.arrondissements USING GIST(geom);

COMMENT ON TABLE ude.arrondissements IS 'Table de référence des 20 arrondissements parisiens — clé de jointure centrale';
COMMENT ON COLUMN ude.arrondissements.geom IS 'Polygone WGS84 importé depuis un GeoJSON officiel (IGN)';

-- ── Migration idempotente ─────────────────────────────────────────────────────
-- Ces ALTER s'appliquent même si la table existait déjà (volume persistant).
-- ADD COLUMN IF NOT EXISTS est sans effet si la colonne est déjà présente.
ALTER TABLE ude.arrondissements ADD COLUMN IF NOT EXISTS superficie_km2     NUMERIC(6,3);
ALTER TABLE ude.arrondissements ADD COLUMN IF NOT EXISTS population_2020    INTEGER;
ALTER TABLE ude.arrondissements ADD COLUMN IF NOT EXISTS densite_population INTEGER;

-- ── Données de référence (upsert idempotent) ──────────────────────────────────
-- superficie_km2  : source IGN (arrondissements.csv)
-- population_2020 : source INSEE recensement 2020
-- densite_population : population / superficie (hab/km²)
INSERT INTO ude.arrondissements
    (arrondissement, code_insee, nom, code_postal, superficie_km2, population_2020, densite_population)
VALUES
    (1,  '75101', 'Paris 1er Arrondissement',   '75001', 1.825,  16266,  8912),
    (2,  '75102', 'Paris 2ème Arrondissement',  '75002', 0.992,  21559,  21733),
    (3,  '75103', 'Paris 3ème Arrondissement',  '75003', 1.171,  34576,  29527),
    (4,  '75104', 'Paris 4ème Arrondissement',  '75004', 1.601,  28088,  17544),
    (5,  '75105', 'Paris 5ème Arrondissement',  '75005', 2.541,  58850,  23161),
    (6,  '75106', 'Paris 6ème Arrondissement',  '75006', 2.153,  41100,  19090),
    (7,  '75107', 'Paris 7ème Arrondissement',  '75007', 4.088,  51765,  12661),
    (8,  '75108', 'Paris 8ème Arrondissement',  '75008', 3.881,  36808,  9484),
    (9,  '75109', 'Paris 9ème Arrondissement',  '75009', 2.178,  59895,  27500),
    (10, '75110', 'Paris 10ème Arrondissement', '75010', 2.893,  90372,  31238),
    (11, '75111', 'Paris 11ème Arrondissement', '75011', 3.667,  147476, 40219),
    (12, '75112', 'Paris 12ème Arrondissement', '75012', 6.383,  142327, 22296),
    (13, '75113', 'Paris 13ème Arrondissement', '75013', 7.146,  181556, 25406),
    (14, '75114', 'Paris 14ème Arrondissement', '75014', 5.621,  135964, 24188),
    (15, '75115', 'Paris 15ème Arrondissement', '75015', 8.502,  233484, 27462),
    (16, '75116', 'Paris 16ème Arrondissement', '75016', 7.913,  165820, 20955),
    (17, '75117', 'Paris 17ème Arrondissement', '75017', 5.669,  167476, 29543),
    (18, '75118', 'Paris 18ème Arrondissement', '75018', 6.005,  195233, 32511),
    (19, '75119', 'Paris 19ème Arrondissement', '75019', 6.786,  184389, 27170),
    (20, '75120', 'Paris 20ème Arrondissement', '75020', 5.983,  196004, 32762)
ON CONFLICT (arrondissement) DO UPDATE SET
    superficie_km2     = EXCLUDED.superficie_km2,
    population_2020    = EXCLUDED.population_2020,
    densite_population = EXCLUDED.densite_population;
