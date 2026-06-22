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
    geom                GEOMETRY(MULTIPOLYGON, 4326),  -- polygone WGS84 (PostGIS)
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Index géospatial pour les requêtes de type "transactions dans cet arrondissement"
CREATE INDEX IF NOT EXISTS idx_arrondissements_geom
    ON ude.arrondissements USING GIST(geom);

COMMENT ON TABLE ude.arrondissements IS 'Table de référence des 20 arrondissements parisiens — clé de jointure centrale';
COMMENT ON COLUMN ude.arrondissements.geom IS 'Polygone WGS84 importé depuis un GeoJSON officiel (IGN)';

-- Données de référence (codes INSEE fixes)
INSERT INTO ude.arrondissements (arrondissement, code_insee, nom, code_postal) VALUES
    (1,  '75101', 'Paris 1er Arrondissement',  '75001'),
    (2,  '75102', 'Paris 2ème Arrondissement', '75002'),
    (3,  '75103', 'Paris 3ème Arrondissement', '75003'),
    (4,  '75104', 'Paris 4ème Arrondissement', '75004'),
    (5,  '75105', 'Paris 5ème Arrondissement', '75005'),
    (6,  '75106', 'Paris 6ème Arrondissement', '75006'),
    (7,  '75107', 'Paris 7ème Arrondissement', '75007'),
    (8,  '75108', 'Paris 8ème Arrondissement', '75008'),
    (9,  '75109', 'Paris 9ème Arrondissement', '75009'),
    (10, '75110', 'Paris 10ème Arrondissement','75010'),
    (11, '75111', 'Paris 11ème Arrondissement','75011'),
    (12, '75112', 'Paris 12ème Arrondissement','75012'),
    (13, '75113', 'Paris 13ème Arrondissement','75013'),
    (14, '75114', 'Paris 14ème Arrondissement','75014'),
    (15, '75115', 'Paris 15ème Arrondissement','75015'),
    (16, '75116', 'Paris 16ème Arrondissement','75016'),
    (17, '75117', 'Paris 17ème Arrondissement','75017'),
    (18, '75118', 'Paris 18ème Arrondissement','75018'),
    (19, '75119', 'Paris 19ème Arrondissement','75019'),
    (20, '75120', 'Paris 20ème Arrondissement','75020')
ON CONFLICT (arrondissement) DO NOTHING;
