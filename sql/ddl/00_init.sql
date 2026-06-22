-- ============================================================
-- Urban Data Explorer — Initialisation PostgreSQL
-- À exécuter en premier : active PostGIS et crée le schéma
-- ============================================================

-- Extension géospatiale (nécessite image postgis/postgis)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Schéma dédié au projet
CREATE SCHEMA IF NOT EXISTS ude;

-- Commentaire de base de données
COMMENT ON SCHEMA ude IS 'Urban Data Explorer — schéma principal';
