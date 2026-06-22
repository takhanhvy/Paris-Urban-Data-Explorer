# Modèles de données — Urban Data Explorer

## 1. Schéma relationnel PostgreSQL (C1.1)

### Diagramme entité-relation

```
ude.arrondissements (PK: arrondissement)
        │
        ├──< ude.transactions_dvf (FK: arrondissement)
        │
        ├──< ude.logements_sociaux (FK: arrondissement)
        │
        ├──< ude.delinquance (FK: arrondissement)
        │
        ├──< ude.revenus_iris (FK: arrondissement)
        │
        └──< ude.indicateurs_gold (FK: arrondissement) ← table de faits principale
```

### Table centrale : `ude.arrondissements`

```sql
arrondissement  SMALLINT PK   -- 1 à 20
code_insee      CHAR(5)       -- '75101' à '75120'
nom             VARCHAR(50)
code_postal     CHAR(5)
superficie_km2  NUMERIC
population_2020 INTEGER
geom            GEOMETRY(MULTIPOLYGON, 4326)  -- PostGIS
```

### Table de faits : `ude.indicateurs_gold`

Clé composite : `(arrondissement, annee)`

```sql
-- Transactions DVF
prix_m2_median, prix_m2_moyen, prix_m2_q1, prix_m2_q3
surface_mediane, nb_transactions, part_appartements

-- Logements sociaux
nb_logements_sociaux, nb_plai, nb_plus_pluscd, nb_pls
taux_logements_sociaux

-- Délinquance
taux_cambriolages_pmille, taux_violences_pmille, taux_delinquance_global

-- Revenus (agrégé depuis IRIS)
revenu_median_arr, indice_gini_arr, rapport_interdecile_arr

-- Indicateur composite
score_attractivite  -- score 0-100
```

---

## 2. Schéma NoSQL MongoDB (C1.2)

### Collection `air_quality`

**Cas d'usage :** cache des données Airparif temps réel, avec expiration automatique après 24h.

**Index :** TTL sur `inserted_at` (expire après 86400s) + index sur `insee`.

```json
{
  "_id": ObjectId("..."),
  "insee": "75106",
  "timestamp": "2025-06-15T08:00:00Z",
  "inserted_at": ISODate("2025-06-15T08:00:01Z"),  // champ TTL
  "data": {
    "today": {
      "indice": 42,
      "qualificatif": "Bon",
      "couleur": "#50F0E6",
      "sous_indices": {
        "NO2": 15,
        "O3": 38,
        "PM10": 12,
        "PM25": 8
      }
    },
    "tomorrow": {
      "indice": 55,
      "qualificatif": "Moyen",
      "couleur": "#F0E641"
    }
  }
}
```

### Collection `arrondissement_shapes`

**Cas d'usage :** stockage des polygones GeoJSON pour le dashboard MapLibre. Index `2dsphere` pour les requêtes géospatiales.

```json
{
  "_id": ObjectId("..."),
  "arrondissement": 6,
  "code_insee": "75106",
  "nom": "Paris 6ème Arrondissement",
  "geometry": {
    "type": "Feature",
    "geometry": {
      "type": "MultiPolygon",
      "coordinates": [[[...]]]
    },
    "properties": {
      "arrondissement": 6,
      "superficie_km2": 2.15
    }
  },
  "updated_at": ISODate("2025-01-01T00:00:00Z")
}
```

**Index :** `{ "geometry.geometry": "2dsphere" }` + `{ "arrondissement": 1 }` (unique)

---

## 3. Star Schema — Modèle dimensionnel (Dashboard)

Le modèle dimensionnel est directement exploité par l'API pour les requêtes analytiques du dashboard (carte choroplèthe, timeline).

```
                    ┌─────────────────────┐
                    │   FAIT              │
                    │  indicateurs_gold   │
                    │  (arrondissement,   │
                    │   annee)            │
                    └──────┬──────────────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
  ┌──────────────┐ ┌─────────────┐ ┌──────────────┐
  │ DIM          │ │ DIM         │ │ DIM          │
  │arrondissement│ │  temps      │ │ type_bien    │
  │  (1–20)      │ │ (2020–2025) │ │ (Appt/Maison)│
  │  geom PostGIS│ │             │ │              │
  └──────────────┘ └─────────────┘ └──────────────┘
```

### Requêtes dashboard typiques

```sql
-- Carte choroplèthe 2024 : prix médian / arrondissement
SELECT a.arrondissement, a.geom, ig.prix_m2_median
FROM ude.indicateurs_gold ig
JOIN ude.arrondissements a USING (arrondissement)
WHERE ig.annee = 2024;

-- Timeline : évolution prix médian 1er arrondissement
SELECT annee, prix_m2_median
FROM ude.indicateurs_gold
WHERE arrondissement = 1
ORDER BY annee;

-- Comparateur : tous indicateurs pour 2 arrondissements
SELECT * FROM ude.indicateurs_gold
WHERE arrondissement IN (6, 11) AND annee = 2024;
```
