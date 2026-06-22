-- ============================================================
-- Mise à jour : superficie et population des arrondissements
-- Source : INSEE 2020 + IGN (données officielles)
-- À exécuter UNE FOIS après l'init du schema
-- ============================================================

UPDATE ude.arrondissements SET superficie_km2 = 1.830, population_2020 = 17100  WHERE arrondissement = 1;
UPDATE ude.arrondissements SET superficie_km2 = 0.992, population_2020 = 22400  WHERE arrondissement = 2;
UPDATE ude.arrondissements SET superficie_km2 = 1.171, population_2020 = 34900  WHERE arrondissement = 3;
UPDATE ude.arrondissements SET superficie_km2 = 1.601, population_2020 = 28400  WHERE arrondissement = 4;
UPDATE ude.arrondissements SET superficie_km2 = 2.541, population_2020 = 60200  WHERE arrondissement = 5;
UPDATE ude.arrondissements SET superficie_km2 = 2.154, population_2020 = 41200  WHERE arrondissement = 6;
UPDATE ude.arrondissements SET superficie_km2 = 4.088, population_2020 = 52200  WHERE arrondissement = 7;
UPDATE ude.arrondissements SET superficie_km2 = 3.882, population_2020 = 37600  WHERE arrondissement = 8;
UPDATE ude.arrondissements SET superficie_km2 = 2.179, population_2020 = 60600  WHERE arrondissement = 9;
UPDATE ude.arrondissements SET superficie_km2 = 2.892, population_2020 = 93600  WHERE arrondissement = 10;
UPDATE ude.arrondissements SET superficie_km2 = 3.666, population_2020 = 147500 WHERE arrondissement = 11;
UPDATE ude.arrondissements SET superficie_km2 = 6.377, population_2020 = 141100 WHERE arrondissement = 12;
UPDATE ude.arrondissements SET superficie_km2 = 7.146, population_2020 = 177200 WHERE arrondissement = 13;
UPDATE ude.arrondissements SET superficie_km2 = 5.638, population_2020 = 136600 WHERE arrondissement = 14;
UPDATE ude.arrondissements SET superficie_km2 = 8.479, population_2020 = 235900 WHERE arrondissement = 15;
UPDATE ude.arrondissements SET superficie_km2 = 7.846, population_2020 = 163100 WHERE arrondissement = 16;
UPDATE ude.arrondissements SET superficie_km2 = 5.668, population_2020 = 169100 WHERE arrondissement = 17;
UPDATE ude.arrondissements SET superficie_km2 = 6.005, population_2020 = 195200 WHERE arrondissement = 18;
UPDATE ude.arrondissements SET superficie_km2 = 6.789, population_2020 = 188600 WHERE arrondissement = 19;
UPDATE ude.arrondissements SET superficie_km2 = 5.984, population_2020 = 195400 WHERE arrondissement = 20;

-- Vérification
SELECT arrondissement, nom, superficie_km2, population_2020,
       ROUND(population_2020 / superficie_km2) AS densite_hab_km2
FROM ude.arrondissements
ORDER BY arrondissement;
