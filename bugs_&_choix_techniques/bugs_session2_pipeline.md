# Bugs identifiés — Pipeline Session 2 (disponibilite_consolidee.parquet)

## Contexte

Le parquet consolidé `data/output/disponibilite_consolidee.parquet` est généré à la fin
de Session 2. Trois bugs ont été détectés lors de l'utilisation en Session 3.

---

## Bug 1 — `df_stations` utilisé avant d'être défini

**Fichier** : `Spark_DIA3_Session_2.ipynb`, Cell 19  
**Symptôme** : `NameError: name 'df_stations' is not defined`

### Cause
Cell 19 effectue deux jointures :
```python
df_joint = df_velib_join.join(broadcast(df_meteo_join), on="heure_tronquee", how="left")
df_joint = df_joint.join(broadcast(df_stations), on="nom_station", how="left")  # ← df_stations non défini ici
```

`df_stations` est défini dans Cell 21 (exercice), APRÈS Cell 19.
Python plante sur la 2e ligne → `df_joint` reste sans la jointure stations → pas de `code_arr`.

### Fix
Déplacer la définition de `df_stations` dans Cell 19, avant les jointures.

---

## Bug 2 — `code_arr` contient des IDs internes, pas des arrondissements

**Fichier** : `Spark_DIA3_Session_2.ipynb`, Cell 21  
**Symptôme** : `code_arr` = `213688`, `19179944`, etc. au lieu de `6`, `16`, `20`...

### Cause
La colonne `code_arr` de `stations_info.csv` est `station_id // 1000` (un identifiant
interne Vélib), pas le numéro d'arrondissement.

L'arrondissement se dérive depuis `stationCode` :
```
stationCode = 16107  →  16107 // 1000 = 16  (16ème arrondissement) ✓
stationCode = 10001  →  10001 // 1000 = 10  (10ème arrondissement) ✓
```

### Fix
Dans la définition de `df_stations`, utiliser `stationCode // 1000` plutôt que `code_arr` :
```python
df_stations = (
    spark.read.option("sep", ";").option("header", True).csv(str(STATIONS_CSV))
    .select(
        F.col("name").alias("nom_station"),
        (F.col("stationCode").cast("int") / 1000).cast("int").alias("code_arr")
    )
)
```

---

## Bug 3 — Aucun overlap temporel entre Vélib et météo

**Symptôme** : Colonnes `temperature_c`, `humidite_pct`, `vent_kmh`, `precipitation_mm`,
`est_pluie` entièrement NULL dans le parquet consolidé.

### Cause
| Source | Période couverte |
|--------|-----------------|
| `historique_stations.csv` (Vélib) | 2020-11-26 → 2021-02-09 |
| `paris_montsouris_horaire.csv` (météo) | 2022-01-01 → 2023-12-31 |

La jointure temporelle (`heure_tronquee`) ne produit aucun match → LEFT JOIN → tout NULL.

### Fix (choix pédagogique)
Décaler les timestamps Vélib de **+2 ans** dans Cell 13 :
```python
df_clean = df_velib.withColumn(
    "timestamp_parsed",
    to_timestamp(col("horodatage"), "yyyy-MM-dd'T'HH:mmX") + F.expr("INTERVAL 2 YEARS")
)
```
Résultat : les données Vélib couvrent `2022-11 → 2023-02`, overlap complet avec la météo.
Les colonnes `annee`/`mois` dérivées de `timestamp_parsed` donnent 2022/2023,
ce qui aligne aussi les filtres des sections Delta Lake (`annee == 2022/2023`).

---

## Résumé des corrections appliquées

| Cell | Changement |
|------|-----------|
| Cell 13 | `timestamp_parsed` + `INTERVAL 2 YEARS` |
| Cell 19 | Définition de `df_stations` déplacée ici (avec `stationCode // 1000`) |
| Cell 21 | Utilise `df_joint` directement (code_arr déjà présent) |
