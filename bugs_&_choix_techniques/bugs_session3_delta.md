# Bugs et décisions techniques — Session 3 (Delta Lake, fenêtrage, SQL)

## Contexte

Session 3 utilise `data/output/disponibilite_consolidee.parquet` produit en Session 2.
Plusieurs erreurs ont été rencontrées en cours de route et des choix pédagogiques ont été faits.

---

## Bug 1 — `station_id` n'existe pas dans notre dataset

**Cellule** : `bd9a8d57` (MERGE INTO) et `58128e6e` (fenêtre cumulative)  
**Symptôme** : `AnalysisException: Column 'station_id' not found`

### Cause
Le notebook original utilisait `station_id` comme identifiant de station.
Notre pipeline Vélib ne génère pas cet identifiant, la colonne disponible est `nom_station`.

### Fix
Remplacer toutes les références à `station_id` par `nom_station`.

```python
# Avant
"cible.station_id = source.station_id AND ..."
Window.partitionBy("station_id")

# Après
"cible.nom_station = source.nom_station AND ..."
Window.partitionBy("nom_station")
```

---

## Bug 2 — `horodatage + INTERVAL` échoue sur une colonne StringType

**Cellule** : `84ddc921` (simulation batch)  
**Symptôme** : Les nouvelles lignes ont un `horodatage` NULL après le calcul.

### Cause
`horodatage` est stocké en `StringType` dans le parquet (ex: `"2022-12-08T01:20Z"`).
Spark ne peut pas additionner un `INTERVAL` directement à une string.

### Fix
Caster en timestamp, décaler, reformater en string :

```python
F.date_format(
    F.to_timestamp(F.col("horodatage"), "yyyy-MM-dd'T'HH:mmX") + F.expr("INTERVAL 2 YEARS"),
    "yyyy-MM-dd'T'HH:mm'Z'"
)
```

---

## Bug 3 — Filtre `annee=2022 AND mois=1` renvoie 0 lignes → AVG = NULL

**Cellule** : `84ddc921` et `726b49da` (time-travel)  
**Symptôme** : Les colonnes de résultat affichent NULL.

### Cause
Le décalage temporel +2 ans appliqué en Session 2 (fix Bug 3 Session 2) fait que
les données Vélib couvrent **novembre 2022 → février 2023**, pas janvier 2022.

| Mois original | Après +2 ans |
|---------------|-------------|
| Nov 2020      | Nov 2022    |
| Déc 2020      | Déc 2022    |
| Jan 2021      | Jan 2023    |
| Fév 2021      | Fév 2023    |

Filtrer `annee=2022 AND mois=1` (janvier 2022) retourne donc 0 lignes.

### Fix
Utiliser **décembre 2022** (le mois le plus riche : ~2,8M lignes) :

```python
filtre = (F.col("annee") == 2022) & (F.col("mois") == 12)
```

---

## Bug 4 — `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`

**Cellule** : `bd9a8d57` (MERGE INTO)  
**Symptôme** :
```
Cannot perform Merge as multiple source rows matched and attempted to modify
the same target row in the Delta table in possibly conflicting ways.
```

### Cause
La sémantique SQL du MERGE exige que chaque ligne source corresponde à **au plus une**
ligne cible. Dans nos données brutes, le couple `(nom_station, horodatage)` n'est pas
unique — plusieurs snapshots peuvent partager exactement le même horodatage pour la
même station (la fréquence de collecte Vélib génère des doublons à la minute).

### Fix
Dédupliquer `df_batch` sur la clé du MERGE avant d'exécuter :

```python
df_batch = (
    df_corrections.union(df_nouveaux)
    .drop("source")
    .dropDuplicates(["nom_station", "horodatage"])
)
```

### Règle générale
Avant tout MERGE Delta Lake, toujours vérifier que la table source est dédupliquée
sur les colonnes de la condition de correspondance.

---

## Décision — Clé du MERGE : `nom_station + horodatage`

La table ne dispose pas d'un identifiant technique unique (pas de `snapshot_id`).
Le couple `(nom_station, horodatage)` est la meilleure clé fonctionnelle disponible
pour identifier un snapshot de manière quasi-unique. Après déduplication, elle est
suffisante pour les besoins pédagogiques de la démonstration MERGE.

---

## Résumé des corrections appliquées

| Cellule     | Changement                                              |
|-------------|---------------------------------------------------------|
| `58128e6e`  | `station_id` → `nom_station` dans la fenêtre cumulative |
| `84ddc921`  | Cast horodatage string → timestamp avant INTERVAL       |
| `84ddc921`  | Filtre `mois=1` → `mois=12` (données réelles disponibles)|
| `84ddc921`  | `.dropDuplicates(["nom_station", "horodatage"])` sur df_batch |
| `bd9a8d57`  | Condition MERGE : `station_id` → `nom_station`          |
| `726b49da`  | Filtre time-travel : `mois=1` → `mois=12`               |
