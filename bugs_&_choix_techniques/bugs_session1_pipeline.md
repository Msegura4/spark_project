# Bugs et décisions techniques — Session 1 (chargement des données, pipeline RDD)

## Contexte

Session 1 construit le pipeline de base : lecture du CSV Vélib brut via RDD,
transformation en DataFrame, et écriture du Parquet partitionné dans `data/velib/parquet/`.
Les données sources utilisées sont :

| Source | Fichier | Période couverte |
|--------|---------|-----------------|
| Vélib historique | `data/velib/raw/historique_stations.csv` | 2020-11-26 → 2021-02-09 |
| Météo Paris-Montsouris | `data/meteo/paris_montsouris_horaire.csv` | 2022-01-01 → 2023-12-31 |
| Infos stations | `data/velib/stations_info.csv` | référentiel statique |

---

## Décision — Absence de fichier météo couvrant la période Vélib (2020-2021)

**Découverte** : en Session 2, lors de la jointure temporelle Vélib × Météo,
toutes les colonnes météo (`temperature_c`, `humidite_pct`, etc.) sont entièrement NULL.

### Cause
Les deux sources n'ont aucun overlap temporel :

```
Vélib  : [2020-11-26 ──────────── 2021-02-09]
Météo  :                                      [2022-01-01 ──── 2023-12-31]
```

La jointure sur `heure_tronquee` ne produit aucune correspondance → LEFT JOIN → tout NULL.

### Options envisagées

| Option | Avantages | Inconvénients |
|--------|-----------|---------------|
| Télécharger un fichier météo 2020-2021 | Données réelles | Nécessite un accès API / téléchargement externe |
| Décaler les timestamps Vélib de +2 ans | Simple, pas de donnée externe | Dates "fausses" (2022/2023 au lieu de 2020/2021) |
| Décaler les timestamps météo de -2 ans | Symétrique | Même problème de dates fausses |

### Choix retenu — Décalage Vélib +2 ans (fix pédagogique)

Dans un contexte d'exercice, on a choisi de **décaler les timestamps Vélib de +2 ans**
dans la cellule de nettoyage (Session 2, Cell 13) :

```python
df_clean = df_velib.withColumn(
    "timestamp_parsed",
    to_timestamp(col("horodatage"), "yyyy-MM-dd'T'HH:mmX") + expr("INTERVAL 2 YEARS")
)
```

**Résultat** : les données Vélib couvrent désormais `2022-11 → 2023-02`,
ce qui crée un overlap complet avec la météo disponible.

**Effet de bord positif** : les colonnes `annee`/`mois` dérivées de `timestamp_parsed`
donnent 2022/2023, ce qui aligne aussi les filtres Delta Lake de Session 3
(`annee == 2022`, `annee == 2023`) sans modification supplémentaire.

**Limites** : les dates affichées dans les résultats (2022/2023) ne correspondent pas
aux dates réelles de collecte (2020/2021). À noter dans toute communication sur les résultats.

---

## Note — Couverture temporelle limitée des données Vélib

Le fichier `historique_stations.csv` ne couvre que **~2,5 mois** (nov 2020 → fév 2021).
C'est suffisant pour les exercices mais insuffisant pour des analyses saisonnières complètes
(pas d'été, pas de printemps dans les données).

Répartition des snapshots par mois :

| Mois original | Après +2 ans | Lignes |
|---------------|-------------|--------|
| Nov 2020      | Nov 2022    | ~540k  |
| Déc 2020      | Déc 2022    | ~2,8M  |
| Jan 2021      | Jan 2023    | ~1,5M  |
| Fév 2021      | Fév 2023    | ~430k  |

Décembre 2022 est de loin le mois le plus représenté (~53% des données).
