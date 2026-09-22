# Bugs & Choix techniques — Session 5 (MLlib, K-Means, GBT, MLflow)

---

## Bug 1 — Chemin `DATA_DIR` incorrect (`../data` au lieu de `data`)


### Correction

```python
# Avant
DATA_DIR = Path("../data")

# Après
DATA_DIR = Path("data")
```

---

## Bug 2 — `station_id` absent de la table Delta

Le notebook Session 5 utilise `station_id` dans les fenêtres de calcul (Window),
le clustering K-Means et la carte Folium. Or la table Delta ne contient pas
cette colonne — elle n'a jamais été écrite dans le parquet ni dans Delta.


### Solution

Dérivation d'un `station_id` entier reproductible par hash du `nom_station` :

```python
df = df.withColumn("station_id", F.abs(F.hash(F.col("nom_station"))))
```

`F.hash()` est un hash Murmur3 32 bits intégré à Spark — le même nom de station
produit toujours le même identifiant, ce qui garantit la cohérence entre les
joins (K-Means, Folium, b422400f).

**Limite :** ce `station_id` est différent de celui du simulateur Session 4
(hash MD5) et de celui de `stations_info.csv` (ID officiel Vélib). Les trois
ne sont pas interopérables dans les faits...

---

## Bug 3 — `folium` non installé

### Solution

Ajout d'une cellule d'installation conditionnelle avant la cellule Folium :

```python
try:
    import folium
except ImportError:
    import subprocess, sys
    subprocess.run([sys.executable, "-m", "pip", "install", "folium", "-q"], check=True)
```

L'installation n'est déclenchée que si le module est absent, la cellule peut être relancée sans effet de bord.

---

## Bug 4 — SparkSession sans `configure_spark_with_delta_pip`

Le notebook configurait les extensions Delta via `.config(...)` mais appelait
directement `.getOrCreate()`. Sur certains environnements, cela provoque une
`ClassNotFoundException` pour les classes Delta au moment de lire le fichier
Delta (`spark.read.format("delta")`).

### Correction

```python
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName(...).config(...)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

---

## Choix technique — Split temporel train/test

Le split train/test est effectué sur l'année et non aléatoirement :

```python
df_train = df_ml.filter(col("annee") == 2022)
df_test  = df_ml.filter(col("annee") == 2023)
```

Un split aléatoire sur des séries temporelles crée une fuite d'infos:
le modèle observe des données du futur pendant l'entraînement, ce qui gonfle
artificiellement les métriques. En séparant par année, le modèle n'a accès
qu'aux données passées pour prédire le futur.

---

## Choix technique : Encodage des features temporelles

L'heure (0–23), le jour de semaine (0–6) et le mois (1–12) sont encodés
en sin/cos plutôt qu'en valeurs brutes :

```python
heure_sin = sin(2π × heure / 24)
heure_cos = cos(2π × heure / 24)
```

une valeur brute `heure=23` est numériquement éloignée de
`heure=0`, alors que ces deux instants sont consécutifs dans la journée.
L'encodage circulaire préserve cette proximité — le GBT peut apprendre
les patterns de fin/début de journée sans artefact de discontinuité.

---

## Choix technique — Carte Folium via join nom_station

`df_clusters` (issu du K-Means) ne contient que `station_id` (hash) et `cluster`.
Pour afficher les stations sur la carte, il faut leur latitude et longitude,
disponibles dans `stations_info.csv` sous la colonne `name` (= `nom_station`).

Le join est fait en deux étapes :
1) Récupération du `nom_station` depuis `df_ml` via `station_id`
2) Join avec le CSV sur `nom_station = name`

```python
df_nom_station  = df_ml.select("station_id", "nom_station").distinct()
df_clusters_nom = df_clusters.join(df_nom_station, on="station_id")
df_carte = df_clusters_nom.toPandas().merge(df_stations_pd, left_on="nom_station", right_on="name")
```

Le join se fait sur le nom et non sur un ID numérique car `stations_info.csv`
utilise l'ID officiel Vélib, incompatible avec notre hash Murmur3.
