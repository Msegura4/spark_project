# Mémo — Les structures de données Spark (Module 3)
> RDD, DataFrame, Dataset, Spark SQL

---

## 1. Les 3 niveaux d'abstraction

| Structure | Description | Usage |
|-----------|-------------|-------|
| **RDD[T]** | Bas niveau — collection distribuée de n'importe quel objet Python | Données non structurées, algo bas niveau |
| **DataFrame** | = Dataset[Row] — colonnes nommées et typées | **Par défaut en PySpark (90% des cas)** |
| **Dataset[T]** | API fortement typée Scala/Java uniquement | Non disponible en Python |

> Plus le niveau est élevé, plus **Catalyst** peut optimiser automatiquement.

---

## 2. Le RDD — Resilient Distributed Dataset

**5 propriétés fondamentales :**

- **Resilient** — tolérant aux pannes : partition perdue → recalculée depuis le lignage (lineage)
- **Distributed** — données découpées en partitions réparties sur le cluster
- **Dataset** — collection d'éléments de n'importe quel type Python
- **Immuable** — pas de modification en place, chaque transformation crée un nouveau RDD
- **Lazy evaluation** — les transformations sont planifiées mais pas exécutées jusqu'à une action

### Transformations (lazy) vs Actions (eager)

```python
# Transformations — retournent un nouveau RDD, rien n'est calculé
rdd2 = rdd1.map(lambda valeur: valeur * 2)
rdd3 = rdd2.filter(lambda valeur: valeur > 4)
# Autres : flatMap, union, groupByKey, reduceByKey, sortBy, distinct, join...

# Actions — déclenchent l'exécution du DAG complet
rdd3.count()           # nb éléments
rdd3.collect()         # tout au Driver  ⚠ danger sur gros volumes !
rdd3.take(5)           # 5 premiers
rdd3.reduce(lambda a, b: a + b)
```

### Création d'un RDD — 4 méthodes

```python
sc = spark.sparkContext
rdd1 = sc.parallelize([1, 2, 3, 4])              # depuis une liste Python
rdd_texte = sc.textFile("data/roman.txt")         # depuis un fichier texte
rdd_pair = sc.parallelize([("Paris", 1500.0)])    # RDD Pair (clé-valeur)
rdd_df = df.rdd.map(tuple)                        # depuis un DataFrame
```

---

## 3. Le DataFrame

Collection distribuée de colonnes nommées et typées. Analogue à un Pandas DataFrame mais sur cluster, ou une table SQL.

**Avantage clé :** le schéma est connu à l'avance → Catalyst peut optimiser (predicate pushdown, projection pushdown, fusion de filtres).

### Création — 5 méthodes

```python
# 1. Depuis une liste Python
df = spark.createDataFrame([("Alice", 1500.0)], schema=["nom", "ca"])

# 2. Schéma explicite (recommandé en production)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
schema = StructType([StructField("nom", StringType(), nullable=False),
                     StructField("ca", DoubleType(), nullable=True)])

# 3. Lecture de fichiers
df = spark.read.option("header", "true").csv("ventes.csv")
df = spark.read.parquet("data/")   # format colonnaire recommandé en prod

# 4. Depuis un RDD       5. Depuis Pandas
df = rdd.toDF(["nom", "age"])
df = spark.createDataFrame(pdf)    # prototypage uniquement
```

### Types de données Spark

| Type Spark | Type Python | Description |
|------------|-------------|-------------|
| StringType | str | Chaîne de caractères |
| IntegerType / LongType | int | Entier 32 / 64 bits |
| DoubleType | float | Flottant 64 bits |
| BooleanType | bool | Booléen |
| DateType / TimestampType | datetime.date / datetime | Date / Date + heure |
| ArrayType | list | Tableau d'éléments typés |
| MapType | dict | Dictionnaire clé-valeur |
| StructType | — | Structure imbriquée (JSON) |

> **Bonne pratique :** toujours définir un schéma **explicite** en production. `inferSchema=True` lit les données deux fois et peut inférer des types incorrects.

### Exploration

```python
from pyspark.sql import functions as F

df.show(5, truncate=False)    # aperçu
df.printSchema()              # types
df.describe().show()          # stats descriptives
df.count()                    # nb lignes

# Accès aux colonnes — F.col() est recommandé (pas d'ambiguïté)
df.select(F.col("nom"), F.col("ville"))
df.filter(F.col("ca") > 100)
df.select("ville").distinct().show()
```

---

## 4. Spark SQL — vues temporaires

```python
df.createOrReplaceTempView("ventes")                     # vue locale à la session
df.createOrReplaceGlobalTempView("ventes_global")        # vue globale (prefixe global_temp.)

result = spark.sql("""
    SELECT ville, SUM(ca) AS ca_total, COUNT(*) AS nb_vendeurs
    FROM ventes
    WHERE ca > 500
    GROUP BY ville
    ORDER BY ca_total DESC
""")
```

**Équivalences SQL / API DataFrame :**
`SELECT` → `.select()` | `WHERE` → `.filter()` | `GROUP BY` → `.groupBy()` | `ORDER BY` → `.orderBy()` | `JOIN` → `.join()` | `LIMIT` → `.limit()` | `DISTINCT` → `.distinct()`

---

## 5. RDD vs DataFrame — Quand choisir ?

| Critère | RDD | DataFrame (par défaut) |
|---------|-----|------------------------|
| Performance | Moins optimisé (pas de Catalyst) | Optimisé automatiquement |
| API | Fonctionnelle : map, filter, reduce | SQL-like : select, filter, groupBy |
| Données non structurées | Idéal (texte brut, objets libres) | Nécessite un schéma |
| ML moderne | API legacy | API moderne (MLlib DataFrame-based) |
| Structured Streaming | Non supporté | Support natif |

> **Règle :** DataFrame par défaut. RDD uniquement pour données non structurées, contrôle fin du partitionnement, ou algos Python complexes.

---

## 6. DAG et plan d'exécution

**Hiérarchie d'exécution :**
```
ACTION (.show(), .count()...)
  └── JOB (1 par action)
        ├── STAGE 1  (calculs sans shuffle — tasks parallèles par partition)
        └── STAGE 2  (après shuffle — nouveau découpage des données)
```
> Un nouveau Stage est créé à chaque opération de shuffle (groupBy, join, repartition, sortBy…)

### Catalyst Optimizer — 3 optimisations clés

- **Predicate pushdown** — les filtres sont appliqués le plus tôt possible (à la lecture du fichier)
- **Projection pushdown** — seules les colonnes nécessaires sont lues (crucial pour Parquet)
- **Fusion de filtres** — plusieurs `.filter()` consécutifs fusionnés en un seul passage

```python
df.explain()                   # plan physique
df.explain(mode="extended")    # 4 niveaux : Parsed / Analyzed / Optimized / Physical
```

---

## 7. Conversions entre structures

```python
# DataFrame → RDD
rdd = df.rdd.map(tuple)

# RDD → DataFrame
df = rdd.toDF(["col1", "col2"])
df = spark.createDataFrame(rdd, schema=schema)

# DataFrame → Pandas  ⚠ rapatrie TOUT en mémoire Driver
pdf = df.limit(1000).toPandas()    # toujours limiter !

# DataFrame → Spark SQL
df.createOrReplaceTempView("ma_table")
result = spark.sql("SELECT * FROM ma_table")
```

> **Performance :** les conversions RDD ↔ DataFrame ont un coût (sérialisation). Préférer rester dans le même paradigme. `Spark → Pandas` en production est dangereux sur gros volumes.
