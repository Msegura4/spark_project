# Fiche de cours — Spark Jour 1 : l'API RDD

**Projet** : ClimaCity Paris — données Vélib' + météo
**Contexte** : Partie 1 (matin) du notebook, dédiée à l'API bas niveau de Spark

---

## 1. Pourquoi Spark ?

Pandas fonctionne sur une seule machine, en mémoire. Il atteint ses limites quand :
- le volume dépasse la RAM disponible,
- il faut paralléliser sur plusieurs cœurs / machines.

**Spark** propose un modèle de calcul **distribué** et **tolérant aux pannes**, basé sur les **RDD** (*Resilient Distributed Datasets*) : des collections **immuables** et **partitionnées**.

Même en local (`local[*]`), Spark parallélise sur tous les cœurs et gère le débordement disque si les données dépassent la RAM.

---

## 2. SparkSession / SparkContext

`SparkSession` = point d'entrée unique (depuis Spark 2.0). Il encapsule :
- `SparkContext` → exécution RDD
- `SQLContext` → SQL / DataFrame
- `HiveContext`

**Une seule session par application.**

Architecture (même en local) :
```
SparkSession (driver)
  └─ SparkContext
       ├─ Executor 1 → Tasks
       └─ Executor 2 → Tasks
```
Un cœur logique = un slot d'exécution de tâche.

---

## 3. Chargement des données : `sc.textFile()`

- Retourne un **RDD de chaînes** (une ligne = un élément).
- **Instantané** : ne lit rien tout de suite, crée juste un plan de calcul.
- Le fichier est découpé en **partitions** → déterminent le parallélisme max.

---

## 4. Évaluation paresseuse (lazy evaluation) — concept central

| Transformations (paresseuses) | Actions (déclenchent le calcul) |
|---|---|
| `map()`, `filter()`, `flatMap()` | `count()`, `collect()`, `take()` |
| `groupByKey()`, `reduceByKey()` | `first()`, `top()`, `reduce()` |
| `join()`, `union()`, `distinct()` | `saveAsTextFile()`, `foreach()` |
| `repartition()`, `coalesce()` | `countByValue()`, `countByKey()` |

- Une **transformation** construit un **DAG** (graphe de calcul), sans rien exécuter.
- Une **action** déclenche réellement la lecture + le calcul.
- Sans `.cache()`, chaque action relit les données depuis le disque.

**DAG** : Spark traduit la chaîne de transformations en graphe orienté acyclique, visible dans le **Spark UI** (onglet Jobs → DAG Visualization). Les flèches entre stages = *shuffles* (transferts de données entre exécuteurs).

---

## 5. Transformations élémentaires

- **`map(f)`** : une entrée → une sortie. Utilisé ici pour parser chaque ligne CSV en tuple structuré.
- **`filter(f)`** : garde les éléments qui satisfont une condition (ex : retirer l'en-tête, ou les lignes invalides).
- **`flatMap(f)`** : une entrée → **0, 1 ou plusieurs** sorties. Utile pour "éclater" une structure imbriquée (ex : un snapshot → plusieurs paires heure/taux).

---

## 6. Agrégations clé-valeur : `reduceByKey`

- Regroupe les éléments par clé et combine les valeurs via une fonction associative.
- **Différence critique avec `groupByKey()`** :
  - `groupByKey()` : rassemble TOUTES les valeurs en mémoire avant de réduire → risque d'`OutOfMemoryError` à grande échelle.
  - `reduceByKey()` : réduit **localement** avant le shuffle → beaucoup moins de données transférées.

> Règle pratique : ne jamais utiliser `groupByKey()` quand `reduceByKey()` suffit.

---

## 7. `join` entre deux RDD

Deux RDD de paires `(clé, valeur)` peuvent être joints comme deux tables (ex : joindre `station_id → nom` avec `station_id → total_vélos`).

---

## 8. Cache / persistance

`.cache()` (= `.persist(StorageLevel.MEMORY_ONLY)`) : évite de relire les données depuis le disque à chaque action, utile quand un RDD est réutilisé plusieurs fois.

---

## 9. Pandas vs Spark

Comparaison pratique du calcul "top 10 stations par nombre de snapshots" dans les deux outils : approche différente (immédiate vs paresseuse), écart de temps et de mémoire à mesurer sur le jeu de données du projet.

---

## Notions clés à retenir pour l'examen

- RDD = immuable, partitionné, paresseux
- transformation ≠ action
- `map` (1→1) vs `flatMap` (1→N)
- `reduceByKey` > `groupByKey` en performance
- DAG + Spark UI pour observer l'exécution
- `.cache()` pour éviter les relectures disque
