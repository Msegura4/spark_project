# Apache Spark MLlib
## Support de cours -- Apprentissage automatique distribué avec PySpark

**Niveau** : Licence Professionnelle / Master  
**Prerequis** : Python intermediaire, notions de machine learning (regression, clustering),
avoir lu les supports des Jours 1 et 2

---

## Sommaire

1. [Positionnement de MLlib](#1-positionnement-de-mllib)
2. [Les trois abstractions fondamentales](#2-les-trois-abstractions-fondamentales)
3. [Representation des donnees en MLlib](#3-representation-des-donnees-en-mllib)
4. [Preparation des donnees : les Transformers](#4-preparation-des-donnees--les-transformers)
5. [Apprentissage non supervise : le clustering K-Means](#5-apprentissage-non-supervise--le-clustering-k-means)
6. [Apprentissage supervise : la regression par arbres boostes](#6-apprentissage-supervise--la-regression-par-arbres-boostes)
7. [Evaluation des modeles](#7-evaluation-des-modeles)
8. [Validation croisee et selection d'hyperparametres](#8-validation-croisee-et-selection-dhyperparametres)
9. [Persistance des modeles](#9-persistance-des-modeles)
10. [Memento de l'API](#10-memento-de-lapi)
11. [Bonnes pratiques et pieges courants](#11-bonnes-pratiques-et-pieges-courants)

---

## 1. Positionnement de MLlib

### 1.1 Deux bibliotheques, une seule recommandation

Spark propose deux bibliotheques de machine learning :

| | `spark.mllib` | `spark.ml` |
|---|---|---|
| Support depuis | Spark 1.0 | Spark 1.3 |
| Structure de donnees | RDD | DataFrame |
| Statut | Maintenance uniquement (deprecie) | Actif, recommande |
| Pipelines | Non | Oui |
| Compatibilite MLflow | Partielle | Complete |

**Nous utilisons exclusivement `spark.ml`** dans ce cours. Tous les imports
proviennent du package `pyspark.ml`.

### 1.2 Pourquoi MLlib et pas scikit-learn ?

scikit-learn est excellent pour les donnees qui tiennent en memoire sur une seule
machine. MLlib devient pertinent dans trois situations :

1. **Le volume de donnees depasse la RAM** : scikit-learn charge tout en memoire,
   MLlib partitionne et traite les donnees par blocs.
2. **L'entrainement doit etre distribue** : sur un cluster, MLlib parallelise
   la construction des arbres de decision, le calcul des gradients, etc.
3. **Le modele s'integre dans un pipeline Spark existant** : lire depuis Delta,
   transformer avec DataFrame, predire avec MLlib, ecrire dans Delta -- tout en
   Spark, sans sortir du cluster.

En mode local (notre cas), MLlib n'offre pas d'avantage de performance sur
scikit-learn. La valeur est dans l'**integration** et dans la **capacite a
passer a l'echelle** sans changer le code.

---

## 2. Les trois abstractions fondamentales

### 2.1 Transformer

Un `Transformer` transforme un DataFrame en un autre DataFrame.  
Il implemente une methode `transform(df) -> DataFrame`.

```
DataFrame entree  -->  Transformer.transform()  -->  DataFrame sortie
(n colonnes)                                         (n + k colonnes)
```

Le `Transformer` ne "s'entraine" pas : il applique une transformation
deterministe dont les parametres sont soit fixes a la construction, soit
calcules en amont par un `Estimator`.

**Exemples de Transformers** : `VectorAssembler`, `Binarizer`,
`Bucketizer`, `SQLTransformer`, et tous les modeles entraines
(`GBTRegressionModel`, `KMeansModel`, `StandardScalerModel`...).

```python
from pyspark.ml.feature import VectorAssembler

# Construction du Transformer avec ses parametres
assembler = VectorAssembler(
    inputCols=["temperature_c", "heure_sin", "taux_lag1"],
    outputCol="features"
)

# Application : retourne un nouveau DataFrame avec la colonne "features" ajoutee
df_transforme = assembler.transform(df)
```

### 2.2 Estimator

Un `Estimator` est un algorithme qui s'entraine sur des donnees.  
Il implemente une methode `fit(df) -> Transformer`.

```
DataFrame entree  -->  Estimator.fit()  -->  Transformer (modele entraine)
(donnees d'entrainement)
```

Apres l'appel a `fit()`, l'`Estimator` a disparu : il a produit un
`Transformer` (le modele ou le preprocesseur ajuste) qu'on peut
ensuite appliquer sur n'importe quel DataFrame.

**Exemples d'Estimators** : `GBTRegressor`, `KMeans`, `StandardScaler`,
`StringIndexer`, `CrossValidator`.

```python
from pyspark.ml.feature import StandardScaler

# L'Estimator : il ne connait pas encore mean/std
scaler = StandardScaler(
    inputCol="features_brutes",
    outputCol="features",
    withMean=True,
    withStd=True
)

# fit() calcule mean et std sur df_train -> retourne un StandardScalerModel
scaler_model = scaler.fit(df_train)   # StandardScalerModel est un Transformer

# transform() applique la normalisation
df_normalise = scaler_model.transform(df_test)
```

> **Regle fondamentale** : on appelle `fit()` uniquement sur les **donnees
> d'entrainement**. Appeler `fit()` sur les donnees de test constitue une
> **fuite d'information** (*data leakage*) : le modele aurait connaissance
> des statistiques du futur.

### 2.3 Pipeline

Un `Pipeline` est une sequence ordonnee d'`Estimators` et de `Transformers`.
C'est lui-meme un `Estimator` : son `fit()` retourne un `PipelineModel`,
qui est un `Transformer`.

```python
from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[assembler, scaler, gbt])

# Un seul appel fit() entraine toute la chaine
model = pipeline.fit(df_train)   # retourne un PipelineModel

# Un seul appel transform() applique toute la chaine
df_pred = model.transform(df_test)
```

**Ce que fait `pipeline.fit(df_train)` en interne :**

```
Stage 1 : assembler  (Transformer) -> transform(df_train) -> df_1
Stage 2 : scaler     (Estimator)   -> fit(df_1) -> scaler_model
                                   -> scaler_model.transform(df_1) -> df_2
Stage 3 : gbt        (Estimator)   -> fit(df_2) -> gbt_model
                                   -> gbt_model.transform(df_2) -> df_3

Resultat : PipelineModel(stages=[assembler, scaler_model, gbt_model])
```

**Ce que fait `model.transform(df_test)` en interne :**

```
Stage 1 : assembler.transform(df_test)     -> df_1
Stage 2 : scaler_model.transform(df_1)    -> df_2
Stage 3 : gbt_model.transform(df_2)       -> df_3  (avec colonne "prediction")
```

Le `Pipeline` garantit qu'aucune etape ne voit les donnees de test
pendant l'entrainement : toute la chaine `fit` se passe uniquement
sur `df_train`.

### 2.4 Paramètres : `Param` et `ParamMap`

Chaque `Estimator` et `Transformer` expose ses hyperparametres via
le systeme `Param`. Cela permet l'introspection et la validation.

```python
# Lister les parametres d'un estimateur
print(gbt.explainParams())

# Lire la valeur d'un parametre
print(gbt.getMaxDepth())      # -> 5
print(gbt.getMaxIter())       # -> 50

# Modifier un parametre apres construction
gbt2 = gbt.setMaxDepth(7)     # retourne un nouvel objet (immutabilite)

# Passer un dictionnaire de parametres a fit() ou transform()
override = {gbt.maxDepth: 7, gbt.maxIter: 100}
model = pipeline.fit(df_train, params=override)
```

---

## 3. Representation des donnees en MLlib

### 3.1 La colonne "features"

MLlib attend que toutes les variables explicatives soient reunies dans
**une seule colonne** de type `Vector`. C'est le role du `VectorAssembler`.

```
df avant VectorAssembler :
+------+----------+----------+
|heure |temperature|taux_lag1|
+------+----------+----------+
|  8   |  12.3    |   0.45  |
|  9   |  11.8    |   0.62  |

df apres VectorAssembler :
+------+----------+----------+---------------------+
|heure |temperature|taux_lag1| features            |
+------+----------+----------+---------------------+
|  8   |  12.3    |   0.45  | [8.0, 12.3, 0.45]  |
|  9   |  11.8    |   0.62  | [9.0, 11.8, 0.62]  |
```

### 3.2 Vecteurs denses et vecteurs creux

MLlib utilise deux representations internes :

```python
from pyspark.ml.linalg import Vectors

# Vecteur dense : toutes les valeurs stockees
v_dense = Vectors.dense([1.0, 0.0, 3.0, 0.0])

# Vecteur creux (sparse) : seules les valeurs non nulles sont stockees
# Utile pour les donnees tres sparses (NLP, donnees categoriques encodees)
# Vectors.sparse(taille, [(index, valeur), ...])
v_sparse = Vectors.sparse(4, [(0, 1.0), (2, 3.0)])

# Les deux sont equivalents pour MLlib
```

`VectorAssembler` produit des vecteurs denses par defaut.

### 3.3 Schema requis

MLlib est strict sur les types de colonnes. Avant d'assembler les features,
toutes les colonnes doivent etre de type numerique (`DoubleType`, `FloatType`,
`IntegerType`). Les booleens et les strings doivent etre converts au prealable.

```python
from pyspark.sql.functions import col

# Conversion booleens -> entiers (obligatoire pour VectorAssembler)
df = df.withColumn("est_pluie_int",   col("est_pluie").cast("int"))
df = df.withColumn("est_weekend_int", col("est_weekend").cast("int"))
```

---

## 4. Preparation des donnees : les Transformers

### 4.1 VectorAssembler

Reunit plusieurs colonnes numeriques en un vecteur de features.

```python
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=[                  # liste des colonnes a assembler
        "heure_sin", "heure_cos",
        "temperature_c", "est_pluie_int",
        "taux_lag1", "taux_lag4",
    ],
    outputCol="features_brutes", # nom de la colonne vecteur produite
    handleInvalid="skip"         # "error" (defaut), "skip" ou "keep"
                                 # skip : exclut les lignes avec des nulls
                                 # keep : remplace par 0.0
)
```

> **`handleInvalid`** est important en production : un null dans une feature
> levait une exception avant Spark 2.4. Avec `"skip"`, la ligne est silencieusement
> exclue -- verifiez le nombre de lignes avant et apres `transform()`.

### 4.2 StandardScaler

Normalise les features pour qu'elles aient une moyenne nulle et/ou
un ecart-type unitaire. Indispensable pour les algorithmes sensibles
aux echelles (clustering, SVM, regression avec regularisation).

Le GBTRegressor est base sur des arbres de decision et est theoriquement
insensible aux echelles -- mais normaliser ne fait pas de mal et
facilite l'interpretation.

```python
from pyspark.ml.feature import StandardScaler

scaler = StandardScaler(
    inputCol="features_brutes",
    outputCol="features",
    withMean=True,   # centre sur la moyenne (produit des vecteurs denses)
    withStd=True     # reduit par l'ecart-type
)

# fit() calcule mean et std sur les donnees d'entrainement
scaler_model = scaler.fit(df_train)

# Les statistiques apprises
print(scaler_model.mean)   # vecteur des moyennes
print(scaler_model.std)    # vecteur des ecarts-types

# transform() applique : x_normalise = (x - mean) / std
df_norme = scaler_model.transform(df_train)
```

> **Attention** : `withMean=True` convertit les vecteurs creux en vecteurs
> denses (car soustraire la moyenne rend toutes les valeurs non nulles).
> Sur des donnees tres sparses (millions de features NLP), cela peut exploser
> la memoire. Utilisez `withMean=False` dans ce cas.

### 4.3 Autres scalers disponibles

```python
from pyspark.ml.feature import MinMaxScaler, MaxAbsScaler, RobustScaler

# MinMaxScaler : ramene chaque feature dans [min_, max_] (defaut [0, 1])
# Sensible aux outliers
MinMaxScaler(inputCol="f", outputCol="f_scaled", min=0.0, max=1.0)

# MaxAbsScaler : divise par max(|x|), produit des valeurs dans [-1, 1]
# Preserve les zeros (utile pour les donnees sparses)
MaxAbsScaler(inputCol="f", outputCol="f_scaled")

# RobustScaler : utilise la mediane et l'IQR (resistant aux outliers)
# Disponible depuis Spark 3.0
RobustScaler(inputCol="f", outputCol="f_scaled",
             withCentering=True, withScaling=True)
```

### 4.4 Encodage des variables categorielles

MLlib ne peut pas utiliser des strings directement. Il faut les convertir.

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# Etape 1 : StringIndexer -> transforme "Paris", "Lyon"... en 0.0, 1.0, 2.0...
indexer = StringIndexer(
    inputCol="ville",
    outputCol="ville_idx",
    handleInvalid="keep"    # valeurs inconnues en test -> categorie supplementaire
)

# Etape 2 : OneHotEncoder -> transforme 0.0, 1.0, 2.0 en vecteurs binaires
encoder = OneHotEncoder(
    inputCols=["ville_idx"],
    outputCols=["ville_ohe"],
    dropLast=True    # elimine la derniere categorie pour eviter la colinearite
)
```

### 4.5 Encodage cyclique (sin/cos)

Les variables temporelles cycliques (heure, jour de la semaine, mois) ont
une structure circulaire : l'heure 23 est "proche" de l'heure 0. Encoder
naïvement avec un entier (0..23) trompe le modele.

La solution standard est le **double encodage sinus/cosinus** :

```
heure=0  -> sin(0 * 2π/24) = 0.000  cos(0 * 2π/24) = 1.000
heure=6  -> sin(6 * 2π/24) = 1.000  cos(6 * 2π/24) = 0.000
heure=12 -> sin(12 * 2π/24) = 0.000  cos(12 * 2π/24) = -1.000
heure=18 -> sin(18 * 2π/24) = -1.000 cos(18 * 2π/24) = 0.000
heure=23 -> sin(23 * 2π/24) = -0.259 cos(23 * 2π/24) = 0.966
    -> proche de heure=0 : sin=0.000 cos=1.000  (distance euclidienne faible)
```

```python
from pyspark.sql.functions import sin, cos, lit
import math

PI = math.pi

df = df.withColumn("heure_sin",   sin(col("heure")    * (2 * PI / 24)))
df = df.withColumn("heure_cos",   cos(col("heure")    * (2 * PI / 24)))
df = df.withColumn("joursem_sin", sin(col("jour_sem") * (2 * PI / 7)))
df = df.withColumn("joursem_cos", cos(col("jour_sem") * (2 * PI / 7)))
df = df.withColumn("mois_sin",    sin(col("mois")     * (2 * PI / 12)))
df = df.withColumn("mois_cos",    cos(col("mois")     * (2 * PI / 12)))
```

### 4.6 Features de lag (variables retardees)

Pour les series temporelles, l'etat passe d'une variable est souvent
la meilleure prediction de son etat futur. On construit des features
de lag avec les fonctions de fenetrage Spark.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, avg

# La fenetre est partitionnee par station et triee par temps
# (on veut le lag AU SEIN de chaque station, pas entre stations)
fenetre = Window.partitionBy("station_id").orderBy("horodatage")

df = df.withColumn("taux_lag1",  lag("taux_occupation", 1).over(fenetre))
df = df.withColumn("taux_lag4",  lag("taux_occupation", 4).over(fenetre))
df = df.withColumn("taux_moy4",  avg("taux_occupation").over(
    fenetre.rowsBetween(-4, -1)   # moyenne des 4 snapshots precedents
))

# Supprimer les premieres lignes de chaque station (lag = null)
df = df.dropna(subset=["taux_lag1", "taux_lag4", "taux_moy4"])
```

> **Piege** : si vous ne partitionnez pas la fenetre par `station_id`, Spark
> calculera le lag entre la derniere ligne d'une station et la premiere ligne
> de la station suivante -- ce qui n'a aucun sens.

---

## 5. Apprentissage non supervise : le clustering K-Means

### 5.1 Principe de l'algorithme

K-Means partitionne n observations en k groupes en minimisant
la somme des carres des distances intra-cluster (inertie).

**Algorithme de Lloyd (version standard) :**

```
1. Initialiser k centroides aleatoirement (ou avec K-Means++)
2. Repeter jusqu'a convergence :
   a. Affectation : chaque point est assigne au centroide le plus proche
   b. Mise a jour : chaque centroide est recalcule comme la moyenne
      des points qui lui sont assignes
3. Terminer quand les centroides ne bougent plus (ou apres maxIter iterations)
```

**Complexite** : O(n * k * d * i) ou n = points, k = clusters,
d = dimensions, i = iterations.

**Parallelisation dans Spark** : chaque executeur calcule les distances
et les moyennes partielles sur sa partition. Le driver agreage les
moyennes partielles pour mettre a jour les centroides.

### 5.2 Choisir k : la methode du coude

K-Means requiert de specifier k a l'avance. La **methode du coude**
consiste a tracer l'inertie (WSSSE) en fonction de k et a choisir
la valeur ou la courbe forme un "coude" -- le gain marginal d'ajouter
un cluster supplementaire devient negligeable.

```
Inertie
  |
  |*
  | *
  |  *
  |   *
  |    *_______________
  |                    *___________
  +--+--+--+--+--+--+--+--+--+----> k
     2  3  4  5  6  7  8  9  10

       ^--- coude ici (k=5 dans cet exemple)
```

Il n'y a pas de coude mathematiquement net dans tous les jeux de donnees.
L'interpretation reste subjective -- c'est un outil d'aide a la decision,
pas un algorithme deterministe.

### 5.3 KMeans en MLlib

```python
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

kmeans = KMeans(
    featuresCol="features_scaled",  # colonne vecteur d'entree
    predictionCol="cluster",         # colonne de sortie (entier 0..k-1)
    k=4,                             # nombre de clusters
    maxIter=100,                     # iterations maximum
    tol=1e-4,                        # tolerance de convergence
    seed=42,                         # reproductibilite
    initMode="k-means||",            # "random" ou "k-means||" (defaut, meilleur)
    initSteps=2,                     # etapes d'initialisation K-Means||
    distanceMeasure="euclidean"      # "euclidean" ou "cosine"
)

# Entrainement
model_km = kmeans.fit(df_train)

# Prediction (affectation de chaque point a un cluster)
df_clusters = model_km.transform(df)

# Acces aux centroides
centroides = model_km.clusterCenters()  # liste de numpy arrays
for i, c in enumerate(centroides):
    print(f"Centroide {i} : {c}")
```

### 5.4 Inertie et evaluation

```python
# Inertie (WSSSE) -- accessible via le summary de l'entrainement
wssse = model_km.summary.trainingCost
print(f"Inertie (WSSSE) pour k={kmeans.getK()} : {wssse:.2f}")

# Score de silhouette -- mesure la coherence des clusters
# entre -1 (mauvais) et 1 (parfait), 0 indique des clusters qui se chevauchent
evaluator = ClusteringEvaluator(
    featuresCol="features_scaled",
    predictionCol="cluster",
    metricName="silhouette",        # "silhouette" ou "squaredSilhouette"
    distanceMeasure="squaredEuclidean"
)
silhouette = evaluator.evaluate(df_clusters)
print(f"Score de silhouette : {silhouette:.4f}")
```

### 5.5 Construction du profil de station par pivot

Pour le projet ClimaCity, le vecteur de profil de chaque station est
son taux d'occupation moyen a chaque heure de la journee (24 dimensions).

```python
from pyspark.sql.functions import avg as spark_avg

# Pivot : une ligne par station, une colonne par heure
df_profil = (
    df
    .groupBy("station_id")
    .pivot("heure", list(range(24)))   # valeurs possibles de "heure" : 0..23
    .agg(spark_avg("taux_occupation"))
)

# Renommage : 0 -> h00, 1 -> h01, ...
for h in range(24):
    df_profil = df_profil.withColumnRenamed(str(h), f"h{h:02d}")

colonnes_profil = [f"h{h:02d}" for h in range(24)]
```

> **Piege du pivot** : si vous ne specifiez pas les valeurs possibles dans
> `pivot("heure", list(range(24)))`, Spark effectue un premier passage sur
> les donnees pour les decouvrir -- ce qui double le temps de calcul.
> Specifier les valeurs explicitement est toujours preferable.

---

## 6. Apprentissage supervise : la regression par arbres boostes

### 6.1 Arbres de decision

Un arbre de decision partitionne recursivement l'espace des features
en regions, chacune associee a une valeur de prediction.

```
Est-il apres 18h ?
    ├── Non : taux moyen = 0.42
    └── Oui : Pleut-il ?
              ├── Non : taux moyen = 0.71
              └── Oui : taux moyen = 0.58
```

Chaque noeud de l'arbre choisit la feature et le seuil qui minimisent
l'impurete (variance pour la regression, entropie/Gini pour la classification).

**Avantages** : interpretables, robustes aux outliers, insensibles aux echelles.  
**Inconvenients** : fort overfitting si l'arbre est trop profond.

### 6.2 Gradient Boosting

Le Gradient Boosting construit sequentiellement un ensemble d'arbres,
chaque arbre corrigeant les erreurs du precedent.

```
Iteration 1 : arbre f1 entraine sur les residus initiaux (y - y_moy)
Iteration 2 : arbre f2 entraine sur les residus de f1
Iteration 3 : arbre f3 entraine sur les residus de f1 + f2
...
Prediction finale : F(x) = y_moy + lr*f1(x) + lr*f2(x) + ... + lr*fN(x)
```

ou `lr` est le **taux d'apprentissage** (`stepSize`), qui controle la
contribution de chaque arbre. Un `lr` faible necessite plus d'arbres
mais generalise mieux.

**Hyperparametres principaux :**

| Parametre | Effet | Valeur courante |
|-----------|-------|-----------------|
| `maxIter` | Nombre d'arbres | 50-200 |
| `maxDepth` | Profondeur max de chaque arbre | 3-8 |
| `stepSize` | Taux d'apprentissage | 0.01-0.2 |
| `subsamplingRate` | Fraction d'observations par arbre | 0.7-1.0 |
| `featureSubsetStrategy` | Fraction de features par noeud | "auto", "sqrt", "log2" |

**Regularisation** : `maxDepth` faible et `stepSize` faible reduisent
l'overfitting, au prix d'un temps d'entrainement plus long.

### 6.3 GBTRegressor en MLlib

```python
from pyspark.ml.regression import GBTRegressor

gbt = GBTRegressor(
    featuresCol="features",      # colonne vecteur d'entree
    labelCol="cible",            # colonne de la variable cible (Double)
    predictionCol="prediction",  # colonne de sortie
    maxIter=50,                  # nombre d'arbres
    maxDepth=5,                  # profondeur maximale
    stepSize=0.1,                # taux d'apprentissage
    subsamplingRate=1.0,         # fraction des donnees par arbre
    featureSubsetStrategy="auto",# features par noeud : "auto" = sqrt(n_features)
    seed=42
)

# Via un Pipeline
pipeline = Pipeline(stages=[assembler, scaler, gbt])
model    = pipeline.fit(df_train)

# Acces au modele GBT au sein du PipelineModel
gbt_model = model.stages[-1]   # le dernier stage est le GBTRegressionModel

# Importance des features (tableau numpy de taille n_features)
importances = gbt_model.featureImportances.toArray()
# Chaque valeur est la reduction moyenne d'impurete apportee par cette feature
```

### 6.4 Autres regresseurs disponibles

```python
from pyspark.ml.regression import (
    LinearRegression,
    RandomForestRegressor,
    DecisionTreeRegressor,
    GeneralizedLinearRegression,
    IsotonicRegression,
)

# LinearRegression -- avec regularisation Ridge (L2), Lasso (L1) ou ElasticNet
lr = LinearRegression(
    featuresCol="features", labelCol="cible",
    regParam=0.01,          # lambda : force de la regularisation
    elasticNetParam=0.0,    # 0.0 = Ridge, 1.0 = Lasso
    maxIter=100
)

# RandomForestRegressor -- ensemble d'arbres independants (bagging)
rf = RandomForestRegressor(
    featuresCol="features", labelCol="cible",
    numTrees=100,
    maxDepth=5,
    seed=42
)
```

---

## 7. Evaluation des modeles

### 7.1 RegressionEvaluator

```python
from pyspark.ml.evaluation import RegressionEvaluator

# L'evaluateur est configure une fois, reutilise pour chaque metrique
evaluator = RegressionEvaluator(
    labelCol="cible",
    predictionCol="prediction",
    metricName="rmse"    # voir tableau ci-dessous
)

# Application sur le DataFrame de predictions
rmse = evaluator.evaluate(df_predictions)

# Changer la metrique sans recreer l'objet
evaluator.setMetricName("r2")
r2 = evaluator.evaluate(df_predictions)
```

**Metriques disponibles :**

| `metricName` | Formule | Interpretation |
|---|---|---|
| `"rmse"` | sqrt(mean((y-y_hat)^2)) | Erreur quadratique moyenne, meme unite que y |
| `"mse"` | mean((y-y_hat)^2) | Carre du RMSE |
| `"mae"` | mean(\|y-y_hat\|) | Erreur absolue moyenne, moins sensible aux outliers |
| `"r2"` | 1 - SS_res/SS_tot | 1 = parfait, 0 = modele nul, peut etre negatif |
| `"var"` | Var(y-y_hat) | Variance des residus |

Pour notre projet (taux_occupation dans [0, 1]) :

- Un RMSE de 0.05 signifie une erreur moyenne de ±5 points de pourcentage.
- Un R² de 0.80 signifie que le modele explique 80% de la variance du taux d'occupation.

### 7.2 ClusteringEvaluator

```python
from pyspark.ml.evaluation import ClusteringEvaluator

evaluator_km = ClusteringEvaluator(
    featuresCol="features_scaled",
    predictionCol="cluster",
    metricName="silhouette",         # seule metrique disponible
    distanceMeasure="squaredEuclidean"
)
score = evaluator_km.evaluate(df_clusters)
```

### 7.3 Split train/test : l'imperatif temporel

Sur des donnees de series temporelles, **ne jamais utiliser un split aleatoire**.
Un split aleatoire melange les observations passees et futures, permettant
au modele de voir le futur pendant l'entrainement.

```python
# INCORRECT sur une serie temporelle
df_train, df_test = df.randomSplit([0.8, 0.2], seed=42)

# CORRECT : split chronologique
df_train = df.filter(col("annee") == 2022)
df_test  = df.filter(col("annee") == 2023)
```

---

## 8. Validation croisee et selection d'hyperparametres

### 8.1 CrossValidator

Le `CrossValidator` divise les donnees en `numFolds` parties, entraine
le modele sur `numFolds - 1` parties et l'evalue sur la partie restante,
en repetant pour toutes les combinaisons de parties et d'hyperparametres.

```
ParamGrid : {maxDepth: [3,5], maxIter: [30,50]} -> 4 combinaisons
numFolds  : 3

Total des entrainements : 4 combinaisons x 3 folds = 12 entrainements
```

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Construction de la grille d'hyperparametres
# addGrid(parametre, liste_de_valeurs)
param_grid = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth,  [3, 5, 7])
    .addGrid(gbt.maxIter,   [30, 50])
    .addGrid(gbt.stepSize,  [0.05, 0.1])
    .build()
    # -> 3 x 2 x 2 = 12 combinaisons
)

cv = CrossValidator(
    estimator=pipeline,         # le Pipeline complet (pas seulement le GBT)
    estimatorParamMaps=param_grid,
    evaluator=evaluator_rmse,   # minimise cette metrique (RMSE)
    numFolds=3,
    seed=42,
    parallelism=2               # evaluations paralleles (limiter selon la RAM)
)

cv_model = cv.fit(df_train)

# Metriques moyennes par combinaison (meme ordre que param_grid)
print(cv_model.avgMetrics)

# Meilleur modele (PipelineModel avec les meilleurs hyperparametres)
best_model = cv_model.bestModel

# Acces aux meilleurs hyperparametres
best_gbt = best_model.stages[-1]
print(f"Meilleur maxDepth : {best_gbt.getMaxDepth()}")
print(f"Meilleur maxIter  : {best_gbt.getMaxIter()}")
```

> **`parallelism`** : avec `parallelism=2`, deux combinaisons sont evaluees
> en meme temps. En mode local, limitez a 2 pour eviter la contention memoire.
> Sur un cluster, vous pouvez augmenter jusqu'au nombre de noeuds disponibles.

### 8.2 TrainValidationSplit (alternative plus rapide)

Le `CrossValidator` est couteux (k entrainements par combinaison).
Le `TrainValidationSplit` n'entraine qu'une seule fois par combinaison,
en reservant une fraction des donnees pour la validation.

```python
from pyspark.ml.tuning import TrainValidationSplit

tvs = TrainValidationSplit(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator_rmse,
    trainRatio=0.8,    # 80% pour l'entrainement, 20% pour la validation
    seed=42,
    parallelism=2
)

tvs_model = tvs.fit(df_train)
best_model = tvs_model.bestModel
```

| | CrossValidator | TrainValidationSplit |
|---|---|---|
| Entrainements par combinaison | k | 1 |
| Fiabilite de l'estimation | Elevee | Plus faible |
| Cout | k x plus cher | 1x |
| Recommande pour | Petits jeux de donnees | Grands jeux de donnees |

### 8.3 Construire une grille avec ParamGridBuilder

```python
# Grille vide (un seul entrainement -- utile pour tester le pipeline)
grille_vide = ParamGridBuilder().build()   # -> [{}]

# Grille avec un seul parametre
grille_simple = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth, [3, 5, 7])
    .build()
)   # -> [{maxDepth: 3}, {maxDepth: 5}, {maxDepth: 7}]

# Grille multi-parametres : produit cartesien
grille_complete = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth, [3, 5])
    .addGrid(gbt.stepSize, [0.05, 0.1])
    .build()
)
# -> 4 combinaisons :
#    [{maxDepth:3, stepSize:0.05}, {maxDepth:3, stepSize:0.1},
#     {maxDepth:5, stepSize:0.05}, {maxDepth:5, stepSize:0.1}]
```

---

## 9. Persistance des modeles

### 9.1 Sauvegarde et chargement natifs Spark

```python
from pyspark.ml import PipelineModel

# Sauvegarde (ecrit dans un repertoire, pas un fichier unique)
model.write().overwrite().save("/chemin/vers/mon_modele")

# Structure du repertoire sauvegarde :
# mon_modele/
#   metadata/    -> JSON decrivant les stages
#   stages/      -> un sous-repertoire par stage
#     0_VectorAssembler.../
#     1_StandardScalerModel.../
#     2_GBTRegressionModel.../

# Chargement
model_recharge = PipelineModel.load("/chemin/vers/mon_modele")

# Application
df_pred = model_recharge.transform(df_nouveau)
```

### 9.2 MLflow : tracking et registre de modeles

MLflow offre deux services distincts :

- **Tracking** : enregistre les runs (parametres, metriques, artefacts)
  pour les comparer et reproduire les resultats.
- **Model Registry** : versionne et promouvoit les modeles
  (Staging -> Production -> Archived).

```python
import mlflow
import mlflow.spark

# Configuration du serveur de tracking
# En local : fichier URI pointant vers un repertoire
mlflow.set_tracking_uri("file:///chemin/vers/mlruns")
# En Docker (projet ClimaCity) : le service mlflow du reseau interne
mlflow.set_tracking_uri("http://mlflow:5000")

# Creation ou recuperation d'une experience
mlflow.set_experiment("ClimaCity-GBT")

# Enregistrement d'un run
with mlflow.start_run(run_name="gbt-depth5-iter50"):

    # Parametres (hyperparametres, choix de features...)
    mlflow.log_params({
        "max_depth"  : 5,
        "max_iter"   : 50,
        "step_size"  : 0.1,
        "n_features" : 15,
    })

    # Metriques (resultats d'evaluation)
    mlflow.log_metrics({
        "rmse" : 0.0823,
        "mae"  : 0.0612,
        "r2"   : 0.7841,
    })

    # Modele Spark (sauvegarde + enregistrement dans MLflow)
    mlflow.spark.log_model(model, artifact_path="pipeline_gbt")

    # Artefacts arbitraires (graphiques, CSV de resultats...)
    mlflow.log_artifact("carte_clusters.html")
```

```python
# Recherche et chargement du meilleur run
from mlflow.tracking import MlflowClient

client  = MlflowClient()
exp     = client.get_experiment_by_name("ClimaCity-GBT")
runs    = client.search_runs(
    experiment_ids=[exp.experiment_id],
    order_by=["metrics.rmse ASC"],   # trier par RMSE croissant
    max_results=1
)
best_run_id = runs[0].info.run_id

# Chargement du modele depuis MLflow
model_uri      = f"runs:/{best_run_id}/pipeline_gbt"
model_recharge = mlflow.spark.load_model(model_uri)
```

---

## 10. Memento de l'API

### 10.1 Imports essentiels

```python
# Session Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Abstraction Pipeline
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel            # pour le chargement

# Features
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    MinMaxScaler,
    MaxAbsScaler,
    RobustScaler,
    StringIndexer,
    OneHotEncoder,
    Binarizer,
    Bucketizer,
    SQLTransformer,
    PCA,
)

# Clustering
from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture

# Regression
from pyspark.ml.regression import (
    GBTRegressor,
    RandomForestRegressor,
    LinearRegression,
    DecisionTreeRegressor,
    GeneralizedLinearRegression,
)

# Classification
from pyspark.ml.classification import (
    GBTClassifier,
    RandomForestClassifier,
    LogisticRegression,
    DecisionTreeClassifier,
)

# Evaluation
from pyspark.ml.evaluation import (
    RegressionEvaluator,
    ClusteringEvaluator,
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)

# Tuning
from pyspark.ml.tuning import (
    CrossValidator,
    TrainValidationSplit,
    ParamGridBuilder,
)

# MLflow
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
```

### 10.2 VectorAssembler -- reference rapide

```python
VectorAssembler(
    inputCols  = [...],          # liste des colonnes numeriques
    outputCol  = "features",     # nom de la colonne vecteur
    handleInvalid = "skip"       # "error" | "skip" | "keep"
)
# Methodes :
assembler.transform(df)          # -> DataFrame avec colonne outputCol ajoutee
assembler.getInputCols()         # -> liste des colonnes d'entree
```

### 10.3 StandardScaler -- reference rapide

```python
StandardScaler(
    inputCol  = "features_brutes",
    outputCol = "features",
    withMean  = True,            # centre (produit vecteurs denses)
    withStd   = True             # reduit
)
# Methodes :
scaler_model = scaler.fit(df)   # -> StandardScalerModel
scaler_model.mean                # vecteur numpy des moyennes
scaler_model.std                 # vecteur numpy des ecarts-types
scaler_model.transform(df)       # -> DataFrame normalise
```

### 10.4 KMeans -- reference rapide

```python
KMeans(
    featuresCol     = "features_scaled",
    predictionCol   = "cluster",
    k               = 4,
    maxIter         = 100,
    tol             = 1e-4,
    seed            = 42,
    initMode        = "k-means||"  # "random" | "k-means||"
)
# Methodes :
km_model = kmeans.fit(df)             # -> KMeansModel
km_model.transform(df)                # -> DataFrame avec colonne cluster
km_model.clusterCenters()             # -> liste de numpy arrays
km_model.summary.trainingCost         # inertie (WSSSE)
km_model.summary.k                    # nombre de clusters
km_model.summary.clusterSizes         # liste : nb de points par cluster
```

### 10.5 GBTRegressor -- reference rapide

```python
GBTRegressor(
    featuresCol          = "features",
    labelCol             = "cible",
    predictionCol        = "prediction",
    maxIter              = 50,          # nombre d'arbres
    maxDepth             = 5,           # profondeur max par arbre
    stepSize             = 0.1,         # taux d'apprentissage
    subsamplingRate      = 1.0,         # fraction observations par arbre
    featureSubsetStrategy= "auto",      # features par noeud
    maxBins              = 32,          # bins pour les features continues
    seed                 = 42
)
# Methodes :
gbt_model = gbt.fit(df)               # -> GBTRegressionModel
gbt_model.transform(df)               # -> DataFrame avec colonne prediction
gbt_model.featureImportances          # SparseVector des importances
gbt_model.featureImportances.toArray()# -> numpy array
gbt_model.numTrees                    # nombre d'arbres effectivement construits
gbt_model.trees                       # liste des DecisionTreeRegressionModels
gbt_model.treeWeights                 # poids de chaque arbre
```

### 10.6 RegressionEvaluator -- reference rapide

```python
RegressionEvaluator(
    labelCol      = "cible",
    predictionCol = "prediction",
    metricName    = "rmse"         # "rmse"|"mse"|"mae"|"r2"|"var"
)
# Methodes :
evaluator.evaluate(df_pred)        # -> float
evaluator.setMetricName("r2")      # modifier la metrique
evaluator.isLargerBetter()         # True pour r2, False pour rmse/mse/mae
```

### 10.7 CrossValidator -- reference rapide

```python
CrossValidator(
    estimator          = pipeline,
    estimatorParamMaps = param_grid,   # sortie de ParamGridBuilder
    evaluator          = evaluator,
    numFolds           = 3,
    seed               = 42,
    parallelism        = 2             # combinaisons evaluees en parallele
)
# Methodes :
cv_model = cv.fit(df_train)           # -> CrossValidatorModel
cv_model.bestModel                    # PipelineModel avec les meilleurs params
cv_model.avgMetrics                   # liste des metriques moyennes par combinaison
cv_model.getEstimatorParamMaps()      # grille d'hyperparametres
```

### 10.8 Pipeline -- reference rapide

```python
Pipeline(stages=[stage1, stage2, stage3])
# stages : liste ordonnee d'Estimators et/ou de Transformers

# Methodes :
pipeline_model = pipeline.fit(df)           # -> PipelineModel
pipeline_model.transform(df)                # -> DataFrame transforme
pipeline_model.stages                       # liste des stages ajustes
pipeline_model.stages[-1]                   # dernier stage (ex: GBTRegressionModel)

# Sauvegarde / chargement
pipeline_model.write().overwrite().save(chemin)
PipelineModel.load(chemin)                  # chargement
```

### 10.9 MLflow -- reference rapide

```python
mlflow.set_tracking_uri(uri)              # configurer le serveur
mlflow.set_experiment(nom)               # creer/recuperer une experience

with mlflow.start_run(run_name=nom):
    mlflow.log_param(cle, valeur)        # un seul parametre
    mlflow.log_params(dictionnaire)      # plusieurs parametres
    mlflow.log_metric(cle, valeur)       # une seule metrique
    mlflow.log_metrics(dictionnaire)     # plusieurs metriques
    mlflow.log_artifact(chemin_fichier)  # fichier arbitraire
    mlflow.spark.log_model(model, path)  # modele Spark

# Recherche de runs
client = MlflowClient()
exp    = client.get_experiment_by_name(nom)
runs   = client.search_runs(
    experiment_ids = [exp.experiment_id],
    order_by       = ["metrics.rmse ASC"],
    max_results    = 5
)
run_id = runs[0].info.run_id

# Chargement d'un modele
model = mlflow.spark.load_model(f"runs:/{run_id}/chemin_artefact")
```

---

## 11. Bonnes pratiques et pieges courants

### Fit uniquement sur le train, transform sur tout

```python
# CORRECT
scaler_model = scaler.fit(df_train)
df_train_sc  = scaler_model.transform(df_train)
df_test_sc   = scaler_model.transform(df_test)

# INCORRECT : data leakage
scaler_model = scaler.fit(df_test)    # le scaler "voit" les donnees de test
```

Le `Pipeline` gere cela automatiquement : ne l'appelez qu'avec `df_train`.

### Specifier `handleInvalid` dans VectorAssembler

Sans `handleInvalid="skip"`, un seul null dans les features leve une
exception sur tout le DataFrame. En production, preferez `"skip"` et
loggez le nombre de lignes perdues.

```python
n_avant = df.count()
df_feat = assembler.transform(df)
n_apres = df_feat.count()
print(f"Lignes exclues pour null : {n_avant - n_apres:,}")
```

### Ne pas appeler count() entre chaque etape

Chaque `count()` relance le calcul depuis le debut (ou depuis le cache).
Dans un pipeline de feature engineering avec de nombreuses `withColumn()`,
accumulez les transformations et n'appelez `count()` qu'a la fin.

```python
# LENT : relit les donnees a chaque count()
df1 = df.withColumn("a", ...);  df1.count()
df2 = df1.withColumn("b", ...); df2.count()

# RAPIDE : accumule les transformations, une seule action a la fin
df_final = (
    df
    .withColumn("a", ...)
    .withColumn("b", ...)
    .withColumn("c", ...)
    .dropna(subset=FEATURES)
)
n = df_final.count()
```

### Cacher les splits avant la validation croisee

Le `CrossValidator` accede aux donnees plusieurs fois (k fois par
combinaison). Sans cache, les donnees sont relues depuis le disque
a chaque iteration.

```python
df_train.cache()
df_train.count()   # force la mise en cache
cv_model = cv.fit(df_train)
df_train.unpersist()   # libere apres usage
```

### Fixer le seed partout

Les algorithmes stochastiques (K-Means, GBT avec `subsamplingRate < 1`,
split aleatoire) produisent des resultats differents a chaque execution
sans seed fixe. Fixez-le a **tous** les niveaux :

```python
SEED = 42

kmeans = KMeans(k=4, seed=SEED)
gbt    = GBTRegressor(seed=SEED)
cv     = CrossValidator(seed=SEED)
df_sample = df.sample(fraction=0.3, seed=SEED)
df_train, df_test = df.randomSplit([0.8, 0.2], seed=SEED)
```

### Lire l'importance des features avant d'optimiser

Avant de lancer une validation croisee couteuse, examinez l'importance
des features sur un premier entrainement. Si certaines features ont une
importance nulle ou quasi nulle, supprimez-les : cela reduit la dimension
du probleme et peut ameliorer les performances.

```python
importances = gbt_model.featureImportances.toArray()
df_imp = pd.DataFrame({"feature": FEATURES, "importance": importances})
print(df_imp.sort_values("importance").head(5))
# Si les 5 dernieres features ont importance < 0.001, considerez les supprimer
```

### Ne pas confondre PipelineModel et Pipeline

`Pipeline` est l'`Estimator` (avant `fit()`).  
`PipelineModel` est le `Transformer` (apres `fit()`).  
Seul le `PipelineModel` peut etre sauvegarde et utilise pour des predictions.

```python
type(pipeline)        # pyspark.ml.Pipeline
type(pipeline_model)  # pyspark.ml.PipelineModel

pipeline.transform(df)        # ERREUR : Pipeline n'a pas de transform()
pipeline_model.transform(df)  # OK
```
