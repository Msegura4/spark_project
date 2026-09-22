# Bugs & Choix techniques — Session 4 (Structured Streaming)

---

## Bug 1 — Script simulateur introuvable

### Symptôme

Le notebook Session 4 fait référence au script `scripts/simulateur_flux.py` dès
la section 2.2 (markdown) et dans la cellule de vérification `compter_fichiers_stream`.
Le script était introuvble sur Hetic Learn

Sans ce script, il est impossible de produire les fichiers JSON qui alimentent
le `readStream` de Spark. Toutes les cellules de streaming auraient bloqué
indéfiniment sur un répertoire vide.

### Solution — création de `scripts/simulateur_flux.py`

Le script a été créé de zéro. Il remplit le rôle décrit dans le notebook :
rejouer les données historiques du parquet en les écrivant sous forme de
fichiers JSON dans un répertoire surveillé par Spark (file source).

**Ce que fait le script :**

1. Lit `data/output/disponibilite_consolidee.parquet` avec PyArrow
   (sans démarrer de SparkSession, pour rester léger).
2. Regroupe les lignes par valeur de `horodatage` (un fichier = un snapshot temporel).
3. Génère un `station_id` entier positif à 6 chiffres via un hash MD5 du `nom_station`.
   Le parquet ne contient pas de `station_id` numérique — seul `nom_station` est disponible.
4. Dérive `velos_meca` et `velos_elec` depuis `velos_disponibles` :
   - `velos_meca = int(velos_disponibles * 0.6)`
   - `velos_elec = velos_disponibles - velos_meca`
   Ces deux colonnes sont requises par le `schema_flux` du notebook mais absentes du parquet.
5. Normalise le format de `horodatage` : le parquet stocke `"2020-12-08T00:20Z"`
   (sans secondes), que Spark ne parse pas en `TimestampType`. Le script le convertit
   en `"2020-12-08T00:20:00Z"` (avec `:00` ajouté).
6. Écrit un fichier `batch_XXXXXX.json` (une ligne JSON par station) dans le répertoire
   de sortie, avec une pause entre fichiers contrôlée par `--vitesse`.

**Usage :**
```bash
# Lancer dans un terminal séparé AVANT d'exécuter les cellules du notebook
python scripts/simulateur_flux.py --output data/output/stream_input --vitesse 3
# --vitesse 3 = 3 minutes de données historiques par seconde réelle
```

### Points d'attention relevés

| Point | Détail |
|-------|--------|
| `horodatage` dans le parquet | Chaîne `"2020-12-08T00:20Z"` — année 2020 (données originales Vélib), alors que les partitions `annee=` reflètent le décalage +2 ans du Jour 1. Le simulateur rejoue les timestamps tels quels. |
| `station_id` | Généré par hash MD5 — reproductible (le même nom donne toujours le même id) mais non lié à un référentiel officiel. |
| `velos_meca` / `velos_elec` | Répartition 60/40 arbitraire — valeur pédagogique, pas réaliste. |
| Taille du flux | Le parquet contient plusieurs millions de lignes. Avec `--vitesse 2`, le simulateur tourne plusieurs heures. Pour le cours, `--vitesse 10` ou plus est recommandé. |

---

## Choix technique — watermark sur les requêtes fenêtrées

Le notebook prévoit d'écrire les fenêtres glissantes en mode `append` vers Delta Lake.
En Structured Streaming, le mode `append` sur une agrégation fenêtrée exige un watermark :
sans lui, Spark ne sait jamais quand une fenêtre est définitivement fermée et refuse
de valider le plan d'exécution.

Watermark ajouté : `.withWatermark("horodatage", "5 minutes")`

Conséquence : Spark n'émet le résultat d'une fenêtre que lorsque le watermark
dépasse la fin de cette fenêtre. Avec une fenêtre de 10 minutes et un watermark
de 5 minutes, les premiers résultats n'apparaissent qu'après 15 minutes de données
ingérées à garder en tête si le répertoire Delta semble vide au début.

---

## Choix technique — état Python dans `foreachBatch`

La logique de détection des ruptures prolongées utilise un dictionnaire Python
`etat_ruptures` (variable globale) pour mémoriser les stations actuellement vides
d'un batch à l'autre.

**Limite importante :** cet état Python n'est **pas sauvegardé dans le checkpoint Spark**.
En cas de redémarrage du kernel ou de la requête, `etat_ruptures` repart vide et les
ruptures en cours sont perdues.
