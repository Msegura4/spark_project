# ClimaCity Paris — Spark DIA3

Projet pédagogique d'analyse de données Vélib' et météo avec Apache Spark et PySpark.

---

## Prérequis

- Python 3.9+
- Java 17 (requis par PySpark)

---

## 1. Installer Java

### Mac (Homebrew)
```bash
brew install openjdk@17
echo 'export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Vérifier l'installation
```bash
java -version
```

> Note : Java est aussi configuré directement dans la première cellule du notebook via `os.environ`, donc même si votre terminal ne le voit pas, Jupyter le trouvera.

---

## 2. Installer les dépendances Python

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## 3. Récupérer les données

```bash
python download_data.py
```

Ce script crée le dossier `data/` et télécharge :
- `data/velib/stations_info.csv` — infos des stations Vélib' (API GBFS)
- `data/meteo/paris_montsouris_horaire.csv` — météo horaire 2022-2023 (Open-Meteo)

> **`historique_stations.csv`** (376 MB) n'est pas versionné.
> Demander le fichier à l'auteur du repo et le placer dans `fichiers_cours/data_exercice/`.

---

## 4. Lancer le notebook

```bash
source venv/bin/activate
jupyter notebook Spark_DIA3_Session_1.ipynb
```

Exécuter les cellules dans l'ordre depuis le début (**Kernel > Restart & Run All**).

---

## Structure du projet

```
spark_project/
├── Spark_DIA3_Session_1.ipynb   # notebook principal
├── download_data.py              # script de téléchargement des données
├── requirements.txt              # dépendances Python
├── fichiers_cours/
│   ├── cours/                   # PDFs de cours
│   ├── fiches_résumés/          # fiches récapitulatives
│   └── data_exercice/           # données manuelles (non versionnées)
└── data/                        # données générées (non versionné)
```
