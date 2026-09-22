#!/usr/bin/env python3
"""
Simulateur de flux Vélib -- Session 4 (Structured Streaming)

Lit la table consolidée (parquet) et rejoue les snapshots historiques
sous forme de fichiers JSON dans un répertoire surveillé par Spark.

Usage:
    python scripts/simulateur_flux.py
    python scripts/simulateur_flux.py --output data/output/stream_input --vitesse 3

Arguments:
    --output    Répertoire de destination (défaut : data/output/stream_input)
    --source    Chemin du parquet source  (défaut : data/output/disponibilite_consolidee.parquet)
    --vitesse   Minutes de données historiques par seconde réelle (défaut : 2)
"""

import argparse
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq


def generer_station_id(nom_station: str) -> int:
    """Hash positif sur 6 chiffres dérivé du nom de la station."""
    hash_bytes = hashlib.md5(nom_station.encode()).digest()
    return int.from_bytes(hash_bytes[:4], "big") % 900_000 + 100_000


def normaliser_horodatage(horodatage_str: str) -> str:
    """Convertit '2020-12-08T00:20Z' en '2020-12-08T00:20:00Z' (secondes requises par Spark)."""
    if horodatage_str.endswith("Z") and "T" in horodatage_str:
        partie_heure = horodatage_str[:-1].split("T")[1]
        if len(partie_heure) == 5:  # HH:mm sans secondes
            horodatage_str = horodatage_str[:-1] + ":00Z"
    return horodatage_str


def charger_et_grouper(chemin_source: str) -> dict[str, list[dict]]:
    """Lit le parquet et regroupe les lignes par horodatage."""
    colonnes_utiles = [
        "nom_station", "code_arr", "capacite",
        "velos_disponibles", "bornettes_libres", "horodatage",
    ]
    dataset = pq.read_table(chemin_source, columns=colonnes_utiles)

    groupes: dict[str, list[dict]] = {}
    for batch in dataset.to_batches(max_chunksize=10_000):
        batch_dict = batch.to_pydict()
        nb_lignes = len(batch_dict["nom_station"])

        for idx in range(nb_lignes):
            horodatage = normaliser_horodatage(batch_dict["horodatage"][idx])
            nom_station = batch_dict["nom_station"][idx]
            velos_disponibles = batch_dict["velos_disponibles"][idx] or 0
            velos_meca = int(velos_disponibles * 0.6)
            velos_elec = velos_disponibles - velos_meca

            enregistrement = {
                "station_id":      generer_station_id(nom_station),
                "nom_station":     nom_station,
                "code_arr":        batch_dict["code_arr"][idx],
                "capacite":        batch_dict["capacite"][idx],
                "velos_meca":      velos_meca,
                "velos_elec":      velos_elec,
                "bornettes_libres": batch_dict["bornettes_libres"][idx] or 0,
                "horodatage":      horodatage,
            }
            if horodatage not in groupes:
                groupes[horodatage] = []
            groupes[horodatage].append(enregistrement)

    return groupes


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulateur flux Vélib")
    parser.add_argument("--output",   default="data/output/stream_input",
                        help="Répertoire de destination JSON")
    parser.add_argument("--source",   default="data/output/disponibilite_consolidee.parquet",
                        help="Parquet source")
    parser.add_argument("--vitesse",  type=int, default=2,
                        help="Minutes de données historiques par seconde réelle")
    args = parser.parse_args()

    repertoire_sortie = Path(args.output)
    repertoire_sortie.mkdir(parents=True, exist_ok=True)
    fichier_log = repertoire_sortie.parent / "simulateur.log"

    print(f"Lecture du parquet : {args.source}")
    groupes = charger_et_grouper(args.source)
    horodatages_tries = sorted(groupes.keys())

    nb_snapshots = len(horodatages_tries)
    nb_lignes_total = sum(len(v) for v in groupes.values())
    print(f"{nb_snapshots} snapshots trouvés -- {nb_lignes_total:,} lignes totales")
    print(f"Démarrage -- vitesse x{args.vitesse} ({args.vitesse} min/s réelle)")
    print(f"Sortie : {repertoire_sortie}")
    print("Ctrl-C pour arrêter\n")

    intervalle_sec = 1.0 / args.vitesse

    for idx, horodatage in enumerate(horodatages_tries):
        lignes = groupes[horodatage]
        nom_fichier = repertoire_sortie / f"batch_{idx:06d}.json"

        with open(nom_fichier, "w", encoding="utf-8") as fichier_json:
            for enregistrement in lignes:
                fichier_json.write(json.dumps(enregistrement) + "\n")

        ts_log = datetime.now().strftime("%H:%M:%S")
        message = f"[{ts_log}] batch {idx:06d} -- {horodatage} -- {len(lignes)} stations"
        print(message)
        with open(fichier_log, "a", encoding="utf-8") as flog:
            flog.write(message + "\n")

        time.sleep(intervalle_sec)

    print(f"\nSimulateur terminé. {nb_snapshots} fichiers écrits dans {repertoire_sortie}")


if __name__ == "__main__":
    main()
