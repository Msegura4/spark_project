"""
Téléchargement des données du projet ClimaCity Paris.

Fichiers téléchargeables :
  - stations_info.csv     : infos statiques des stations Vélib' (API GBFS)
  - paris_montsouris_horaire.csv : données météo horaires 2022-2023 (Open-Meteo)

Fichier fourni manuellement :
  - historique_stations.csv : historique de disponibilité Vélib' (5,3M lignes)
"""

import requests
import pandas as pd
from pathlib import Path

DATA_DIR     = Path("data")
STATIONS_CSV = DATA_DIR / "velib" / "stations_info.csv"
METEO_CSV    = DATA_DIR / "meteo" / "paris_montsouris_horaire.csv"

STATIONS_CSV.parent.mkdir(parents=True, exist_ok=True)
METEO_CSV.parent.mkdir(parents=True, exist_ok=True)


def download_stations():
    """Télécharge les infos statiques des stations Vélib' depuis l'API GBFS."""
    url = (
        "https://velib-metropole-opendata.smovengo.cloud"
        "/opendata/Velib_Metropole/station_information.json"
    )
    print("Téléchargement des stations Vélib'...")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()

    stations = resp.json()["data"]["stations"]
    df = pd.DataFrame(stations)[["station_id", "name", "lat", "lon", "capacity", "stationCode"]]
    df["station_id"] = df["station_id"].astype(int)
    df["code_arr"] = df["station_id"] // 1000

    df.to_csv(STATIONS_CSV, index=False, sep=";")
    print(f"  {len(df)} stations sauvegardées → {STATIONS_CSV}")


def download_meteo(date_debut="2020-11-01", date_fin="2021-02-28"):
    """Télécharge les données météo horaires Paris-Montsouris via Open-Meteo."""
    params = {
        "latitude"       : 48.8214,
        "longitude"      : 2.3378,
        "start_date"     : date_debut,
        "end_date"       : date_fin,
        "hourly"         : "temperature_2m,precipitation,windspeed_10m,relativehumidity_2m,surface_pressure",
        "timezone"       : "Europe/Paris",
        "wind_speed_unit": "ms",
    }
    print(f"Téléchargement météo Paris-Montsouris ({date_debut} → {date_fin})...")
    resp = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params, timeout=60)
    resp.raise_for_status()

    df = pd.DataFrame(resp.json()["hourly"])
    df.rename(columns={
        "time"               : "horodatage",
        "temperature_2m"     : "temperature",
        "precipitation"      : "precipitations",
        "windspeed_10m"      : "vent_ms",
        "relativehumidity_2m": "humidite",
        "surface_pressure"   : "pression",
    }, inplace=True)

    df.to_csv(METEO_CSV, index=False, sep=";")
    print(f"  {len(df):,} lignes sauvegardées → {METEO_CSV}")


if __name__ == "__main__":
    download_stations()
    download_meteo()
    print("\nTéléchargements terminés.")
