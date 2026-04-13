from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

#  Constantes   ──
DVF_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/2023/full.csv.gz"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

#  Configuration du DAG   
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : téléchargement -> HDFS raw -> PostgreSQL curated",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():

    @task(task_id="verifier_sources")
    def verifier_sources():
        result = {}

        try:
            response = requests.get(DVF_URL, timeout=10)
            result["api_dvf"] = response.status_code == 200
        except Exception:
            result["api_dvf"] = False

        try:
            url = f"{WEBHDFS_BASE_URL}/?op=GETHOMEDIRECTORY&user.name={WEBHDFS_USER}"
            response = requests.get(url, timeout=10)
            result["hdfs"] = response.status_code == 200
        except Exception:
            result["hdfs"] = False

        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            conn.cursor().execute("SELECT 1;")
            result["postgres"] = True
        except Exception:
            result["postgres"] = False

        if not all(result.values()):
            raise Exception(f"Erreur sources: {result}")

        return result
    
    @task(task_id="telecharger_dvf")
    def telecharger_dvf() -> str:
        """
        Télécharge le fichier DVF et le stocke temporairement.
        Retourne le chemin local du fichier.
        """

        logger.info("Début téléchargement DVF...")

        response = requests.get(DVF_URL, stream=True)

        if response.status_code != 200:
            raise Exception(f"Erreur téléchargement DVF: {response.status_code}")

        # Création d'un fichier temporaire
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz")
        
        with open(tmp_file.name, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        logger.info(f"Fichier téléchargé: {tmp_file.name}")

        return tmp_file.name
    
    @task(task_id="upload_hdfs")
    def upload_hdfs(local_file_path: str) -> str:
        """
        Upload le fichier DVF vers HDFS.
        """

        from helpers.webhdfs import WebHDFSClient

        client = WebHDFSClient()

        # Créer le dossier si besoin
        client.mkdirs(HDFS_RAW_PATH)

        filename = os.path.basename(local_file_path)
        hdfs_path = f"{HDFS_RAW_PATH}/{filename}"

        logger.info(f"Upload vers HDFS: {hdfs_path}")

        client.upload(hdfs_path, local_file_path)

        return hdfs_path

    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        import io

        logger.info(f"Lecture du fichier depuis HDFS: {hdfs_path}")

        #  1. Lire depuis HDFS   
        url = f"{WEBHDFS_BASE_URL}{hdfs_path}?op=OPEN&user.name={WEBHDFS_USER}"
        response = requests.get(url, stream=True)

        if response.status_code != 200:
            raise Exception("Erreur lecture HDFS")

        chunks = pd.read_csv(
        response.raw,
        compression="gzip",
        chunksize=50000,
        low_memory=False
        )

        df_list = []

        for chunk in chunks:
            chunk.columns = [col.lower().replace(" ", "_") for col in chunk.columns]

            chunk = chunk[
                (chunk["type_local"] == "Appartement") &
                (chunk["nature_mutation"] == "Vente") &
                (chunk["code_postal"].astype(str).str.startswith("75")) &
                (chunk["surface_reelle_bati"] > 9) &
                (chunk["surface_reelle_bati"] < 500) &
                (chunk["valeur_fonciere"] > 10000)
            ]

            chunk["code_postal"] = chunk["code_postal"].astype(str).str.replace(".0", "", regex=False)
            chunk = chunk[chunk["code_postal"].str.match(r"^75\d{3}$")]

            chunk["prix_m2"] = chunk["valeur_fonciere"] / chunk["surface_reelle_bati"]
            chunk["arrondissement"] = chunk["code_postal"].str[-2:].astype(int)

            df_list.append(chunk)

        df = pd.concat(df_list)

        logger.info(f"Nombre de lignes initial: {len(df)}")

        #  2. Nettoyer colonnes 
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]

        #  3. Filtrer   
        df = df[
            (df["type_local"] == "Appartement") &
            (df["nature_mutation"] == "Vente") &
            (df["code_postal"].astype(str).str.startswith("75")) &
            (df["surface_reelle_bati"] > 9) &
            (df["surface_reelle_bati"] < 500) &
            (df["valeur_fonciere"] > 10000)
        ]

        logger.info(f"Après filtrage: {len(df)} lignes")

        #  4. Prix au m²  ──
        df = df[df["surface_reelle_bati"] > 0]
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

        #  5. Arrondissement   
        df["code_postal"] = df["code_postal"].astype(str).str.replace(".0", "", regex=False)

        df = df[df["code_postal"].str.match(r"^75\d{3}$")]

        df["arrondissement"] = df["code_postal"].str[-2:].astype(int)

        #  6. Agrégations  
        grouped = df.groupby("code_postal").agg({
            "prix_m2": ["mean", "median", "min", "max"],
            "surface_reelle_bati": "mean",
            "id_mutation": "count"
        })

        grouped.columns = [
            "prix_m2_moyen",
            "prix_m2_median",
            "prix_m2_min",
            "prix_m2_max",
            "surface_moyenne",
            "nb_transactions"
        ]

        grouped = grouped.reset_index()

        agregats = grouped.to_dict(orient="records")

        # 7. Stats globales 
        stats_globales = {
            "nb_transactions_total": int(len(df)),
            "prix_m2_moyen_paris": float(df["prix_m2"].mean()),
            "prix_m2_median_paris": float(df["prix_m2"].median()),
            "arrdt_plus_cher": int(df.groupby("arrondissement")["prix_m2"].mean().idxmax()),
            "arrdt_moins_cher": int(df.groupby("arrondissement")["prix_m2"].mean().idxmin()),
            "surface_mediane": float(df["surface_reelle_bati"].median())
        }

        logger.info("Traitement terminé")

        return {
            "agregats": agregats,
            "stats_globales": stats_globales
        }

    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        """
        Insère les données agrégées dans PostgreSQL (zone curated).
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        agregats = resultats.get("agregats", [])
        stats_globales = resultats.get("stats_globales", {})

        now = datetime.now()
        annee = now.year
        mois = now.month

        # arrondissements 
        upsert_query = """
        INSERT INTO prix_m2_arrondissement (
            code_postal, arrondissement, annee, mois,
            prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max,
            nb_transactions, surface_moyenne, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (code_postal, annee, mois)
        DO UPDATE SET
            prix_m2_moyen = EXCLUDED.prix_m2_moyen,
            prix_m2_median = EXCLUDED.prix_m2_median,
            prix_m2_min = EXCLUDED.prix_m2_min,
            prix_m2_max = EXCLUDED.prix_m2_max,
            nb_transactions = EXCLUDED.nb_transactions,
            surface_moyenne = EXCLUDED.surface_moyenne,
            updated_at = NOW();
        """

        count = 0

        for row in agregats:
            hook.run(
                upsert_query,
                parameters=(
                    row["code_postal"],
                    int(row["code_postal"][-2:]),
                    annee,
                    mois,
                    float(row["prix_m2_moyen"]),
                    float(row["prix_m2_median"]),
                    float(row["prix_m2_min"]),
                    float(row["prix_m2_max"]),
                    int(row["nb_transactions"]),
                    float(row["surface_moyenne"]),
                )
            )
            count += 1

        # stats globales 
        upsert_stats_query = """
        INSERT INTO stats_marche (
            annee, mois,
            nb_transactions_total,
            prix_m2_median_paris,
            prix_m2_moyen_paris,
            arrdt_plus_cher,
            arrdt_moins_cher,
            surface_mediane,
            date_calcul
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (annee, mois)
        DO UPDATE SET
            nb_transactions_total = EXCLUDED.nb_transactions_total,
            prix_m2_median_paris = EXCLUDED.prix_m2_median_paris,
            prix_m2_moyen_paris = EXCLUDED.prix_m2_moyen_paris,
            arrdt_plus_cher = EXCLUDED.arrdt_plus_cher,
            arrdt_moins_cher = EXCLUDED.arrdt_moins_cher,
            surface_mediane = EXCLUDED.surface_mediane,
            date_calcul = NOW();
        """

        hook.run(
            upsert_stats_query,
            parameters=(
                annee,
                mois,
                int(stats_globales["nb_transactions_total"]),
                float(stats_globales["prix_m2_median_paris"]),
                float(stats_globales["prix_m2_moyen_paris"]),
                int(stats_globales["arrdt_plus_cher"]),
                int(stats_globales["arrdt_moins_cher"]),
                float(stats_globales["surface_mediane"]),
            )
        )

        logger.info(f"{count} lignes insérées/mises à jour")

        return count    

    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        """
        Génère un rapport des arrondissements classés par prix médian.
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        now = datetime.now()
        annee = now.year
        mois = now.month

        # Requête SQL
        query = """
        SELECT 
            arrondissement,
            prix_m2_median,
            prix_m2_moyen,
            nb_transactions,
            surface_moyenne
        FROM prix_m2_arrondissement
        WHERE annee = %s AND mois = %s
        ORDER BY prix_m2_median DESC
        LIMIT 20;
        """

        results = hook.get_records(query, parameters=(annee, mois))

        # plus de style
        report = "\n Classement des arrondissements (prix médian au m²)\n"
        report += "------------------------------------------------------------\n"
        report += "Arrdt | Median €/m² | Moyen €/m² | Transactions | Surface\n"
        report += "------|-------------|-------------|--------------|--------\n"

        for row in results:
            arrondissement, median, moyen, nb, surface = row

            report += f"{arrondissement:>5} | {int(median):>11} | {int(moyen):>11} | {nb:>12} | {int(surface):>6}\n"

        report += "------------------------------------------------------------\n"
        report += f"Total lignes insérées: {nb_inseres}\n"

        logger.info(report)

        return report


    t_verif = verifier_sources()
    t_download = telecharger_dvf()
    t_hdfs = upload_hdfs(t_download)
    t_traiter = traiter_donnees(t_hdfs)
    t_pg = inserer_postgresql(t_traiter)
    t_rapport = generer_rapport(t_pg)

    chain(t_verif, t_download, t_hdfs, t_traiter, t_pg, t_rapport)


pipeline_dvf()