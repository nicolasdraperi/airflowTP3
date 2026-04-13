import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)

WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"


class WebHDFSClient:
    """Client léger pour l'API WebHDFS d'Apache Hadoop."""

    def __init__(self, base_url: str = WEBHDFS_BASE_URL, user: str = WEBHDFS_USER):
        self.base_url = base_url
        self.user = user

    def _url(self, path: str, op: str, **params) -> str:
        """Construit l'URL WebHDFS pour une opération donnée."""
        url = f"{self.base_url}{path}?op={op}&user.name={self.user}"
        for key, value in params.items():
            url += f"&{key}={value}"
        return url

    def mkdirs(self, hdfs_path: str) -> bool:
        """Crée un répertoire dans HDFS."""
        url = self._url(hdfs_path, "MKDIRS")
        response = requests.put(url)

        if response.status_code == 200:
            return response.json().get("boolean", False)
        else:
            raise Exception(f"MKDIRS failed: {response.text}")

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        """Upload un fichier vers HDFS (2 étapes WebHDFS)."""

        # Étape 1 : demande de redirection au NameNode
        url = self._url(hdfs_path, "CREATE", overwrite="true")
        response = requests.put(url, allow_redirects=False)

        if response.status_code != 307:
            raise Exception(f"Upload step 1 failed: {response.text}")

        redirect_url = response.headers["Location"]

        # Étape 2 : upload réel vers le DataNode
        with open(local_file_path, "rb") as f:
            response = requests.put(redirect_url, data=f)

        if response.status_code not in (200, 201):
            raise Exception(f"Upload step 2 failed: {response.text}")

        return hdfs_path

    def open(self, hdfs_path: str) -> bytes:
        """Lire un fichier HDFS."""
        url = self._url(hdfs_path, "OPEN")

        response = requests.get(url, allow_redirects=True)

        if response.status_code == 200:
            return response.content
        else:
            raise Exception(f"OPEN failed: {response.text}")

    def exists(self, hdfs_path: str) -> bool:
        """Vérifie si un fichier existe."""
        url = self._url(hdfs_path, "GETFILESTATUS")

        response = requests.get(url)

        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            raise Exception(f"EXISTS failed: {response.text}")

    def list_status(self, hdfs_path: str) -> list:
        """Liste un dossier HDFS."""
        url = self._url(hdfs_path, "LISTSTATUS")

        response = requests.get(url)

        if response.status_code == 200:
            return response.json()["FileStatuses"]["FileStatus"]
        else:
            raise Exception(f"LISTSTATUS failed: {response.text}")