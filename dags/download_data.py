import requests
import zipfile
import logging
from pathlib import Path

def download_and_extract_data():
    logging.basicConfig(level=logging.DEBUG)
    url = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
    response = requests.get(url)
    zip_path = "ml-latest-small.zip"
    extract_path = Path("data")
    
    with open(zip_path, "wb") as file:
        file.write(response.content)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    logging.info("Data successfully downloaded and extracted.")