import sys
import os
from glob import glob
from minio import Minio

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)

from helper import load_cfg
from minio_utils import MinIOClient

cfg_file = "../Config/datalake.yaml"
years = ["2022"]

def extract_load(endpoint_url, access_key, secret_key):
    cfg =  load_cfg(cfg_file)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    client = MinIOClient(
        endpoint= endpoint_url,
        access_key= access_key,
        secret_key= secret_key
    )
    client.create_bucket(datalake_cfg["bucket_name_1"])
    for year in years:
        data_files_local = glob(os.path.join("..",nyc_data_cfg["folder_path"],year,"*.parquet"))
        for file in data_files_local:
            print(f"Updating {file}")
            client_minio = client.create_conn()
            client_minio.fput_object(
                bucket_name=datalake_cfg["bucket_name_1"],
                object_name= f"{datalake_cfg["folder_name"]}/{os.path.basename(file)}",
                file_path= file
            )
if __name__ == "__main__":
    cfg = load_cfg(cfg_file)
    datalake = cfg["datalake"]

    endPoint_url_local = datalake["endpoint"]
    access_key_local = datalake["access_key"]
    secret_key_local = datalake["secret_key"]
    extract_load(endPoint_url_local,access_key_local,secret_key_local)



