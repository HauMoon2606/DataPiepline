import pandas as pd
from glob import glob
import sys
import os
from minio import Minio
import time
import s3fs

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)

from helper import load_cfg
from minio_utils import MinIOClient
# #path in airflow docker
# cfg_file = "/opt/airflow/Config/datalake.yaml"

#path in local
cfg_file = "../Config/datalake.yaml"
taxi_lookup_path = os.path.join(os.path.dirname(__file__),"Data","taxi_lookup.csv")

def drop_column(df:pd.DataFrame, file: str):
    if "store_and_fwd_flag" in df.columns:
        df=df.drop(columns=["store_and_fwd_flag"])
        print(f"Dropped column store_and_fwd_flag from file {file}")
    else:
        print(f"Column store_and_fwd_flag not found in file {file}")

    if file.startswith('green'):
        df = df.drop(columns=["ehail_fee"])
    if file.startswith('yellow'):
        df = df.drop(columns=['airport_fee'])
    return df

def merge_taxi_zone(df:pd.DataFrame, file):
    df_lookup = pd.read_csv(taxi_lookup_path)

    def merge_and_rename(df:pd.DataFrame,location_id,lat_col,long_col):
        df = df.merge(df_lookup, left_on=location_id, right_on="LocationID")
        df = df.drop(columns=["LocationID","Borough","service_zone","zone"])
        df = df.rename(columns={
            "latitude":lat_col,
            "longitude":long_col
        })
        return df
    
    if "pickup_latitude" not in df.columns:
        df = merge_and_rename(df,"pulocationid","pickup_latitude","pickup_longitude")
    if "dropoff_latitude" not in df.columns:
        df = merge_and_rename(df,"dolocationid","dropoff_latitude","dropoff_longitude")

    df = df.drop(columns= [col for col in df.columns if "Unname" in col],errors='ignore').dropna()
    print(f"Merged data from file {file}")
    return df

def process(df:pd.DataFrame, file:str):
    if file.startswith("green"):
        df.rename(
            columns={
                "lpep_pickup_datetime":"pickup_datetime",
                "lpep_dropoff_datetime":"dropoff_datetime"
            },
            inplace= True
        )
        if "trip_type" in df.columns:
            df.drop(columns=["trip_type"], inplace=True)   
    elif file.startswith("yellow"):
        df.rename(
            columns={
                "tpep_pickup_datetime":"pickup_datetime",
                "tpep_dropoff_datetime":"dropoff_datetime",
            },
            inplace=True
        )
    if "payment_type" in df.columns:
        df["payment_type"] = df["payment_type"].astype(int)
    

    df = df.dropna()
    df = df.reindex(sorted(df.columns),axis=1)

    print("Transformed data from file: "+file)
    return df

def transform_data(endpoint_url, access_key, secret_key,year,month):
    cfg = load_cfg(cfg_file)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    client = MinIOClient(
        endpoint=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )
    client.create_bucket(datalake_cfg['bucket_name_2'])
    # run in airflow
    # data_files_local = glob(os.path.join("/opt/airflow",nyc_data_cfg["folder_path"],year,f"*_{year}-{month}.parquet"))

    #run in local
    data_files_local = glob(os.path.join("..",nyc_data_cfg['folder_path'],str(year),f"*_{year}-{month}.parquet"))
    for file in data_files_local:
        file_name = file.split('\\')[-1]
        print(f"Reading parquet file {file_name}")
        df = pd.read_parquet(file,engine="pyarrow")
        df.columns = map(str.lower,df.columns)

        df = drop_column(df, file_name)
        df = merge_taxi_zone(df,file_name)
        df = process(df, file_name)

        path_in_s3 = f"s3://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}/" + file_name

        storage_options = {
            'key': access_key,
            'secret': secret_key,
            'client_kwargs': {
                'endpoint_url': f"http://{endpoint_url}"
            }
        }

        df.to_parquet(path_in_s3,index=False,storage_options=storage_options, engine="pyarrow" )
        print("Finished transforming data in file: "+path_in_s3)
        print("="*100)

if __name__ == "__main__":
    cfg = load_cfg(cfg_file)
    datalake_cfg = cfg["datalake"]

    endpoint_url_local = datalake_cfg["endpoint"]
    access_key_local = datalake_cfg["access_key"]
    secret_key_local = datalake_cfg["secret_key"]
    transform_data(endpoint_url_local,access_key_local,secret_key_local,"2023","01")




    
    
