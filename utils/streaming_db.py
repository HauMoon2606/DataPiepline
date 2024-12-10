import os
import sys
from time import sleep
from pyarrow.parquet import ParquetFile
import pyarrow as pa
from dotenv import load_dotenv
load_dotenv("../.env")
from postgresql_client import Postgresql_Client


TABLE_NAME = "streaming.taxi_nyc_time_series"
PARQUET_FILE = "../Data/2023/yellow_tripdata_2023-01.parquet"
NUM_ROWS = 10000

def format_record(row):
    taxi_res = {
        'VendorID': row['VendorID'],
        'RatecodeID': row['RatecodeID'],
        'DOLocationID': row['DOLocationID'],
        'PULocationID': row['PULocationID'],
        'payment_type': row['payment_type'],
        'tpep_dropoff_datetime': str(row['tpep_dropoff_datetime']),
        'tpep_pickup_datetime': str(row['tpep_pickup_datetime']),
        'passenger_count': row['passenger_count'],
        'trip_distance': row['trip_distance'],
        'extra': row['extra'],
        'mta_tax': row['mta_tax'],
        'fare_amount': row['fare_amount'],
        'tip_amount': row['tip_amount'],
        'tolls_amount': row['tolls_amount'],
        'total_amount': row['total_amount'],
        'improvement_surcharge': row['improvement_surcharge'],
        'congestion_surcharge': row['congestion_surcharge'],
        'Airport_fee': row['Airport_fee'],
    }
    return taxi_res

def main():
    pc = Postgresql_Client(
        database= os.getenv("POSTGRES_DB"),
        user= os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
    except Exception as e:
        print(f"Failed with error:{e}")
    
    pf = ParquetFile(PARQUET_FILE)
    first_n_rows = next(pf.iter_batches(batch_size=NUM_ROWS))
    df = pa.Table.from_batches([first_n_rows]).to_pandas() 
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].astype(dtype='str')
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].astype(dtype='str')

    for _,row in df.iterrows():
        query = f"""
            insert into {TABLE_NAME} ({",".join(columns)})
            values {tuple(row)}
        """
        print(f"Sent: {format_record(row)}")
        pc.execute_query(query)
        print("-"*100)
        sleep(2)
if __name__ =="__main__":
    main()
    
