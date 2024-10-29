import os
from dotenv import load_dotenv
from postgresql_client import Postgresql_Client
load_dotenv("../.env")

def main():
    pc = Postgresql_Client(
        database= os.getenv("POSTGRES_DB"),
        user= os.getenv("POSTGRES_USER"),
        password= os.getenv("POSTGRES_PASSWORD")
    )
    create_table_staging = """
        CREATE TABLE IF NOT EXISTS staging.nyc_taxi (
            year                    VARCHAR,
            month                   VARCHAR,
            dow                     VARCHAR,
            vendor_id               INT, 
            rate_code_id            FLOAT, 
            pickup_location_id      INT, 
            dropoff_location_id     INT, 
            payment_type_id         INT, 
            service_type            INT,
            pickup_datetime         TIMESTAMP WITHOUT TIME ZONE, 
            dropoff_datetime        TIMESTAMP WITHOUT TIME ZONE, 
            pickup_latitude         FLOAT,
            pickup_longitude        FLOAT,
            dropoff_latitude        FLOAT,
            dropoff_longitude       FLOAT,
            passenger_count         FLOAT, 
            trip_distance           FLOAT,
            extra                   FLOAT, 
            mta_tax                 FLOAT, 
            fare_amount             FLOAT, 
            tip_amount              FLOAT, 
            tolls_amount            FLOAT, 
            total_amount            FLOAT, 
            improvement_surcharge   FLOAT, 
            congestion_surcharge    FLOAT
        );
    """
    try:
        pc.execute_query(create_table_staging)
    except Exception as e:
        print(f"Failed to create table with error: {e}")

if __name__ =="__main__":
    main()