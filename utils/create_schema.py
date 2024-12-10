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
    # print(os.getenv("POSTGRES_DB"))
    # print(os.getenv("POSTGRES_USER"))
    # print(os.getenv("POSTGRES_PASSWORD"))

    create_staging_schema ="""CREATE SCHEMA IF NOT EXISTS staging;"""
    create_production_schema ="""CREATE SCHEMA IF NOT EXISTS production;"""
    create_streaming_schema = """CREATE SCHEMA IF NOT EXISTS streaming;"""

    try:
        pc.execute_query(create_staging_schema)
        pc.execute_query(create_production_schema)
        pc.execute_query(create_streaming_schema)
    except Exception as e:
        print(f"Failed to create schema with error: {e}")

if __name__ == "__main__":
    main()