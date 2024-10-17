import os
from minio import Minio
class MinIOClient:
    def __init__(self, endpoint, access_key, secret_key):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
    
    def create_conn(self):
        client = Minio(
            endpoint= self.endpoint,
            access_key= self.access_key,
            secret_key= self.secret_key,
            secure=False
        )
        return client

    def create_bucket(self,bucket_namne):
        client = self.create_conn()

        found = client.bucket_exists(bucket_name=bucket_namne)
        if not found:
            client.make_bucket(bucket_name=bucket_namne)
            print(f"Bucket {bucket_namne} created successfully!")
        else:
            print(f"Bucket {bucket_namne} already exists")

    def list_parquet_files(self, bucket_name, prefix=""):
        client = self.create_conn()
        objects = client.list_objects(bucket_name,prefix,recursive=True)
        parquet_files = [obj.object_name for obj in objects if obj.object_name.endwith('.parquet')]
        return parquet_files



    
