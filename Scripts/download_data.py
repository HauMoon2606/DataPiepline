import os
import requests
from datetime import datetime


#type in [yellow,green]
def download_file_by_month_year(type, year, month):
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    folder_path = f"/opt/airflow/Data/{year}"
    suffix = f"{type}_tripdata_{year}-{month}.parquet"
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)
    
    file_path = os.path.join(folder_path,suffix)

    url = prefix + suffix

    response = requests.get(url)

    with open(file_path,"wb") as file:
        file.write(response.content)
#test
if __name__ == "__main__":
    year = "2024"
    month = "01"
    download_file_by_month_year("yellow",year,month)
