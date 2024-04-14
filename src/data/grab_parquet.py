import os
import requests
from minio import Minio
import sys

def main():
    grab_data()
    write_data_minio()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    output_folder = "../../data/raw"
    months = [f"{month:02d}" for month in range(11, 13)] # Liste des mois de 11 et 12

    for month in months:
        file_name = f"yellow_tripdata_2023-{month}.parquet"
        url = base_url + file_name

        print(f"Downloading {file_name}...")
        try: 
            result = requests.get(url)
            with open(os.path.join(output_folder, file_name), 'wb') as f:
                f.write(result.content)
        except Exception as e:
            print("Erreur:", e)

def write_data_minio():
    """
    This method puts all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "buckettaxi"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    
    folder_path = "../../data/raw"
    parquet_files = [f for f in os.listdir(folder_path) if
        f.lower().endswith('.parquet') and os.path.isfile(os.path.join(folder_path, f))]

    for parquet_file in parquet_files:
        file_path = os.path.join(folder_path, parquet_file)
        object_name = f"{parquet_file}"  # Nom de l'objet dans le bucket Minio
        try:
            # Upload du fichier dans le bucket Minio
            client.fput_object(bucket, object_name, file_path)
            print(f"Uploaded {parquet_file} to Minio bucket {bucket} as {object_name}")
        except Exception as e:
            print(f"Error uploading {parquet_file} to Minio bucket {bucket}: {e}")

if __name__ == '__main__':
    main()
