import sys
import gc
import io

import pandas as pd
from sqlalchemy import create_engine
from minio import Minio


def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.Dataframe) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success: bool = True
            print("Connection successful! Writing Parquet file to database")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')

    except Exception as e:
        success: bool = False
        print(f"Error connecting to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite its columns into a lowercase format.

    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def main() -> None:
    # Configuration Minio
    minio_endpoint = 'localhost:9000'
    minio_access_key = 'minio'
    minio_secret_key = 'minio123'
    minio_bucket_name = 'buckettaxi'
    minio_file_prefix = 'yellow_tripdata_2023-'  # Préfixe des noms de fichiers Parquet dans Minio

    # Initialiser le client Minio
    minio_client = Minio(minio_endpoint,
                         access_key=minio_access_key,
                         secret_key=minio_secret_key,
                         secure=False)

    print(f"connection minio ok")
    # Récupérer la liste des objets (fichiers) dans le bucket Minio
    parquet_files = [obj.object_name for obj in minio_client.list_objects(minio_bucket_name)
                     if obj.object_name.startswith(minio_file_prefix)]
    print(f"liste d'objet récupéré")

    for parquet_file in parquet_files:
        # Télécharger le fichier Parquet depuis Minio et le lire dans un DataFrame
        parquet_object = minio_client.get_object(minio_bucket_name, parquet_file)
        
        # Read Parquet file content into BytesIO object
        parquet_content = parquet_object.read()

        # Use BytesIO object to read Parquet file using pd.read_parquet
        parquet_df = pd.read_parquet(io.BytesIO(parquet_content), engine="pyarrow")

        print(f"Nettoyage des colonnes")
        # Nettoyer les noms de colonnes du DataFrame
        clean_column_name(parquet_df)
        print(f"Colonne nettoyé")

        # Écrire les données dans PostgreSQL
        if not write_data_postgres(parquet_df):
            print(f"Écrire les données dans PostgreSQL")
            del parquet_df
            gc.collect()
            return

        del parquet_df
        gc.collect()


if __name__ == '__main__':
    sys.exit(main())
