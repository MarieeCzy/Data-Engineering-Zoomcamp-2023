from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True, retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read taaxi data from web into pandas DataFrame'''
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    '''Fix some dtype issues'''
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(df.shape)
    return df
 
@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    


@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    '''The main ETL function'''
    color = 'yellow'
    year = 2021
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
    
    df = fetch(dataset_url)
    clean_df = clean(df)
    path = write_local(clean_df, color, dataset_file)

if __name__ == "__main__":
    etl_web_to_gcs()