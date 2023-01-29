import pandas as pd
from sqlalchemy import create_engine
from time import time
import  argparse

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.database
    table_name = params.table_name
    csv_name = 'output.csv'
    
    #dwonload the csv
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
 
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    #load first data to table
    df.to_sql(name=table_name, con=engine, if_exists='append')

    #load rest of chunks
    while True:
        start_time = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        end_time = time()
    
    print('Another chunk loaded in: %3f seconds.' %(end_time - start_time))
    
parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

parser.add_argument('user', help='username for postgres')
parser.add_argument('password', help='password for postgres')
parser.add_argument('host', help='host for postgres')
parser.add_argument('port', help='port for postgres', type=int)
parser.add_argument('db', help='database name for postgres')
parser.add_argument('table_name', help='name of the table where we will wirte the results to')
parser.add_argument('url', help='url of the csv file')

args = parser.parse_args()

if __name__ == 'main':
    main()