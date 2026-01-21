#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target_table', default='green_taxi_data', help='Target table name')
@click.option('--chunksize', default=10000, type=int, help='Chunk size for processing')


def run(user, password, host, port, db, target_table, chunksize):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_parquet('green_tripdata_2025-11.prct')

    # df = pd.read_parquet(url)
    df = df.astype(dtype)
    
    # Create chunks from the dataframe
    chunks = [df[i:i+chunksize] for i in range(0, len(df), chunksize)]
    df_iter = iter(chunks)
    first = True
    
    for df_chunk in tqdm(df_iter):
            if first:
                df_chunk.head(0).to_sql(
                    name=target_table,
                    con=engine,
                    if_exists='replace'
                )
                first = False

            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists='append'
            )

if __name__ == '__main__':
    run()

# def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
#     """Ingest NYC taxi data into PostgreSQL database."""
#     prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
#     url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

#     engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

#     df_iter = pd.read_csv(
#         url,
#         dtype=dtype,
#         parse_dates=parse_dates,
#         iterator=True,
#         chunksize=chunksize,
#     )

#     first = True

#     for df_chunk in tqdm(df_iter):
#         if first:
#             df_chunk.head(0).to_sql(
#                 name=target_table,
#                 con=engine,
#                 if_exists='replace'
#             )
#             first = False

#         df_chunk.to_sql(
#             name=target_table,
#             con=engine,
#             if_exists='append'
#         )

# if __name__ == '__main__':
#     run()