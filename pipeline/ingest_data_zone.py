#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target_table', default='green_taxi_zone', help='Target table name')
@click.option('--chunksize', default=10000, type=int, help='Chunk size for processing')


def run(user, password, host, port, db, target_table, chunksize):
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        iterator=True,
        chunksize=chunksize,
    )

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
