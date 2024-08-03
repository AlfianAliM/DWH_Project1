import os
import connection
import sqlparse
import pandas as pd

try:
    if __name__ == '__main__':
        # connection data source
        conf = connection.config('marketplace_prod')
        conn, engine = connection.get_conn(conf, 'DataSource')
        if conn is not None and engine is not None:
            cursor = conn.cursor()
        else:
            print("Failed to connect to DataSource")

        # connection dwh
        conf_dwh = connection.config('dwh')
        conn_dwh, engine_dwh = connection.get_conn(conf_dwh, 'DWH')
        if conn_dwh is not None and engine_dwh is not None:
            cursor_dwh = conn_dwh.cursor()
        else:
            print("Failed to connect to DWH")

        # get query string
        path_query = os.getcwd() + '/query/'
        query = sqlparse.format(
            open(path_query + 'query.sql', 'r').read(), strip_comments=True
        ).strip()
        dwh_design = sqlparse.format(
            open(path_query + 'dwh_design.sql', 'r').read(), strip_comments=True
        ).strip()

        # get data
        print('[INFO] service etl is running..')
        df = pd.read_sql(query, engine)
        print(df)

        # create schema 
        cursor_dwh.execute(dwh_design)
        conn_dwh.commit()

        # ingest data to dwh
        df.to_sql(
            'dim_orders_alfian',
            engine_dwh,
            schema='public',
            if_exists='replace',
            index=False
        )


except Exception as e:
    print(f"An error occurred: {str(e)}")
