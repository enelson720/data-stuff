import logging
import sys
import pandas as pd
from time import time
from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv, find_dotenv
import os
from os.path import join, dirname


from snowflake.sqlalchemy import URL as snowflake_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

def snowflake_engine_factory():

    load_dotenv(dotenv_path=find_dotenv(), verbose=True,override=True)

    conn_string = snowflake_URL(
        user=str(os.environ.get("SNOWFLAKE_USERNAME")),
        password=str(os.environ.get("SNOWFLAKE_PASSWORD")),
        account=str(os.environ.get("SNOWFLAKE_ACCOUNT")),
        database=str(os.environ.get("SNOWFLAKE_ANALYTICS_DB")),
        warehouse=str(os.environ.get("SNOWFLAKE_WAREHOUSE")),
        #role=vars_dict["ROLE"],  # Don't need to do a lookup on this one
        #schema=schema,
    )

    return create_engine(conn_string, connect_args={"sslcompression": 0})


def execute_query(engine: Engine, query: str) -> List[Tuple[Any]]:
    """
    Execute DB queries safely.
    """

    try:
        logging.warning(f"Running query on Snowflake: \n{query}")
        connection = engine.connect()
        results = connection.execute(query).fetchall()
    finally:
        connection.close()
        engine.dispose()
    return results


def execute_dataframe(engine, query):
    """ Takes a query as a string argument and executes it.
        Results stored in dataframe if as_df = True. """
    cur = engine.raw_connection().cursor()

    try:
        results = cur.execute(query)
        df = pd.DataFrame.from_records(
            iter(results), columns=[x[0] for x in cur.description]
        )
        return df
    except Exception as e:
        return print(f"Oh no! There was an error executing your query: {e}")
    cur.close()