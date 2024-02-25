# db_utils.py
import psycopg2
import psycopg2.extras
import logging
from PredictionFunction.utils.params import params
from datetime import datetime


def execute_query(query, query_name):
    today = datetime.now()
    start_of_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, [start_of_month, today])
                conn.commit()
            logging.info(f"Successfully executed query from {query_name}")
    except Exception as e:
        logging.info(f"Error while executing {query_name}: {e}")
