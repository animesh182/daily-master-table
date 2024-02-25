import azure.functions as func
import logging
from PredictionFunction.raw_queries import historical_table_update
from PredictionFunction.utils.db_executor import execute_query


def main(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed.")

    all_queries = [
        (historical_table_update.raw_query, "historical_table_update"),
    ]

    for query, name in all_queries:
        execute_query(query, name)
