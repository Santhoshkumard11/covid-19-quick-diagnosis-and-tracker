import datetime
import logging
from cosmos_db_client import CosmosDBClient
from .generate_graph import GenerateGraph

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info("Generate word cloud trigger started!!")

    cosmos_db_client_obj = CosmosDBClient(
        "covid-19-quick-diagnosis", "PatientTable", "pincode")

    cosmos_db_client_obj.connect()

    logging.info("Successfully connected with cosmos db!!")

    generate_graph_obj = GenerateGraph(
        cosmos_db_client_obj)

    generate_graph_obj.start_process()

    logging.info("Word Cloud generation process successfully completed!!")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
