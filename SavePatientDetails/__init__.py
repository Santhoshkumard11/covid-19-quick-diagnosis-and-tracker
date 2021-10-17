import logging

import azure.functions as func
import json
from .save_patient_data import save_patient_details

def main(msg: func.ServiceBusMessage):
    
    logging.info('Python ServiceBus queue trigger processed message: %s',
                 msg.get_body().decode('utf-8'))
    

    patient_details_json = json.loads(msg.get_body().decode("utf-8"))
    
    result, error = save_patient_details(patient_details_json)
    
    logging.info(f"Result - {result} error - {error}")
    