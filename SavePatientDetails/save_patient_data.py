from cosmos_db_client import CosmosDBClient

import logging

def save_patient_details(patent_json_date:dict):
    
    try:

        cosmos_db_client_obj = CosmosDBClient("covid-19-quick-diagnosis", "PatientTable","pincode")

        cosmos_db_client_obj.connect()
        
        new_item_dict = {
            "name": patent_json_date.get("name"),
            "age": patent_json_date.get("age"),
            "gender": patent_json_date.get("gender"),
            "father_name": patent_json_date.get("father_name"),
            "contact_number": patent_json_date.get("contact_number"),
            "state": patent_json_date.get("state"),
            "district": patent_json_date.get("district"),
            "pincode": patent_json_date.get("pincode"),
            "test_result": patent_json_date.get("test_result"),
            "is_vaccinated_status": patent_json_date.get("is_vaccinated_status"),
            "vaccine_name": patent_json_date.get("vaccine_name")}
        
        cosmos_db_client_obj.add_item(new_item_dict)
        
        return "success", "none"
    
    except Exception as e:
        
        logging.exception("An exception occurred while saving user data")
        
        return "failed", str(e)
    
