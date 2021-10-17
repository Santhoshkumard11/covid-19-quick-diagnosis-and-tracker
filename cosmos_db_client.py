import logging
import azure.cosmos.cosmos_client as cosmos_client
from azure.cosmos.partition_key import PartitionKey
import os
import random
from datetime import datetime

# Building a custom cosmos db client
class CosmosDBClient:
    def __init__(self, database_name: str, container_name: str, partition_key_name: str):

        # get all the configurations to connect to the Cosmos DB
        self.HOST = os.environ.get("COSMOS_DB_HOST")
        self.MASTER_KEY = os.environ.get("COSMOS_DB_HOST_KEY")

        self.database_name = database_name

        self.container_name = container_name
        self.partition_key_name = partition_key_name

    def connect(self):
        """Initiate a connection to cosmos DB"""
        # connect with the cosmos DB client

        self.client = cosmos_client.CosmosClient(
            self.HOST,
            {"masterKey": self.MASTER_KEY},
            user_agent="CosmosDB",
            user_agent_overwrite=True,
        )

        # get the database connection object
        self.database = self.client.create_database_if_not_exists(id=self.database_name)

        # get the container connection object
        self.container = self.database.create_container_if_not_exists(
            id=self.container_name,
            partition_key=PartitionKey(path=f"/{self.partition_key_name}"),
            offer_throughput=400,
        )

        logging.info("Successfully connected to CosmosDB")

    def add_item(self,item: dict):
        """Add new item to cosmos DB"""
        try:
            # create item in document from cosmos db
            item["id"] = str(random.randint(1,10000)) + datetime.now().strftime("%d%m%y%H%M%S")
            
            logging.info(f"Trying to add new item - {item}")
            
            self.container.create_item(body=item)

            logging.info(f"Successfully added the item to the database")

        except Exception as e:
            logging.error(f"Error while adding items to the container - {e}")
            raise e

    def get_all_items(self):
        """ Read all the items from cosmos db
        """
        read_items = ""
        
        try:
            # get the document from cosmos db
            read_items = list(self.container.read_all_items(max_item_count=100))
        
            logging.info(f"Successfully received all the items")
            
        except Exception as e:
            pass
            logging.error(
                f"Error while getting the items from the container")
            
        return read_items
