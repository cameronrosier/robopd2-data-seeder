import os
import logging
from pymongo import MongoClient


logger = logging.getLogger(__name__)
mongodb_uri = os.getenv('MONGODB_URI')
database_name = 'robopd2'

async def get_or_create_db(client: MongoClient) -> dict:
    """
    Given a MongoDB client, get or create a database with name 'database_name'

    Args:
        client (MongoClient): MongoDB client
        database_name (str): Name of the database to get or create
    
    Returns:
        dict: Dictionary containing the database
    """

    logger.info(f"Getting or creating database: {database_name}")
    database = client[database_name]
    return database


async def get_or_create_collection(database: dict, collection_name: str) -> dict:
    """
    Given a database, get or create a collection with name 'collection_name'

    Args:
        database (dict): Dictionary containing the database
        collection_name (str): Name of the collection to get or create
    
    Returns:
        dict: Dictionary containing the collection
    """

    logger.info(f"Getting or creating collection: {collection_name}")
    collection = database[collection_name]
    return collection


async def store_documents_in_mongo(collection_name: str, data: list) -> None:
    """
    Given a list of data, store each item in MongoDB collection named 'collection_name'

    Args:
        collection_name (str): Name of the MongoDB collection
        data (list): List of data to store
    
    Returns:
        None
    """

    client = MongoClient(mongodb_uri)
    database = await get_or_create_db(client)
    collection = await get_or_create_collection(database, collection_name)
    for item in data:
        try:
            inserted_item = collection.insert_one(item)
            logger.info(f"Inserted item {inserted_item.inserted_id} into {collection_name}.")

        except Exception as e:
            logger.error(f"Failed to insert item into MongoDB collection {collection_name}: {e}")
    logger.info(f"Finished inserting {len(data)} items into MongoDB collection {collection_name}.")
