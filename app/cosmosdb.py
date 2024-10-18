import os
import uuid
import logging
from azure.cosmos.aio import CosmosClient as cosmos_client
from azure.cosmos import exceptions


logger = logging.getLogger(__name__)
endpoint = os.getenv('AZURE_COSMOSDB_ENDPOINT')
key = os.getenv('AZURE_COSMOSDB_KEY')

database_name = 'robopd2-cosmosdb'

async def get_or_create_db(client: cosmos_client, database_name: str) -> dict:
    """
    Given a CosmosDB client, get or create a database with name 'database_name'

    Args:
        client (CosmosClient): CosmosDB client
        database_name (str): Name of the database to get or create
    
    Returns:
        dict: Dictionary containing the database
    """

    logger.info(f"Getting or creating database: {database_name}")
    try:
        database = client.get_database_client(database_name)
        await database.read()
        return database
    except exceptions.CosmosResourceNotFoundError:
        logger.info(f"Database {database_name} not found. Creating...")
        return await client.create_database_if_not_exists(id=database_name)


async def get_or_create_container(database: dict, container_name: str) -> dict:
    """
    Given a database, get or create a container with name 'container_name'

    Args:
        database (dict): Dictionary containing the database
        container_name (str): Name of the container to get or create
    
    Returns:
        dict: Dictionary containing the container
    """

    logger.info(f"Getting or creating container: {container_name}")
    try:
        container = database.get_container_client(container_name)
        await container.read()
        return container
    except exceptions.CosmosResourceNotFoundError:
        logger.error(f"Container {container_name} did not exist. Containers are managed through Terraform.  Check that your Terraform applied correctly.")
        raise NotImplementedError("These containers are managed through Terraform.  Check that your Terraform applied correctly.")


async def store_items_in_cosmos(container_name: str, data: list) -> None:
    """
    Given a dictionary of data, store each item in CosmosDB Contaienr named 'container_name'

    Args:
        container_name (str): Name of the CosmosDB Container
        data (list): Dictionary of data to store
    
    Returns:
        None
    """

    async with cosmos_client(endpoint, credential = key) as client:
        database = await get_or_create_db(client, database_name)
        container = await get_or_create_container(database, container_name)
        for item in data:
            try:
                item['id'] = str(uuid.uuid4())
                inserted_item = await container.create_item(body=item)
                logger.info(f"Inserted item {inserted_item['id']} into {container_name}.")

            except exceptions.CosmosHttpResponseError as e:
                logger.error(f"Failed to insert item with ID: {item['id']} into CosmosDB Container {container_name}: {e}")
        logger.info(f"Finished inserting {len(data)} items into CosmosDB Container {container_name}.")