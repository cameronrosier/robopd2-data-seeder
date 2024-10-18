import json
import codecs
import argparse
import logging
import asyncio

from pathlib import Path
from app.cosmosdb import store_items_in_cosmos
from app.mongodb import store_documents_in_mongo

LOG_LEVEL = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def main(json_files_path: str, storage_engine: str) -> None:
    """
    Main entrypoint into the application.
    Will takes a folder of json files, de-serialize them into a dictionary, and store them in a database (Cosmos or Mongo).

    Args:
        json_files_path (str): Path to folder containing json files
        storage_engine (str): Storage engine to use (Cosmos or Mongo)
    
    Returns:
        None
    """
    json_files = list(Path(json_files_path).iterdir())
    for json_file in json_files:
        data = json.load(codecs.open(Path(json_files_path) / json_file, 'r', 'utf-8-sig'))
        if storage_engine == 'cosmos':
            await store_items_in_cosmos(json_file.stem, data)
        elif storage_engine == 'mongo':
            await store_documents_in_mongo(json_file.stem, data)
        else:
            raise ValueError(f"Unknown storage engine: {storage_engine}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json-files", dest="json_files", type=str, help="path to json files")
    parser.add_argument("--storage-engine", dest="storage_engine", type=str, choices=["cosmos", "mongo"], help="storage engine")
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args.json_files, args.storage_engine))
