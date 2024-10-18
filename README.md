# RoboPD2 Data Seeder
The purpose of this application is to seed data from json files into either CosmosDB or MongoDB

## Getting Started
1. Create a virtual environment
`python3 -m venv .venv`

2. Activate the virtual environment
`source .venv/bin/activate`

3. Install the requirements
`pip install -r requirements.txt`

4. Run the application
`python3 main.py --json-files="path/to/pd2-json-files/json" --storage-engine cosmos`
The path should be the path to the json files that were outputted from robopd2-data-generator

5. Verify that the files are added to the database in the Azure Portal under CosmosDB or in MongoDB Compass