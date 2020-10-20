#cassandra-etl-template

Template of an ETL pipeline to load data into a Cassandra database.
The template is based on a NoSQL project designed for the Data Engineering Nanodegree of Udacity.

## Design
The database is designed to perform low latency queries on big datasets.
Each table in the keyspace is custom designed for one query only.  
Currently, 3 tables ara available:
- **session_analysis**, to get song choices in a given session
- **user_activity**, to get song choices from a given user
- **song_preference**, to get users that heard a given song

The database is designed to handle big dataset,  
hence transformation and loading of the data are performed
line by line on the fly.

Note:  
In etl.py, etl functions are similar to each other.  
Despite this, it has been chosen to leave them independently on purpose.

## Installation

Before to start:  
Add the current project folder path to PYTHONPATH.  
In ~/.bashrc, append:
```
PYTHONPATH=your/path/to/repo:$PYTHONPATH 
export PYTHONPATH
```
e.g.
```
PYTHONPATH=~/PycharmProjects/SparkifyCassandraDB:$PYTHONPATH 
export PYTHONPATH
```

To install and activate the environment:
```
conda env create -f environment.yml
conda activate sparkify_cassandra_db 
```


## Usage
To drop the current tables and create new empty ones:
```
python sparkify_cassandra_db/src/create_tables.py
```

To run the etl pipeline on the test data:
```
python sparkify_cassandra_db/src/etl.py sparkify_cassandra_db/tests/test_data/  sparkify_cassandra_db/tests/test_data_processed --reset-table
```

To run the etl pipeline on the full data,  
Download the data from Udacity and run:
```
python sparkify_cassandra_db/src/etl.py path/to/raw/data path/to/processed/data --reset-table
```
e.g:
```
python sparkify_cassandra_db/src/etl.py event_data event_data_processed --reset-table
```
Else a jupyter notebook is available to run the etl pipeline and the queries:
```
jupyter lab 
notebooks/example.py
```

To check the content of the database, run:
```
python sparkify_cassandra_db/src/check_database.py
```

## Tests
To run all unittests:
```
python -m unittest discover sparkify_cassandra_db/tests
```

