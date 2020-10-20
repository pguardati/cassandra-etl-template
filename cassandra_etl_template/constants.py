import os

# General
PROJECT_NAME = 'cassandra_etl_template'
REPOSITORY_PATH = os.path.realpath(__file__)[:os.path.realpath(__file__).find(PROJECT_NAME)]
PROJECT_PATH = os.path.join(REPOSITORY_PATH, PROJECT_NAME)

DIR_DATA = os.path.join(REPOSITORY_PATH, 'event_data')
DIR_DATA_PROCESSED = os.path.join(REPOSITORY_PATH, 'event_data_processed')

DIR_DATA_TEST = os.path.join(PROJECT_PATH, 'tests', 'test_data')
DIR_DATA_PROCESSED_TEST = os.path.join(PROJECT_PATH, 'tests', 'test_data_processed')

print("Project Path:{}".format(PROJECT_PATH))
print("Data Path:{}".format(DIR_DATA))
