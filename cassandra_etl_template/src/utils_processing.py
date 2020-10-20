import os
import glob
import csv
from tqdm import tqdm

from cassandra_etl_template.src import sql_queries


def get_index_of_columns(full_columns, subset_columns):
    """Get indexes of a subset of column names"""
    return [full_columns.index(col) for col in subset_columns]


def clean_csv_dataset(path_data, path_data_processed):
    """Aggregate and clean raw csv data.
    Store the clean file in a different directory.
    Args:
        path_data(str): directory of the raw csv data
        path_data_processed(str): directory of the processed csv file

    Returns:
        str: path of processed csv file
    """
    # generate path of output file
    os.makedirs(path_data_processed, exist_ok=True)
    file_processed = os.path.join(path_data_processed, 'event_datafile_new.csv')

    # collect each file from the data path
    for root, dirs, files in os.walk(path_data):
        file_path_list = glob.glob(os.path.join(root, '*'))

    print("Aggregating files in: {}".format(path_data))
    full_data_rows_list = []
    for f in file_path_list:
        # reading csv file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            # extracting each data row one by one and append it
            for line in csvreader:
                full_data_rows_list.append(line)
    print("Lines in the unique raw file: {}".format(len(full_data_rows_list)))

    print("Processing file, storing clean file in: {}".format(file_processed))
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open(file_processed, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName',
                         'length', 'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if row[0] == '':
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6],
                             row[7], row[8], row[12], row[13], row[16]))

    with open(file_processed, 'r', encoding='utf8') as f:
        print("Lines in the clean file: {}".format(sum(1 for line in f)))

    return file_processed


def etl_session_analysis(session, file_processed):
    """Line by line loading of a subset of columns into session_analysis table"""
    with open(file_processed, encoding='utf8') as f:
        csvreader = csv.reader(f)

        # count lines and return to line 0
        lines = len(list(csvreader)) - 1
        f.seek(0)

        # use the header to select the indexes
        header = next(csvreader)
        column_indexes = get_index_of_columns(
            header,
            ["sessionId", "itemInSession", "artist", "song", "length"]
        )
        # select columns and cast types from string
        for line in tqdm(csvreader, total=lines, desc="Inserting lines into session_analysis table"):
            line = [line[i] for i in column_indexes]
            line = list(map(lambda elem, type_i: type_i(elem), line, [int, int, str, str, float]))
            session.execute(sql_queries.session_analysis_insert, line)


def etl_user_activity(session, file_processed):
    """Line by line loading of a subset of columns into user_activity"""
    with open(file_processed, encoding='utf8') as f:
        csvreader = csv.reader(f)

        # count lines and return to line 0
        lines = len(list(csvreader)) - 1
        f.seek(0)

        # use the header to select the indexes
        header = next(csvreader)
        column_indexes = get_index_of_columns(
            header,
            ["userId", "sessionId", "itemInSession", "artist", "song", "firstName", "lastName"]
        )
        # select columns and cast types from string
        for line in tqdm(csvreader, total=lines, desc="Inserting lines into user_activity table"):
            line = [line[i] for i in column_indexes]
            line = list(map(lambda elem, type_i: type_i(elem), line, [int, int, int, str, str, str, str]))
            session.execute(sql_queries.user_activity_insert, line)


def etl_song_preference(session, file_processed):
    """Line by line loading of a subset of columns into song_preference"""
    with open(file_processed, encoding='utf8') as f:
        csvreader = csv.reader(f)

        # count lines and return to line 0
        lines = len(list(csvreader)) - 1
        f.seek(0)

        # use the header to select the indexes
        header = next(csvreader)
        column_indexes = get_index_of_columns(
            header,
            ["song", "userId", "lastName", "firstName"]
        )
        # select columns and cast types from string
        for line in tqdm(csvreader, total=lines, desc="Inserting lines into song_preference table:q"):
            line = [line[i] for i in column_indexes]
            line = list(map(lambda elem, type_i: type_i(elem), line, [str, int, str, str]))
            session.execute(sql_queries.song_preference_insert, line)
