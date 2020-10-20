"""Query 1: - session_analysis
Goal:
Get Artist, Song title and Length in the music app history
that was heard during a given session and with a given amount of items in that session.

Table Design:
Partition key: session_id
Clustering key: item_in_session
"""

session_analysis_create = """CREATE TABLE IF NOT EXISTS session_analysis (
                      session_id int, 
                      item_in_session int, 
                      artist_name text, 
                      song_title text, 
                      song_length float,
                      PRIMARY KEY (session_id, item_in_session))"""

session_analysis_insert = """INSERT INTO session_analysis (
                      session_id, 
                      item_in_session, 
                      artist_name, 
                      song_title, 
                      song_length) 
                      VALUES (%s, %s, %s, %s, %s)"""

session_analysis_query = """SELECT artist_name, song_title, song_length 
                     FROM session_analysis 
                     WHERE session_id=%s AND item_in_session=%s;"""

session_analysis_drop = """DROP TABLE IF EXISTS session_analysis"""

"""Query2: user_activity
Goal:
Get Artist and Song title, sorted by the number of items in a sessions,
and Name and Surname of the user for a given userid and sessionid 

Table Design:
Partition key: user_id, session_id
Clustering key: item_in_session

"""
user_activity_create = """CREATE TABLE IF NOT EXISTS user_activity (
                      user_id int, 
                      session_id int, 
                      item_in_session int, 
                      artist_name text, 
                      song_title text, 
                      user_first_name text,
                      user_last_name text,
                      PRIMARY KEY ((user_id, session_id), item_in_session))"""

user_activity_insert = """INSERT INTO user_activity (
                      user_id, 
                      session_id, 
                      item_in_session, 
                      artist_name, 
                      song_title, 
                      user_first_name,
                      user_last_name) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s)"""

user_activity_query = """SELECT artist_name, song_title, user_first_name, user_last_name  
                     FROM user_activity 
                     WHERE user_id=%s AND session_id=%s;"""

user_activity_drop = """DROP TABLE IF EXISTS user_activity"""

"""Query3: song_preference
Goal:
Get Name and Surname of every user that listened to a given song

Table Design:
Partition key: song_title
Clustering key: user_id
"""
song_preference_create = """CREATE TABLE IF NOT EXISTS song_preference (
                      song_title text, 
                      user_id int,
                      user_last_name text,
                      user_first_name text,
                      PRIMARY KEY (song_title, user_id))"""

song_preference_insert = """INSERT INTO song_preference (
                      song_title, 
                      user_id,
                      user_last_name,
                      user_first_name)
                      VALUES (%s, %s, %s, %s)"""

song_preference_query = """SELECT user_last_name, user_first_name  
                     FROM song_preference
                     WHERE song_title=%s;"""

song_preference_drop = """DROP TABLE IF EXISTS song_preference"""

"""Iterators to loop over all tables"""
table_names = [
    "session_analysis",
    "user_activity",
    "song_preference"
]
create_table_queries = [
    session_analysis_create,
    user_activity_create,
    song_preference_create
]
drop_table_queries = [
    session_analysis_drop,
    user_activity_drop,
    song_preference_drop
]
check_table_queries = [
    session_analysis_query,
    user_activity_query,
    song_preference_query
]
