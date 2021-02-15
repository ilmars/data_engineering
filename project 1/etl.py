import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np


def process_song_file(cur, filepath):
    '''
    proceeds files from data/song_data (filepath) and saves them 
    to database tables "songs" and "artists"

            Parameters:
                    cur (obj): database connection currsor
                    filepath (text): file path
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    # print(df['title'][0])
    file_object = open('song_data.txt', 'a', encoding="utf-8")
    # Append song title at the end of file
    file_object.write('\n')
    file_object.write(df['title'][0])
    file_object.close()
    print(song_data.values.tolist()[0])
    cur.execute(song_table_insert, song_data.values.tolist()[0])

    # insert artist record
    artist_data = df[['artist_id', 'artist_name',
                      'artist_location', 'artist_latitude', 'artist_longitude']]
    # replaces all Nan values to 0 (affects lat and lng)
    artist_data = artist_data.astype(object).replace(np.nan, 0)
    cur.execute(artist_table_insert, artist_data.values.tolist()[0])


def process_log_file(cur, filepath):
    '''
    proceeds files from data/log_data (filepath) and saves them 
    to database tables "times", "users" and "songplay"

            Parameters:
                    cur (obj): database connection currsor
                    filepath (text): file path
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [df['ts'], t.dt.hour, t.dt.day,
                 t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day',
                     'week', 'month', 'year', 'weekday']
    time_df = pd.concat(time_data, axis=1, keys=column_labels)  # , columns

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # file_object = open('log_data.txt', 'a', encoding="utf-8")
        # # Append new line and song title at the end of file
        # file_object.write('\n')
        # file_object.write(row.song)
        # file_object.close()

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Cravls throu data files directories and send each 
    file to corresponding function.

            Parameters:
                    cur (obj): database connection currsor
                    conn (obj): database connection object
                    filepath (text): log files directory path
                    function (text): file proceeding function name

    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def check_data():
    # check log data against song data
    df = pd.read_csv('log_data.txt', sep=";", header=None)
    df.columns = ["title"]

    # Keep only unique sonng names and add column with false flag
    # for verification if song exist in song_data
    dfGrouped = df.groupby(['title']).size().to_frame('size')
    dfGrouped.sort_values('size', ascending=False)
    dfGrouped['is_in_song_data'] = 0

    # Create song_data dataframe
    dfSongs = pd.read_csv('song_data.txt', sep=";", header=None)
    dfSongs.columns = ["song"]

    for index, row in dfGrouped.iterrows():
        if (dfSongs['song'] == row.name).any():
            dfGrouped['is_in_song_data'][index] = 1
            print(row.name, True)

    print(dfGrouped.loc[dfGrouped['is_in_song_data'] == True])


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    if os.path.exists("song_data.txt"):
        os.remove("song_data.txt")
    if os.path.exists("log_data.txt"):
        os.remove("log_data.txt")
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

    # check_data()


if __name__ == "__main__":
    main()
