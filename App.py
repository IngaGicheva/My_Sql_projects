#!/usr/bin/env python
# coding: utf-8

import sqlite3
import numpy as np
import random
import time
import datetime
from datetime import datetime


def creating_a_database():
    '''
    Creating a database "App.db"
    '''
    try:
        sqlite_connection = sqlite3.connect('App.db')  # connecting to the database
        cursor = sqlite_connection.cursor()  # create a Cursor to execute SQLite queries from Python
        print("Database connection is established successfully!")

        sqlite_select_query = "select sqlite_version() ;"  # getting the version of the SQLite database
        cursor.execute(sqlite_select_query)  # given database operation (query or command)
        record = cursor.fetchall()  # getting the result of the query from the resultSet
        print("SQLite database version: ", record)
        cursor.close()

    except sqlite3.Eror as error:  # handle any error and exception which might encounter while working with SQLite from Python
        print("Error Connecting to SQlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()  # close SQLite connection
            print("SQLite connection closed")


def creating_a_table_users():
    '''Creating a table users which contains data about user_id, username, user age, country
    '''
    try:
        sqlite_connection = sqlite3.connect('App.db')  # connecting to the database
        create_users_table = """CREATE TABLE IF NOT EXISTS users (
                             user_id INTEGER PRIMARY KEY,
                             name TEXT NOT NULL,
                             age INTEGER NOT NULL,
                             country TEXT NOT NULL); """  # create a users table if such table not exists before
        cursor = sqlite_connection.cursor()  # create a Cursor object
        print("Database connection is established successfully!")
        cursor.execute(create_users_table)  # database query
        sqlite_connection.commit()  # to make sure the changes made to the database are consistent
        print("SQLite table created")

        cursor.close()

    except sqlite3.Error as error:
        print("Error Connecting to SQlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("SQLite connection closed")


def creating_a_table_sessions():
    '''Creating a table sessions which contains data about session_id(PRIMARY KEY), user_id, session_start, session_end
    '''
    try:
        sqlite_connection = sqlite3.connect('App.db')  # connecting to the database
        create_sessions_table = """ CREATE TABLE IF NOT EXISTS sessions(
                                session_id INTEGER PRIMARY KEY,
                                user_id INTEGER NOT NULL,
                                session_start datetime,
                                session_end datetime
                                );
                                """  # create a sessions table if such table not exists before
        cursor = sqlite_connection.cursor()  # create a Cursor object
        print("Database connection is established successfully!")
        cursor.execute(create_sessions_table)  # database query
        sqlite_connection.commit()  # to make sure the changes made to the database are consistent
        print("SQLite table created")
        cursor.close()

    except sqlite3.Error as error:
        print("Error Connecting to SQlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("SQLite connection closed")


def insert_multiple_records_into_table(table_name, par, records):
    '''
    Insert multiple records into the table users
    '''
    try:
        sqlite_connection = sqlite3.connect('App.db')  # connecting to the database
        cursor = sqlite_connection.cursor()  # create a cursor
        print("Database connection is established successfully!")

        sqlite_insert_query = f'''INSERT INTO {table_name}{par}
                                  VALUES (?, ?, ?, ?);'''
        cursor.executemany(sqlite_insert_query, records)  # records are inserted into the table
        sqlite_connection.commit()  # to make sure the changes made to the database are consistent
        print("Records successfully inserted into the users table", cursor.rowcount)
        sqlite_connection.commit()  # to make sure the changes made to the database are consistent
        cursor.close()

    except sqlite3.Error as error:
        print("Error Connecting to SQlite", error)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            print("SQLite connection closed")


def generating_user_id():
    '''Generates a random sample from a given 1-D array without replacement
    '''
    user_id = np.random.choice(range(1000, 10000), 1000, replace=False)
    return user_id


def creating_names_list():
    '''Creates a names list  by split string with names by space
    '''
    name = 'Noah Liam Jacob William Mason Ethan Michael Alexander James Elijah Benjamin Daniel Aiden Logan Jayden Matthew Lucas David Jackson Joseph Anthony Samuel Joshua Gabriel Andrew John Christopher Oliver Dylan Carter Isaac Luke Henry Owen Ryan Nathan Wyatt Caleb Sebastian Jack Christian Jonathan Julian Landon Levi Isaiah Hunter Aaron Charles Thomas Eli Jaxon Connor Nicholas Jeremiah Grayson Cameron Brayden Adrian Evan Jordan Josiah Angel Robert Gavin Tyler Austin Colton Jose Dominic Brandon Ian Lincoln Hudson Kevin Zachary Adam Mateo Jason Chase Nolan Ayden Cooper Parker Xavier Asher Carson Jace Easton Justin Leo Bentley Jaxson Nathaniel Blake Elias Theodore Kayden Luis Tristan Ezra Bryson Juan Brody Vincent Micah Miles Santiago Cole Ryder Carlos Damian Leonardo Roman Max Sawyer Jesus Diego Greyson Alex Maxwell Axel Eric Wesley Declan Giovanni Ezekiel Braxton Ashton Ivan Hayden Camden Silas Bryce Weston Harrison Jameson George Antonio Timothy Kaiden Jonah Everett Miguel Steven Richard Emmett Victor Kaleb Kai Maverick Joel Bryan Maddox Kingston Aidan Patrick Edward Emmanuel Jude Alejandro Preston Luca Bennett Jesse Colin Jaden Malachi Kaden Jayce Alan Kyle Marcus Brian Ryker Grant Jeremy Abel Riley Calvin Brantley Caden Oscar Abraham Brady Sean Jake Tucker Nicolas Mark Amir Avery King Gael Kenneth Bradley Cayden Xander Graham Rowan Emma Olivia Sophia Isabella Ava Mia Abigail Emily Charlotte Madison Elizabeth Amelia Evelyn Ella Chloe Harper Avery Sofia Grace Addison Victoria Lily Natalie Aubrey Lillian Zoey Hannah Layla Brooklyn Scarlett Zoe Camila Samantha Riley Leah Aria Savannah Audrey Anna Allison Gabriella Claire Hailey Penelope Aaliyah Sarah Nevaeh Kaylee Stella Mila Nora Ellie Bella Alexa Lucy Arianna Violet Ariana Genesis Alexis Eleanor Maya Caroline Peyton Skylar Madelyn Serenity Kennedy Taylor Alyssa Autumn Paisley Ashley Brianna Sadie Naomi Kylie Julia Sophie Mackenzie Eva Gianna Luna Katherine Hazel Khloe Ruby Melanie Piper Lydia Aubree Madeline Aurora Faith Alexandra Alice Kayla Jasmine Maria Annabelle Lauren Reagan Elena Rylee Isabelle Bailey Eliana Sydney Makayla Cora Morgan Natalia Kimberly Vivian Quinn Valentina Andrea Willow Clara London Jade Liliana Jocelyn Kinsley Trinity Brielle Mary Molly Hadley Delilah Emilia Josephine Brooke Ivy Lilly Adeline Payton Lyla Isla Jordyn Paige Isabel Mariah Mya Nicole Valeria Destiny Rachel Ximena Emery Everly Sara Angelina Adalynn Kendall Reese Aliyah Margaret Juliana Melody Amy Eden Mckenzie Laila Vanessa Ariel Gracie Valerie Adalyn Brooklynn Gabrielle Kaitlyn Athena Elise Jessica Adriana Leilani Ryleigh Daisy Nova Norah Eliza Rose Rebecca Michelle Alaina Catherine Londyn Summer Lila Jayla Katelyn Daniela Harmony Alana Amaya Emerson Julianna Cecilia Izabella'
    name = name.split()
    return name


def generating_age_list():
    '''Generates a list of age according to normal distribution with
    '''
    age = np.random.normal(loc=25, scale=3, size=1000)
    return age


def creating_countries_list():
    '''Creates a countries list  by split string with countries by space
    '''
    country = 'Russia Germany United_Kingdom France Italy Spain Ukraine Poland Romania Netherlands Belgium Czech_Republic(Czechia) Greece Portugal Sweden Hungary Belarus Austria Serbia Switzerland Bulgaria Denmark Finland Slovakia Norway Ireland Croatia Moldova Bosnia_and_Herzegovina Albania Lithuania North_Macedonia Slovenia Latvia Estonia Montenegro Luxembourg Malta Iceland Andorra Monaco Liechtenstein San_Marino Holy_See'
    country = country.split()
    return country


def records_to_insert_into_table_users():
    ''' Preparing data to insert into table users by creating a list of combining the data of user_id, name, age, country
    '''
    user_id = generating_user_id()
    name = creating_names_list()
    age = generating_age_list()
    country = creating_countries_list()
    records_to_insert_into_table_users = []
    for i in range(1000):
        records_to_insert_into_table_users.append([int(user_id[i]), random.choice(name), int(age[i]), random.choice(country)])  # add all previous data to one list for further inserting records into the table users
    return records_to_insert_into_table_users


def random_date(start, end, duration):
    """Get a time at a proportion of a range of two formatted times.
    start and end should be strings specifying times formatted in the '%Y-%m-%d %H:%M:%S' format (strftime-style),
    giving an interval [start, end].
    The returned times will be in the '%Y-%m-%d %H:%M:%S' format.
    first returned time is session start
    second returned time is session end
    """

    stime = time.mktime(time.strptime(start, '%Y-%m-%d %H:%M:%S'))  # convert a time.struct_time object to time in seconds passed since epoch in local time
    etime = time.mktime(time.strptime(end, '%Y-%m-%d %H:%M:%S'))
    prop = random.random()  # specifies how a proportion of the interval to be taken after start

    session_start = stime + prop * (etime - stime)  # generates a random date from the selected range
    session_end = session_start + 60 * duration  # calculating time of session end by adding to the time of session start duration from session_duration
    session_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(session_start))  # convert a time expressed in seconds since the epoch to a time and then to string by time.strftime
    session_end = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(session_end))
    return session_start, session_end


def generating_session_id(session_amount):
    '''
    Generate a random sample from a given 1-D array without replacement from the specified sequence
    '''
    session_id = np.random.choice(range(10000, 100000), session_amount, replace=False)
    return session_id


def creating_session_user_id(session_num):
    '''Generating a list of use_id according to session_num
    '''
    user_id_session = []
    user_id = generating_user_id()
    for i in range(len(user_id)):
        for k in range(int(session_num[i])+1):
            user_id_session.append(user_id[i])
    return user_id_session


def records_to_insert_into_table_sessions():
    ''' Preparing data to insert into table sessions by creating a list of combining
    the data of session_id, user_id_session, session_start, session_end by random_date()
    '''
    session_num = np.random.poisson(lam=1, size=1000)  # generate list of session duration according to poisson distribution
    session_amount = 0
    for i in session_num:
        i += 1
        session_amount += i
    session_duration = np.random.normal(loc=10, scale=2, size=session_amount)  # generate list of session duration according to normal distribution
    session_user_id = creating_session_user_id(session_num)
    session_id = generating_session_id(session_amount)
    records_to_insert_into_table_sessions = []
    for i in range(session_amount):
        session_start, session_end = random_date("2021-06-14 00:00:00", "2021-06-20 00:00:00", int(session_duration[i]))
        records_to_insert_into_table_sessions.append([int(session_id[i]), int(session_user_id[i]), session_start, session_end])
    return records_to_insert_into_table_sessions


def main():
    creating_a_database()
    creating_a_table_users()
    creating_a_table_sessions()   
    users = records_to_insert_into_table_users()
    insert_multiple_records_into_table('users', '(user_id, name, age, country)', users)
    sessions = records_to_insert_into_table_sessions()
    insert_multiple_records_into_table('sessions', '(session_id, user_id, session_start, session_end)', sessions)


if __name__ == '__main__':
    main()
