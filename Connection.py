import mysql.connector
import pandas as pd
from mysql.connector import Error

import constants
from constants import *

#For starting with this project is necessary to setup first the MYSQL int your local machine
#Follow the steps on this video, and you will be fine to go on:
#https://www.youtube.com/watch?v=2HQC94la6go&ab_channel=BlaineRobertson

#After doing that, create a "constants" class, to contains two attributes, it being:
#DATABASE_NAME = 'The name of the DATABASE that you will use'
#SERVER_PASS = 'The DATABASSE connection password'



#Returns the header of eacch DataFrame, to complement the data request, and create a new DataFrame
#possibilities: 'coin_info', 'coin_ranking'
def get_dataframe_header(df_name):
    if df_name == 'coin_info':
        return ['id', 'symbol', 'name']
    elif df_name == 'coin_ranking':
        return ['id', 'current_price', 'market_cap',
                'market_cap_rank', 'fully_diluted_valuation', 'total_volume',
                'high_24h', 'low_24h', 'price_change_24h', 'price_change_percentage_24h',
                'market_cap_change_24h', 'market_cap_change_percentage_24h', 'circulating_supply',
                'total_supply', 'max_supply', 'ath', 'ath_date',
                'atl', 'atl_date', 'last_updated']

#Creates a new object, responsable to connect the server
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

#Creates a new DATABASE
#This function will be executed only once, in the project
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

#This will be the function to be called to execute the queries along the project
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

#Function to connect the Database created and than, than allowing to create and read queries.
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

#Connects to the dataframe and runs a reading query
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

#Create the DATABASE.
#Run on only once.
#Connects to MySQL server running locally
#connection = create_server_connection("localhost", "root", constants.SERVER_PASS)
#Query to create database
#create_database_query = "CREATE DATABASE DATABASE_NAME"
#create_database(connection, create_database_query)

create_coin_info = """
CREATE TABLE coin_info(
  `id` VARCHAR(100), 
  `symbol` VARCHAR(15), 
  `name` VARCHAR(50),
   PRIMARY KEY (`ID`));
 """

create_coin_ranking_table = """
CREATE TABLE coin_ranking(
  `id` VARCHAR(50), 
  `current_price` FLOAT NOT NULL, 
  `market_cap`FLOAT NOT NULL,
  `market_cap_rank` FLOAT NOT NULL, 
  `fully_diluted_valuation` FLOAT NOT NULL, 
  `total_volume` FLOAT NOT NULL,
  `high_24h` FLOAT NOT NULL, 
  `low_24h` FLOAT NOT NULL, 
  `price_change_24h` FLOAT NOT NULL, 
  `price_change_percentage_24h` FLOAT NOT NULL,
  `market_cap_change_24h` FLOAT NOT NULL, 
  `market_cap_change_percentage_24h` FLOAT NOT NULL, 
  `circulating_supply` FLOAT NOT NULL,
  `total_supply` FLOAT NOT NULL, 
  `max_supply` FLOAT , 
  `ath` FLOAT NOT NULL, 
  `ath_date` DATETIME, 
  `atl` FLOAT NOT NULL, 
  `atl_date` DATETIME,
  `date` DATETIME,
  PRIMARY KEY(`id`, `date`),
  FOREIGN KEY (`id`)
        REFERENCES coin_info(ID) 
);
"""

#Create coins table
#connection = create_db_connection("localhost", "root", constants.SERVER_PASS, constants.DATABASE_NAME)
#execute_query(connection, create_coin_info)
#execute_query(connection, create_coin_ranking_table)

#Passes a new Dataframe as parameter, populated with all queries listeds in coin_gecko
#(usually using thecoin_gecko_data.get_coin_list() function)
def insert_coin_info(coin_info_df):
    connection = create_db_connection("localhost", "root", constants.SERVER_PASS, constants.DATABASE_NAME)

    for index, row in coin_info_df.iterrows():
        if int(select_single_coin_info(row['id']).shape[0]) == 0:
            pop_coin_info_query = f'INSERT INTO coin_info VALUES(' \
                                  f'"{row["id"]}",' \
                                  f'"{row["symbol"]}",' \
                                  f'"{row["name"]}");'
            execute_query(connection, pop_coin_info_query)
            #print(pop_coin_info_query)

#Insert a new top-n-coins listed in coin_gecko
def insert_coin_rank(coin_pandas_df):
    connection = create_db_connection("localhost", "root", constants.SERVER_PASS, constants.DATABASE_NAME)

    for index, row in coin_pandas_df.iterrows():
        pop_lists_query = f'INSERT INTO coin_ranking VALUES (' \
                          f'"{row["id"]}",' \
                          f'{row["current_price"]},' \
                          f'{row["market_cap"]},' \
                          f'{row["market_cap_rank"]},' \
                          f'{row["fully_diluted_valuation"]},' \
                          f'{row["total_volume"]},' \
                          f'{row["high_24h"]},' \
                          f'{row["low_24h"]},' \
                          f'{row["price_change_24h"]},' \
                          f'{row["price_change_percentage_24h"]},' \
                          f'{row["market_cap_change_24h"]},' \
                          f'{row["market_cap_change_percentage_24h"]},' \
                          f'{row["circulating_supply"]},' \
                          f'{row["total_supply"]},' \
                          f'{row["max_supply"]},' \
                          f'{row["ath"]},' \
                          f'"{row["ath_date"]}",' \
                          f'{row["atl"]},' \
                          f'"{row["atl_date"]}",' \
                          f'"{row["last_updated"]}");'
        print(pop_lists_query)

        execute_query(connection, pop_lists_query)
        print('Row insertion successfully')

#returns all coin_information inside the coin_info table
def select_single_coin_info(coin_id):
    connection = create_db_connection("localhost", "root", constants.SERVER_PASS, constants.DATABASE_NAME)

    search_coin_query = f'select * from coin_info where id="{coin_id}"'

    results = read_query(connection, search_coin_query)

    header = get_dataframe_header('coin_info')

    df = pd.DataFrame(data=results, columns=header)
    return df

#Receives a coin_ids list containing multiple coin ids, selects all the coins information inside the coin_id database
#related to those coins.
def select_multiple_rankings(coin_ids):
    connection = create_db_connection("localhost", "root", constants.SERVER_PASS, constants.DATABASE_NAME)

    results = []

    for coin_id in coin_ids:
        search_coin_query = f'select * from coin_ranking where id="{coin_id}"'
        result = read_query(connection, search_coin_query)
        results += result

    header = get_dataframe_header()
    df = pd.DataFrame(data=results, columns=header)
    return df

#Selects the last top-250 rank registred in the database
def select_last_rank_update():
    connection = create_db_connection("localhost", "root", constants.SERVER_PASS, constants.DATABASE_NAME)

    search_last_ranking_query = f'select * from coin_ranking' \
                                f' order by date desc, market_cap desc ' \
                                f'limit 250'

    result = read_query(connection, search_last_ranking_query)

    header = get_dataframe_header('coin_ranking')
    df = pd.DataFrame(data=result, columns=header)

    return df

#returns all ids inside the coin_info table
def select_all_ids():
    connection = create_db_connection("localhost", "root", constants.SERVER_PASS, constants.DATABASE_NAME)

    search_coin_query = f'select id from coin_info;'

    results = read_query(connection, search_coin_query)

    all_ids = []

    for res in results:
        all_ids.append(res[0].replace(',', ''))

    return all_ids
