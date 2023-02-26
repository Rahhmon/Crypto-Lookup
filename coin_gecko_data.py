import requests
import json
import pandas as pd
from datetime import datetime

import Connection

def parse_timestamp_to_date(timestamp_with_ms):
    timestamp, ms = divmod(timestamp_with_ms, 1000)
    return pd.to_datetime(datetime.fromtimestamp(timestamp))

#Receives two parameters n_coins, currency to retrieve all top-listed coins by market-cap value
#n_coins: the number of top-coins (max 250)
#currency: the currency to compare the coin value (i.e 1'btc' : 22.000'usd')
def get_top_n_coins(n_coins, currency):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": currency,
        "order": "market_cap_desc",
        "per_page": n_coins,
        "page": 1
    }

    response = requests.get(url, params=params)

    data = response.json()

    # Creates a empty array who will keep each coin information
    coin_ranking_data = []
    # Selects each coin important information
    for coin in data:
        coin_ranking_data.append({
            'id': coin['id'],
            'current_price': coin['current_price'],
            'market_cap': coin['market_cap'],
            'market_cap_rank': coin['market_cap_rank'],
            'fully_diluted_valuation': coin['fully_diluted_valuation'],
            'total_volume': coin['total_volume'],
            'high_24h': coin['high_24h'],
            'low_24h': coin['low_24h'],
            'price_change_24h': coin['price_change_24h'],
            'price_change_percentage_24h': coin['price_change_percentage_24h'],
            'market_cap_change_24h': coin['market_cap_change_24h'],
            'market_cap_change_percentage_24h': coin['market_cap_change_percentage_24h'],
            'circulating_supply': coin['circulating_supply'],
            'total_supply': coin['total_supply'],
            'max_supply': coin['max_supply'],
            'ath': coin['ath'],
            'ath_date': coin['ath_date'],
            'atl': coin['atl'],
            'atl_date': coin['atl_date'],
            'last_updated': coin['last_updated']
        })
    # Creates a new Dataframe with that data
    df_ranking = pd.DataFrame(data=coin_ranking_data, columns=Connection.get_dataframe_header('coin_ranking'))
    df_ranking = parse_coinGeckoTimestamp_to_pandasTimeStamp(df_ranking)
    df_ranking = df_ranking.fillna(0)

    return df_ranking

#Gets the list of all coins listeds in the CoinGecko API
def get_coin_list():
    url = "https://api.coingecko.com/api/v3/coins/list"

    response = requests.get(url)

    if response.status_code == 200:
        coins_list = response.json()
        return coins_list
    else:
        print("Error: Could not retrieve coins list")

#When retrieve data from coingecko API, they come in a DATE-'T'-TIME FORMAT
#This function will extract the date, time and convert those into a Pandas datetime format
def parse_str_to_date(date_str):
    date_ = date_str.split('T')[0]
    time_ = date_str.split('T')[1][:8]
    date_time = date_ + ' ' + time_

    date_time = pd.to_datetime(date_time)
    return date_time

#Receives a pandas DataFrame as parameter and parse all the coingecko timestamp to pandas.to_datetime
#Using the function above
def parse_coinGeckoTimestamp_to_pandasTimeStamp(df_coin_ranking):
    df_coin_ranking['last_updated'] = df_coin_ranking.apply(lambda x: parse_str_to_date(x['last_updated']),axis=1)
    df_coin_ranking['ath_date'] = df_coin_ranking.apply(lambda x: parse_str_to_date(x['ath_date']),axis=1)
    df_coin_ranking['atl_date'] = df_coin_ranking.apply(lambda x: parse_str_to_date(x['atl_date']),axis=1)

    return df_coin_ranking

#Uses the API to retrieve a OPEN-HIGH-LOW-CLOSE financial data information over a coin-id
def get_historical_ohlc(coin_id):
    # CoinGecko API endpoint
    url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency=usd&days=30'
    # Make a request to the CoinGecko API
    response = requests.get(url)

    # Parse the response data into a Pandas DataFrame
    df = pd.DataFrame(response.json(), columns=['time', 'open', 'high', 'low', 'close'])

    # Convert the Unix timestamp to a datetime object
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    return df

#Uses the API to retrieve the market capitalization data information over a coin-id
def get_historical_mkt_cap(coin_id):
    # CoinGecko API endpoint
    url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days=30&interval=daily'

    # Make a request to the CoinGecko API
    response = requests.get(url)

    # Parse the response data into a Pandas DataFrame
    data = response.json()
    df = pd.DataFrame(data['market_caps'], columns=['time', 'market_cap'])

    # Convert the Unix timestamp to a datetime object
    df['time'] = pd.to_datetime(df['time'], unit='ms')

    return df