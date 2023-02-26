import Connection
import coin_gecko_data
import dash
from dash import dcc, dash_table, html
import pandas as pd
import dash_bootstrap_components as dbc
from app import *
from coin_gecko_data import *
import plotly.graph_objects as go

### LAYOUT -------------------
app.layout = dbc.Container(children=[
    dbc.Row([
        dbc.Row([
            html.H4('Cryptocurrencies with highest price variation'),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5('Lower price variation'),
                        html.Div(id='table_low_container')
                    ])
                ])
            ],lg=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5('Higher price variation'),
                        html.Div(id='table_high_container')
                    ])
                ])
            ], lg=6)
        ], style={'max-height': '400px'}),
        dbc.Row([
            html.Div([
                html.H4('Coin information: '),
                dcc.Dropdown(Connection.select_all_ids(),
                             'bitcoin',
                             optionHeight=50,
                             style={
                                 'background-color': '#000000',
                                 'color': '#1a237e'
                             },
                             id='coin_id_select',
                             multi=False),
            ]),

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='ohlc_graph')
                    ])
                ])
            ], lg=6),

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='mkt_cap_graph')
                    ])
                ])
            ], lg=6),

        ], style={"margin-top": '15px'})
    ]),
    dcc.Interval(id='interval-component', interval=10 * 60 * 1000, n_intervals=0)
],  style={"padding": "15px"}, fluid=True)

### DATA PREPARATION -----------------
def compare_ranks(old_df, live_df):
    old_df = old_df[['rank', 'name']].copy()
    old_df.rename(columns={'rank': 'old_rank'}, inplace=True)

    live_df = live_df[['rank', 'name']].copy()
    live_df.rename(columns={'rank': 'new_rank'}, inplace=True)

    comparsion_df = pd.merge(old_df, live_df, how='outer', on='id')
    comparsion_df['rank_dif'] = comparsion_df['old_rank'] - comparsion_df['new_rank']

    return comparsion_df

#Receives 2 DataFrames as parameters
#Joins both and compares the prices of each, (live_price - old_price)
#returns the difference
def compare_price(old_df, live_df):
    old_df = old_df[['id', 'current_price']].copy()
    old_df.rename(columns={'current_price': 'old_price'}, inplace=True)

    live_df = live_df[['id', 'current_price']].copy()
    live_df.rename(columns={'current_price': 'new_price'}, inplace=True)

    comparsion_df = pd.merge(old_df, live_df, how='outer', on='id')
    comparsion_df['price_dif'] = comparsion_df['new_price'] - comparsion_df['old_price']
    comparsion_df['price_dif_percent'] = comparsion_df['price_dif'] / comparsion_df['old_price']

    return comparsion_df

#returns two DataFrames.
#1 is the last updated data from Coin Gecko API
# 2 is the last written data in the Database (each 10 minutes a new top-250 is written in)
def get_dataframes():
    #Get last ranking registred in the DataBase
    last_update = Connection.select_last_rank_update()
    #connecst to coinmarketcap API and get the current 500 top coins listed in
    online_rank = coin_gecko_data.get_top_n_coins(250, 'usd')

    return last_update, online_rank

#checks if all the top-250 coins ids are into the database coin_info;
#if not, inserts it in
def update_coinInfo():
    #Get all the listed coins in CoinGecko
    coin_info_data = coin_gecko_data.get_coin_list()
    #Create a new Dataframe with the data
    df_coin_info = pd.DataFrame(data=coin_info_data, columns=Connection.get_dataframe_header('coin_info'))
    #Inserts the coins informations in the coin_info table into the database
    Connection.insert_coin_info(df_coin_info)


def update_data():
    last_update, online_rank = get_dataframes()

    #Compares the live time and the registred time.
    sample_data_live = online_rank['last_updated'][0]
    sample_data_onDb = last_update['last_updated'][0]
    data_dif = (sample_data_live - sample_data_onDb).seconds /60
    #If has been more than 9 minutes since the last database update, than save it again
    if(data_dif >= 10):
        Connection.insert_coin_rank(online_rank)
        update_coinInfo()

    #Compares the prices between live-data and written data
    price_comparsion = compare_price(last_update, online_rank)
    price_comparsion = price_comparsion[['id', 'old_price', 'new_price',
                                         'price_dif', 'price_dif_percent']]
    #Selects prices variations above 1% and below -1%
    high_price_moviment = price_comparsion[price_comparsion['price_dif_percent'] > 0.01].sort_values('price_dif_percent', ascending=False)
    low_price_moviment = price_comparsion[price_comparsion['price_dif_percent'] < -0.01].sort_values('price_dif_percent', ascending=True)

    #Passes the data abova to two differents dash.DataTable()
    price_low = dash_table.DataTable(
        id='price_low_table',
        columns=[{"name": i, "id": i} for i in low_price_moviment[:5].columns],
        data=high_price_moviment.to_dict('records'),
        style_data_conditional=[
            {
                "if": {"row_index": "odd"},
                "backgroundColor": "#343a40",
                "color": "white"
            }
        ],
        style_header={
            "backgroundColor": "#343a40",
            "color": "white",
            "fontWeight": "bold"
        },
        style_cell = {
            "color": "black"
        },
    )

    price_high = dash_table.DataTable(
        id='price_high_table',
        columns=[{"name": i, "id": i} for i in high_price_moviment[:5].columns],
        data=low_price_moviment.to_dict('records'),
        style_data_conditional=[
            {
                "if": {"row_index": "odd"},
                "backgroundColor": "#343a40",
                "color": "white"
            }
        ],
        style_header={
            "backgroundColor": "#343a40",
            "color": "white",
            "fontWeight": "bold"
        },
        style_cell = {
                     "color": "black"
                 },
    )
    return price_low, price_high

### CALLBACKS ---------
@app.callback(
    [dash.dependencies.Output('table_low_container', 'children'),
     dash.dependencies.Output('table_high_container', 'children'),
     dash.dependencies.Output('ohlc_graph', 'figure'),
     dash.dependencies.Output('mkt_cap_graph', 'figure')],
    [dash.dependencies.Input('coin_id_select', 'value'),
     dash.dependencies.Input('interval-component', 'n_intervals')])

def update_tables(coin_id, n):
    #dash.DataTables() created on update_data()
    table1, table2 = update_data()

    #Create plotting objects and returns it into the callbacks

    ohlc_data = coin_gecko_data.get_historical_ohlc(coin_id)
    ohcl_graph = go.Figure(data=go.Candlestick(x=ohlc_data['time'],
                                        open=ohlc_data['open'],
                                        high=ohlc_data['high'],
                                        low=ohlc_data['low'],
                                        close=ohlc_data['close']))
    ohcl_graph.update_layout(template='plotly_dark',
                             title='OHLC Data',
                             title_x=0.5)

    mkt_cap_data = coin_gecko_data.get_historical_mkt_cap(coin_id)
    mkt_cap_graph = go.Figure()
    mkt_cap_graph.add_trace(go.Scatter(x=mkt_cap_data['time'],
                                       y=mkt_cap_data['market_cap'],
                                       fill='tozeroy'))
    mkt_cap_graph.update_layout(template='plotly_dark',
                                title='Market Captalization',
                                title_x=0.5)

    return table2, table1, ohcl_graph, mkt_cap_graph

### EXECUTE ----------
if __name__ == '__main__':
    app.run_server(debug=True)
