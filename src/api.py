import requests
import pandas as pd
from datetime import datetime, timedelta
import pandas as pd

df=pd.read_csv(r'C:\Users\LENOVO\OneDrive\Desktop\OPTION_CHAIN_ETL_PROJECT\complete.csv')


# Function to fetch historical data for a given time period
def fetch_historical_data(symbol, start_date, end_date):
    url = f'https://api.upstox.com/v2/historical-candle/{symbol}/1minute/{end_date}/{start_date}'
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json().get('data', [])
        return pd.DataFrame(data)
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

# Function to fetch instrument_keys based on a list of names using boolean indexing
def get_instrument_keys(df, names):
    instrument_keys = {}

    for name in names:
        mask = df['tradingsymbol'] == name
        if mask.any():
            instrument_key = df.loc[mask, 'instrument_key'].iloc[0]
            instrument_keys[name.lower()] = instrument_key
        else:
            print(f"No instrument_key found for name: {name}")

    return instrument_keys


