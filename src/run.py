from datetime import datetime,timedelta
from src.api import fetch_historical_data
import pandas as pd
df=pd.read_csv(r'C:\Users\LENOVO\OneDrive\Desktop\OPTION_CHAIN_ETL_PROJECT\ind_nifty50list.csv')

end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')
for instrument_key in df['instrument_key']:

    df_current_period = fetch_historical_data(instrument_key, start_date, end_date)
    print(df_current_period)
    break

