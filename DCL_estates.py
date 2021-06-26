
import requests
import json
from gql import gql, Client
import pandas as pd
import time
from datetime import datetime
from time import sleep 
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd

# Select your transport with a defined url endpoint
transport = RequestsHTTPTransport(url="https://api.thegraph.com/subgraphs/name/decentraland/marketplace")
# Create a GraphQL client using the defined transport
client = Client(transport=transport, fetch_schema_from_transport=True)

mystring = """
query {{
    orders (first: 1000 orderBy: updatedAt, orderDirection: asc where: {{ status:sold category:estate updatedAt_gt:"{0}"}}) {{
        
        id
        category 
        price
        status
        updatedAt
        nft {{
            owner {{
                id
                }}
            name
            tokenURI
            }}
        }}
    }}"""

df = pd.DataFrame()
#update parameter used in mystring to start querying the database at the earliest update date of sale. The update 
#date is specified in epoch date and needs to be converted to datetime for human consumption.
update = 1

while True:
    #query the data using GraphQL python library.
    query = gql(mystring.format(update))
    result = client.execute(query)
    #if there is no data returned it means you reached the end and should stop querying.
    if len(client.execute(query)['orders']) <= 1:
        break
    
    else:
        #Create a temporary dataframe to later append to the final dataframe that compiles all 1000-row dataframes.
        df1 = pd.json_normalize(result['orders'])

        # #append your temp dataframe to your master dataset.
        df = df.append(df1)
    
        #Pass into the API the max date from your dataset to fetch the next 1000 records.
        update = df['updatedAt'].max()

#reformat the update date in human-readable datetime format.
df['updatedAt_dt'] = df['updatedAt'].apply(lambda x: time.strftime('%Y-%m-%d', time.localtime(int(x))) )
df1['updatedAt_dt'] = df1['updatedAt'].apply(lambda x: time.strftime('%Y-%m-%d', time.localtime(int(x))) )

df.price = df.price.astype(float)
df.price = df.price.round()
df['date'] = pd.to_datetime(df["updatedAt_dt"])

estate = df
estate['date'] = pd.to_datetime(df["updatedAt_dt"])
estate = estate.set_index('date')
estate.to_csv("estate_data.csv")

# estate = estate[['price']]
# estate = estate.tail(5)

# import influxdb_client
# from datetime import datetime
# from influxdb_client import InfluxDBClient, Point, WritePrecision
# from influxdb_client.client.write_api import SYNCHRONOUS

# token = 'Token Hash'
# org = "My Org"
# bucket = "My Bucket"
# destination="The URL of the DB"

# client = influxdb_client.InfluxDBClient(url=destination,token=token,org=org)

# write_api = client.write_api()

# write_api.write(bucket=bucket, org=org, record=estate,
#                 data_frame_measurement_name='price_sold',
#                 data_frame_tag_columns=['Estates'])
    
    
    
'''
#PARCELS
total_sold_parcels = sum(parcel['price'])
annual_parcel_price = parcel.groupby(pd.Grouper(freq='Y'))['price'].sum().reset_index()
monthly_parcel_price = parcel.groupby(pd.Grouper(freq='M'))['price'].sum().reset_index()
daily_parcel_price = parcel.groupby(pd.Grouper(freq='D'))['price'].sum().reset_index()
avg_annual_parcel_price = parcel.groupby(pd.Grouper(freq='Y'))['price'].mean().reset_index()
avg_monthly_parcel_price = parcel.groupby(pd.Grouper(freq='M'))['price'].mean().reset_index()
avg_daily_parcel_price = parcel.groupby(pd.Grouper(freq='D'))['price'].mean().reset_index()

total_transactions_parcels = parcel.category.count()
annual_parcel_transactions = parcel.groupby(pd.Grouper(freq='Y'))['category'].count().reset_index()
monthly_parcel_transactions = parcel.groupby(pd.Grouper(freq='M'))['category'].count().reset_index()
daily_parcel_transactions = parcel.groupby(pd.Grouper(freq='D'))['category'].count().reset_index()


#ESTATES
total_sold_estates = sum(estate['price'])
annual_estates_price = estate.groupby(pd.Grouper(freq='Y'))['price'].sum().reset_index()
monthly_estates_price = estate.groupby(pd.Grouper(freq='M'))['price'].sum().reset_index()
daily_estates_price = estate.groupby(pd.Grouper(freq='D'))['price'].sum().reset_index()
avg_annual_estates_price = estate.groupby(pd.Grouper(freq='Y'))['price'].mean().reset_index()
avg_monthly_estates_price = estate.groupby(pd.Grouper(freq='M'))['price'].mean().reset_index()
avg_daily_estates_price = estate.groupby(pd.Grouper(freq='D'))['price'].mean().reset_index()

total_transactions_estates = estate.category.count()
annual_estates_transactions = estate.groupby(pd.Grouper(freq='Y'))['category'].count().reset_index()
monthly_estates_transactions = estate.groupby(pd.Grouper(freq='M'))['category'].count().reset_index()
daily_estates_transactions = estate.groupby(pd.Grouper(freq='D'))['category'].count().reset_index()


#TOTAL LAND
total_sold_land = total_sold_parcels + total_sold_estates
annual_land_price = annual_parcel_price.price + annual_estates_price.price
annual_land_price = annual_parcel_price.set_index('date')
monthly_land_price = monthly_parcel_price.price + monthly_estates_price.price
monthly_land_price = monthly_parcel_price.set_index('date')
daily_land_price = daily_parcel_price.price + daily_estates_price.price
daily_land_price = daily_parcel_price.set_index('date')
avg_annual_land_price = avg_annual_parcel_price.price + avg_annual_estates_price.price
avg_monthly_land_price = avg_monthly_parcel_price.price + avg_monthly_estates_price.price
avg_daily_land_price = avg_daily_parcel_price.price + avg_daily_estates_price.price

total_transactions_land = total_transactions_parcels + total_transactions_estates
annual_land_transactions = annual_parcel_transactions.category + annual_estates_transactions.category
annual_land_transactions = annual_parcel_transactions.set_index('date')
monthly_land_transactions = monthly_parcel_transactions.category + monthly_estates_transactions.category
monthly_land_transactions = monthly_parcel_transactions.set_index('date')
daily_land_transactions = daily_parcel_transactions.category + daily_estates_transactions.category
daily_land_transactions = daily_parcel_transactions.set_index('date')
'''    