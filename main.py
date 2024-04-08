import asyncio
import concurrent.futures
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import gspread
from oauth2client.service_account import ServiceAccountCredentials

GOOGLE_TABLE_NAME = 'Data Analysis'

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name('CloudDemo_JJ.json', scope)
Sheets_client = gspread.authorize(credentials)

credentials = service_account.Credentials.from_service_account_file('CloudDemo_JJ.json')
Bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

dataset_id = "bigquery-public-data.google_analytics_sample"
table_id = "ga_sessions_*"
fields = ["date", "trafficSource.source", "geoNetwork.country", "totals.pageviews", "totals.bounces"]

async def get_data(start_date, end_date):
    query = f"""
        SELECT {", ".join(fields)}
        FROM `{dataset_id}.{table_id}`
        WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
    """
    df = Bigquery_client.query(query).to_dataframe()
    df['date'] = pd.to_datetime(df['date'])
    return df

async def save_data(df, sheet_name):
    worksheet = Sheets_client.open(GOOGLE_TABLE_NAME).add_worksheet(title=sheet_name, rows=df.shape[0], cols=df.shape[1])
    worksheet.update([df.columns.tolist()] + df.values.tolist())

def view_by_source(df):
    sheet_name = 'View by Source'
    df_source = df.groupby('source', as_index=False).agg({'pageviews': 'sum'})
    return df_source, sheet_name

def bounces_by_country(df):
    sheet_name = 'Bounces by Country'
    df_country = df.groupby('country', as_index=False).agg({'bounces': 'sum'})
    return df_country, sheet_name

def source_by_country(df):
    sheet_name = 'Source_by Country'
    df_country = pd.pivot_table(df, index="country", columns="source", values="pageviews", aggfunc="count")
    df_country.fillna(0, inplace=True)
    df_country.reset_index(inplace=True)
    return df_country, sheet_name

async def main():
    
    times = [('2016-08-01', '2016-09-12'), ('2016-10-24', '2016-12-22'), ('2017-08-20', '2017-09-18')]

    tasks = [
        asyncio.create_task(get_data(time[0].replace('-', ''), time[1].replace('-', ''))) for time in times
    ]
    results = await asyncio.gather(*tasks)

    df = pd.concat(results)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(view_by_source, df),
            executor.submit(bounces_by_country, df),
            executor.submit(source_by_country, df),
            ]
        aggregated_results = [future.result() for future in futures]

    tasks = [
        asyncio.create_task(save_data(*agg_result)) for agg_result in aggregated_results
    ]
    results = await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
    
