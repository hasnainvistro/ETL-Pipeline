import pandas as pd 
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np 

csv_path = 'Countries_by_GDP.csv'
log_file = "log_file.txt"
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_name = 'Countries_by_GDP'
db_name = 'World_Economies.db'
attributes_list = ["Country","GDP_USD_millions"]



def extract(url, attributes_list):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns = attributes_list)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—'not in col[2]:
                    data_dict = {"Country":col[0].find('a').text.strip(),
                                "GDP_USD_millions":col[2].contents[0]}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df,df1], ignore_index=True)
                    df['Country'] = df['Country'].astype(str)
                    

    return df

def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df["GDP_USD_millions"] = df["GDP_USD_millions"].astype(float)
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, conn, table_name):
    #conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists = 'replace', index = False)
    #conn.close()

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, conn)
    print(query_output)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open (log_file, "a") as f:
        f.write(timestamp + ' ' + message + '\n')


log_progress("ETL Job Started")

log_progress("Extract phase Started")
df = extract(url, attributes_list)
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
df = transform(df)
print("Transformed Data")
print(df)
log_progress("Tranform phase Ended")

log_progress("Load phase Started")
load_to_csv(df, csv_path)

conn = sqlite3.connect(db_name)
load_to_db(df, conn, table_name)
log_progress("Load phase Ended")

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement,conn)

log_progress("ETL job ended")

conn.close()