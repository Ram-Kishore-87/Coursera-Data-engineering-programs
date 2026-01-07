import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3

# ----------------------------------------------------
# Logging Function
# ----------------------------------------------------
def log_progress(message):
    """Logs the progress of the ETL process"""
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    timestamp = datetime.now().strftime(timestamp_format)
    with open('code_log.txt', 'a') as f:
        f.write(f"{timestamp} : {message}\n")

# ----------------------------------------------------
# Extract Function
# ----------------------------------------------------
def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')

    table = soup.find_all('table', class_='wikitable')[0]
    rows = table.find_all('tr')

    data = []

    for row in rows:
        cols = row.find_all('td')
        if len(cols) > 2 and '-' not in cols[2].text:
            data.append({
                "Name": cols[1].get_text(strip=True),
                "MC_USD_Billion": float(cols[2].text.replace(',', ''))
            })

    df = pd.DataFrame(data, columns=table_attribs)
    return df

# ----------------------------------------------------
# Transform Function
# ----------------------------------------------------
def transform(df, csv_path):
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index('Currency')['Rate'].to_dict()

    df['MC_GBP_Billion'] = df['MC_USD_Billion'].apply(
        lambda x: np.round(x * exchange_rate['GBP'], 2)
    )
    df['MC_EUR_Billion'] = df['MC_USD_Billion'].apply(
        lambda x: np.round(x * exchange_rate['EUR'], 2)
    )
    df['MC_INR_Billion'] = df['MC_USD_Billion'].apply(
        lambda x: np.round(x * exchange_rate['INR'], 2)
    )

    return df

# ----------------------------------------------------
# Load to CSV
# ----------------------------------------------------
def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

# ----------------------------------------------------
# Load to Database
# ----------------------------------------------------
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# ----------------------------------------------------
# Run SQL Query
# ----------------------------------------------------
def run_query(query_statement, sql_connection):
    df = pd.read_sql(query_statement, sql_connection)
    print(df)

# ----------------------------------------------------
# Main ETL Execution
# ----------------------------------------------------
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
output_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

conn = sqlite3.connect(db_name)

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete')

df = transform(df, csv_path)
log_progress('Data transformation complete')

load_to_csv(df, output_path)
log_progress('Data saved to CSV')

load_to_db(df, conn, table_name)
log_progress('Data loaded into database')

queries = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "SELECT Name FROM Largest_banks LIMIT 5"
]

for q in queries:
    print(f"\nExecuting query: {q}")
    run_query(q, conn)

conn.close()
log_progress('ETL process completed successfully')
