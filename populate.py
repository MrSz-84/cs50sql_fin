import tools.conf
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np


# Openes connection to the DB
conn = psycopg2.connect(
    f'''dbname={tools.conf.DB}
        user={tools.conf.USER}
        host={tools.conf.HOST}
        port={tools.conf.PORT}
    '''
)

cur = conn.cursor()

# Reads the dataframe
df = pd.read_csv('./data/baza.csv', delimiter=';')
df.sort_values(by='data', axis=0, inplace=True)
df.reset_index(inplace=True)
df.drop('index', axis=1, inplace=True)


# Commits day of week names into pl_dow_names
dow2 = ['Poniedziałek', 'Wtorek', 'Środa', 'Czwartek', 'Piątek',
        'Sobota', 'Niedziela']

for day in dow2:
    cur.execute(
        sql.SQL("INSERT INTO {} (dow_name) VALUES (%s)")
        .format(sql.Identifier('pl_dow_names')), (day,)
    )
conn.commit()


# Commits months into pl_month_names
months = ['Styczeń', 'Luty', 'Marzec', 'Kwiecień', 'Maj',
          'Czerwiec', 'Lipiec', 'Sierpień', 'Wrzesień',
          'Październik', 'Listopad', 'Grudzień']

query = sql.SQL("INSERT INTO {table} ({field}) VALUES (%s)")

for month in months:
    cur.execute(
        query.format(
            table=sql.Identifier('pl_month_names'),
            field=sql.Identifier('month_name')), (month,)
    )
conn.commit()

# Commits dates into date_time table
dates = df['data'].unique()

query = sql.SQL("INSERT INTO {table} ({field}) VALUES (%s)")

for date in dates:
    cur.execute(
        query.format(
            table=sql.Identifier('date_time'),
            field=sql.Identifier('date')), (date,)
    )
conn.commit()

# Commits brands into brands table.
brands = df['brand'].sort_values().unique()

query = sql.SQL('INSERT INTO {table} ({field}) VALUES(%s)')

for brand in brands:
    cur.execute(
        query.format(
            table=sql.Identifier('brands'),
            field=sql.Identifier('brand')), (brand,)
    )
conn.commit()


# Commits lengths into unified_length table.
lengths = df['dł_ujednolicona'].sort_values().unique()

query = sql.SQL('INSERT INTO {table} ({field}) VALUES (%s)')

for length in lengths:
    cur.execute(
        query.format(
            table=sql.Identifier('unified_lenghts'),
            field=sql.Identifier('length')), (str(length),)
    )
conn.commit()


# Commits dayparts into dayparts table.
dayparts = df['daypart'].unique()

query = sql.SQL('INSERT INTO {table} ({field}) VALUES (%s)')

for daypart in dayparts:
    cur.execute(
        query.format(
            table=sql.Identifier('dayparts'),
            field=sql.Identifier('daypart')), (str(daypart),)
    )
conn.commit()


# Commits products into product_types table.
product_types = df['produkt(4)'].sort_values().unique()

query = sql.SQL('INSERT INTO {table} ({field}) VALUES (%s)')

for product in product_types:
    cur.execute(
        query.format(
            table=sql.Identifier('product_types'),
            field=sql.Identifier('product_type')), (str(product),)
    )
conn.commit()



conn.close()
