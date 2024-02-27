import tools.conf
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np


def add_one_field(data, table_name, field_name):
    query = sql.SQL("INSERT INTO {table} ({field}) VALUES (%s)")

    for elem in data:
        cur.execute(
            query.format(
                table=sql.Identifier(f'{table_name}'),
                field=sql.Identifier(f'{field_name}')), (elem,)
        )
    conn.commit()
    
def iter_over_inputs(data_set):
    
    for elem in data_set:
        data = elem['data']
        table = elem['table']
        field = elem['field']
        add_one_field(data, table, field)

def add_3_fields(data_set):
    col = data_set['data'].columns.values.tolist()
    
    query = sql.SQL("INSERT INTO {table} ({field1}, {field2}, {field3}) VALUES (%s, %s ,%s)")

    for _, elem in data_set['data'].iterrows():
        cur.execute(
            query.format(
                table=sql.Identifier(data_set['table']),
                field1=sql.Identifier(data_set['fields'][0]),
                field2=sql.Identifier(data_set['fields'][1]),
                field3=sql.Identifier(data_set['fields'][2])), 
                (elem[col[0]], elem[col[1]], elem[col[2]],)
        )
        
    conn.commit()

def get_id_for_submediums():
    submediums = df[['submedium', 'wydawca_nadawca', 'zasięg medium']].sort_values(by='submedium')
    submediums.drop_duplicates(subset=['submedium'], keep='first', inplace=True, ignore_index=True)
    submediums.index = submediums.index + 1
    
    if sum(submediums.value_counts()) != submediums.index.max():
        exit('Max index inni niż długość listy.')
    
    query1 = sql.SQL('SELECT {fields} FROM {table}').format(
    fields=sql.SQL(',').join([
        sql.Identifier('broadcaster'),
        sql.Identifier('id')
    ]),
    table=sql.Identifier('broadcasters'))
    cur.execute(query1)
    broadcasters = dict(cur.fetchall())

    query2 = sql.SQL('SELECT {fields} FROM {table}').format(
        fields=sql.SQL(',').join([
            sql.Identifier('reach'),
            sql.Identifier('id')
        ]),
        table=sql.Identifier('ad_reach'))
    cur.execute(query2)
    ad_reach = dict(cur.fetchall())

    submediums['wydawca_nadawca'] = submediums['wydawca_nadawca'].map(broadcasters)
    submediums['zasięg medium'] = submediums['zasięg medium'].map(ad_reach)
    
    return submediums


        

# Reads the dataframe
df = pd.read_csv('./data/baza.csv', delimiter=';', thousands=',', dtype={'dł_ujednolicona': 'object'}, encoding='utf-8')
df.sort_values(by='data', axis=0, inplace=True)
df.reset_index(inplace=True)
df.drop('index', axis=1, inplace=True)


dow2 = ['Poniedziałek', 'Wtorek', 'Środa', 'Czwartek', 'Piątek',
        'Sobota', 'Niedziela']
months = [
    'Styczeń', 'Luty', 'Marzec', 'Kwiecień', 'Maj',
    'Czerwiec', 'Lipiec', 'Sierpień', 'Wrzesień',
    'Październik', 'Listopad', 'Grudzień'
]
dates = df['data'].unique()
brands = df['brand'].sort_values().unique()
lengths = df['dł_ujednolicona'].sort_values().unique()
dayparts = df['daypart'].unique()
product_types = df['produkt(4)'].sort_values().unique()
broadcasters = df['wydawca_nadawca'].sort_values().unique()
reaches = df['zasięg medium'].unique()

data_set = [{'data': dow2, 'table': 'pl_dow_names', 'field': 'dow_name'},
            {'data': months, 'table': 'pl_month_names', 'field': 'month_name'},
            {'data': dates, 'table': 'date_time', 'field': 'date'},
            {'data': brands, 'table': 'brands', 'field': 'brand'},
            {'data': lengths, 'table': 'unified_lenghts', 'field': 'length'},
            {'data': dayparts, 'table': 'dayparts', 'field': 'daypart'},
            {'data': product_types, 'table': 'product_types', 'field': 'product_type'},
            {'data': broadcasters, 'table': 'broadcasters', 'field': 'broadcaster'},
            {'data': reaches, 'table': 'ad_reach', 'field': 'reach'},
            ]

# Openes connection to the DB
conn = psycopg2.connect(
    f'''dbname={tools.conf.DB}
        user={tools.conf.USER}
        host={tools.conf.HOST}
        port={tools.conf.PORT}
    '''
)

cur = conn.cursor()

iter_over_inputs(data_set)

submediums = get_id_for_submediums()
data_set2 = [{'data': submediums, 'table': 'mediums', 'fields': ['submedium', 'broadcaster_id', 'ad_reach_id']}]
add_3_fields(data_set2)


conn.close()
