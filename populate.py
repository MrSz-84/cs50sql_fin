import tools.conf
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import time


def add_1_field(data, table_name, field_name):
    query = sql.SQL('INSERT INTO {table} ({field}) VALUES (%s)')

    for elem in data:
        cur.execute(
            query.format(
                table=sql.Identifier(f'{table_name}'),
                field=sql.Identifier(f'{field_name}')), (elem,)
        )
    conn.commit()


# def iter_over_inputs(data_set):
    
#     for elem in data_set:
#         data = elem['data']
#         table = elem['table']
#         field = elem['field']
#         add_1_field(data, table, field)

    
def iter_over_inputs(data_set):
    
    for elem in data_set:
        data = elem['data']
        table = elem['table']
        field = elem['field']
        new_data, data = check_for_data_1_field(data, table, field)
        if new_data:
            add_1_field(data, table, field)


def check_for_data_1_field(data_, table_, field_):
    query = sql.SQL('SELECT {field} FROM {table}')
    cur.execute(
        query.format(
            table=sql.Identifier(table_),
            field=sql.Identifier(field_))
    )
    
    in_db = pd.DataFrame([elem[0] for elem in cur.fetchall()])
    in_db.rename(columns={0: field_}, inplace=True)
    
    if len(in_db) == 0:
        return (True, data_)
    else:
        if field_ == 'date':
            in_db['date'] = pd.to_datetime(in_db['date'])
            in_db = list(in_db['date'])
        else: 
            in_db = list(in_db[field_])
        
        if len(in_db) != 0 and table_ in avoid_adding:
            print(f'>>> Not adding to {table_}. No new data found.')
            return (False, None)
        in_df = pd.DataFrame(data_)
        in_df = in_df.rename(columns={0: field_})
        if field_ == 'date':
            in_df['date'] = pd.to_datetime(in_df['date'])
        
        # we check if df contains new data in comparison to DB
        new_data = in_df[~in_df.isin(in_db)].dropna()
        new_data = new_data[field_]
        new_data = list(new_data)
        
        if len(new_data) != 0:
            print(f'>>> Adding to {table_}. New data found.')
            return (True, new_data)
        else:
            return (False, None)

def add_3_fields(data_set):
    col = data_set['data'].columns.values.tolist()
    
    query = sql.SQL('INSERT INTO {table} ({field1}, {field2}, {field3}) VALUES (%s, %s ,%s)')

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

def add_8_fields(data_set):
    col = data_set['data'].columns.values.tolist()
    
    query = sql.SQL(
        '''
        INSERT INTO {table} ({field1}, {field2}, {field3}, {field4}, {field5}, {field6}, {field7}, {field8}) 
        VALUES (%s, %s ,%s, %s ,%s, %s ,%s ,%s)
        ''')

    for _, elem in data_set['data'].iterrows():
        cur.execute(
            query.format(
                table=sql.Identifier(data_set['table']),
                field1=sql.Identifier(data_set['fields'][0]),
                field2=sql.Identifier(data_set['fields'][1]),
                field3=sql.Identifier(data_set['fields'][2]),
                field4=sql.Identifier(data_set['fields'][3]),
                field5=sql.Identifier(data_set['fields'][4]),
                field6=sql.Identifier(data_set['fields'][5]),
                field7=sql.Identifier(data_set['fields'][6]),
                field8=sql.Identifier(data_set['fields'][7])), 
                (elem[col[0]], elem[col[1]], elem[col[2]], 
                 elem[col[3]], elem[col[4]], elem[col[5]], 
                 elem[col[6]], elem[col[7]],
                )
        )
        
    conn.commit()

def add_10_fields(data_set):
    col = data_set['data'].columns.values.tolist()
    
    query = sql.SQL(
        '''
        INSERT INTO {table} ({field1}, {field2}, {field3}, {field4}, {field5}, {field6}, {field7}, {field8}, {field9}, {field10}) 
        VALUES (%s, %s ,%s, %s ,%s, %s ,%s ,%s ,%s ,%s)
        ''')

    for _, elem in data_set['data'].iterrows():
        cur.execute(
            query.format(
                table=sql.Identifier(data_set['table']),
                field1=sql.Identifier(data_set['fields'][0]),
                field2=sql.Identifier(data_set['fields'][1]),
                field3=sql.Identifier(data_set['fields'][2]),
                field4=sql.Identifier(data_set['fields'][3]),
                field5=sql.Identifier(data_set['fields'][4]),
                field6=sql.Identifier(data_set['fields'][5]),
                field7=sql.Identifier(data_set['fields'][6]),
                field8=sql.Identifier(data_set['fields'][7]),
                field9=sql.Identifier(data_set['fields'][8]),
                field10=sql.Identifier(data_set['fields'][9])), 
                (elem[col[0]], elem[col[1]], elem[col[2]], 
                 elem[col[3]], elem[col[4]], elem[col[5]], 
                 elem[col[6]], elem[col[7]], elem[col[8]], 
                 elem[col[9]],
                )
        )
        
    conn.commit()

def get_id_for_submediums():
    submediums = df[['submedium', 'wydawca_nadawca', 'zasięg medium']].sort_values(by='submedium')
    submediums.drop_duplicates(subset=['submedium'], keep='first', inplace=True, ignore_index=True)
    submediums.index = submediums.index + 1
    
    if sum(submediums.value_counts()) != submediums.index.max():
        exit('Max index different than the length of the list.')
    
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


def get_id_for_ad_time():
    ad_time = df[['data', 'godzina_bloku_reklamowego', 'gg', 'mm', 'dl_mod', 'daypart', 'dł_ujednolicona', 'ad_time_details']]
    # ad_time['kod_reklamy'] = df['ad_time_details']
    ad_time.index = ad_time.index + 1

    if sum(ad_time.value_counts()) != ad_time.index.max():
        exit('Max index different than the length of the list.')

    query1 = sql.SQL('SELECT {fields} FROM {table}').format(
        fields=sql.SQL(',').join([
            sql.Identifier('length'),
            sql.Identifier('id')
            ]),
        table=sql.Identifier('unified_lengths'))

    cur.execute(query1)
    unified_lengths = dict(cur.fetchall())

    query2 = sql.SQL('SELECT {fields} FROM {table}').format(
        fields=sql.SQL(',').join([
            sql.Identifier('daypart'),
            sql.Identifier('id')
            ]),
        table=sql.Identifier('dayparts'))

    cur.execute(query2)
    dayparts = dict(cur.fetchall())

    ad_time.loc[:, 'daypart'] = ad_time['daypart'].map(dayparts)
    ad_time.loc[:, 'dł_ujednolicona'] = ad_time['dł_ujednolicona'].map(unified_lengths)
    
    return ad_time

def get_id_for_ads_desc():
    ads_desc = df[['data', 'opis_reklamy', 'kod_reklamy', 'brand', 'submedium', 'ad_time_details', 'produkt(4)', 'koszt', 'l_emisji', 'typ_reklamy']]
    ads_desc.index = ads_desc.index + 1

    if sum(ads_desc.value_counts()) != ads_desc.index.max():
        exit('Max index different than the length of the list.')

    query1 = sql.SQL('SELECT {fields} FROM {table}').format(
        fields=sql.SQL(',').join([
            sql.Identifier('brand'),
            sql.Identifier('id')
            ]),
        table=sql.Identifier('brands'))

    cur.execute(query1)
    brands_id = dict(cur.fetchall())


    query2 = sql.SQL('SELECT {fields} FROM {table}').format(
        fields=sql.SQL(',').join([
            sql.Identifier('submedium'),
            sql.Identifier('id')
            ]),
        table=sql.Identifier('mediums'))

    cur.execute(query2)
    medium_id = dict(cur.fetchall())


    query3 = sql.SQL('SELECT {fields} FROM {table}').format(
        fields=sql.SQL(',').join([
            sql.Identifier('ad_code'),
            sql.Identifier('id')
            ]),
        table=sql.Identifier('ad_time_details'))

    cur.execute(query3)
    ad_time_details_id = dict(cur.fetchall())


    query4 = sql.SQL('SELECT {fields} FROM {table}').format(
        fields=sql.SQL(',').join([
            sql.Identifier('product_type'),
            sql.Identifier('id')
            ]),
        table=sql.Identifier('product_types'))

    cur.execute(query4)
    product_type_id = dict(cur.fetchall())

    ads_desc['brand'] = ads_desc['brand'].map(brands_id)
    ads_desc['submedium'] = ads_desc['submedium'].map(medium_id)
    ads_desc['ad_time_details'] = ads_desc['ad_time_details'].map(ad_time_details_id)
    ads_desc['produkt(4)'] = ads_desc['produkt(4)'].map(product_type_id)
    
    return ads_desc

def get_colum_names(table_name):
    query = sql.SQL(
    '''
    SELECT c.column_name 
    FROM information_schema.columns c 
    WHERE c.table_name = %s
    ORDER BY c.ordinal_position;
    ''').format()
    cur.execute(query, (table_name,))
    table_data = cur.fetchall()
    temp = []
    for elem in table_data[1:]:
        temp.append(elem[0])
    table_data = temp
    
    return table_data


m_start = time.time()
print('Creating DataFrame.')

df_start = time.time()
# Reads the dataframe
df = pd.read_csv('./data/baza.csv', delimiter=';', thousands=',', dtype={'dł_ujednolicona': 'object'}, encoding='utf-8', parse_dates=['data'])
df.sort_values(by='data', axis=0, inplace=True)
df.reset_index(inplace=True)
df.drop('index', axis=1, inplace=True)
ind = df.index.values + 1
df['ad_time_details'] = df[['data', 'kod_reklamy']].apply(lambda x: f'{x[0]} - {x[1]} - {ind[x.name]}', axis=1)
df_end = time.time()
df_diff = df_end - df_start

# print(df.head(50))

# Create datasets for simple tables
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
            {'data': lengths, 'table': 'unified_lengths', 'field': 'length'},
            {'data': dayparts, 'table': 'dayparts', 'field': 'daypart'},
            {'data': product_types, 'table': 'product_types', 'field': 'product_type'},
            {'data': broadcasters, 'table': 'broadcasters', 'field': 'broadcaster'},
            {'data': reaches, 'table': 'ad_reach', 'field': 'reach'},
            ]

avoid_adding = ['pl_dow_names', 'pl_month_names', 'dayparts', 'ad_reach']
# tab_df_pairs = {'pl_dow_names': 'data',
#                   'pl_month_names': 'data',
#                   'date_time': 'data',
#                   'brands': 'brand',
#                   'unified_lengths': 'dł_ujednolicona',
#                   'dayparts': 'daypart',
#                   'product_types': 'produkt(4)',
#                   'broadcasters': 'wydawca_nadawca',
#                   'ad_reach': 'zasięg medium',
#                   'mediums': '',
#                   }

# Openes connection to the DB
print('Oppening connection.')
conn = psycopg2.connect(
    f'''dbname={tools.conf.DB}
        user={tools.conf.USER}
        host={tools.conf.HOST}
        port={tools.conf.PORT}
    '''
)

cur = conn.cursor()

# Inserting data into simple tables
ones_start = time.time()
print('Inserting data to one input tables.')
try:
    iter_over_inputs(data_set)
except psycopg2.OperationalError as e:
    conn.close()
    print('Failed to input the data.')
    print(f'Error: {e}')
ones_end = time.time()
ones_diff = ones_end - ones_start

# Create and insert data into mediums table
three_start = time.time()
print('Inserting data to the three input table.')
submediums = get_id_for_submediums()
fields = get_colum_names('mediums')
data_set2 = {'data': submediums, 'table': 'mediums', 'fields': fields}
try:
    add_3_fields(data_set2)
except psycopg2.OperationalError as e:
    conn.close()
    print('Failed to input the data.')
    print(f'Error: {e}')
three_end = time.time()
three_diff = three_end - three_start

# Create and insert data into ad_time_details table
eight_start = time.time()
print('Inserting data to the eight input table.')
ad_time = get_id_for_ad_time()
fields = get_colum_names('ad_time_details')
data_set3 = {'data': ad_time, 'table': 'ad_time_details', 'fields': fields}
try:
    add_8_fields(data_set3)
except psycopg2.OperationalError as e:
    conn.close()
    print('Failed to input the data.')
    print(f'Error: {e}')
eight_end = time.time()
eight_diff = eight_end - eight_start

# Create and insert data into ad_time_details table
ten_start = time.time()
print('Inserting data to the ten input table.')
ads_desc = get_id_for_ads_desc()
fields = get_colum_names('ads_desc')
data_set4 = {'data': ads_desc, 'table': 'ads_desc', 'fields': fields}
try:
    add_10_fields(data_set4)
except psycopg2.OperationalError as e:
    conn.close()
    print('Failed to input the data.')
    print(f'Error: {e}')
ten_end = time.time()
ten_diff = ten_end - ten_start


print('Closing connection.')
conn.close()
m_end =  time.time()
m_diff = m_end - m_start


print('Program has finished.')
print(f"""
Total time           : {m_diff:.2f}
DF creation          : {df_diff:.2f}
Ones processing time : {ones_diff:.2f}
Three processing time: {three_diff:.2f}
Eight processing time: {eight_diff:.2f}
Ten processing time  : {ten_diff:.2f}
"""
)