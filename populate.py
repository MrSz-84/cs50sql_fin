import tools.conf
import psycopg
from psycopg import sql
import pandas as pd
import numpy as np
import time


def add_1_field(data:list, table_name:str, field_name: str)-> None:
    """
    Skeleton function for adding data to single column tables.

    :param data: List of strings or integers representing table contents
    :param table_name: String reprexsenting name of the table into which data is going to be added
    :raise psycopg.DataError: If data type does not match table restrictions
    :return: None
    """
    
    query = sql.SQL('INSERT INTO {table} ({field}) VALUES (%s)')

    for elem in data:
        cur.execute(
            query.format(
                table=sql.Identifier(f'{table_name}'),
                field=sql.Identifier(f'{field_name}')), (elem,)
        )
    conn.commit()

def iter_over_inputs(data_set:list[dict[list,str,str]])-> None:
    """
    Main loop for iteration over one column tables.

    :param data_set: List containing dicts with data, table name and field/column name.
    List contains strings or integers representing the data to be added into selected tables.
    :param table_name: String representing name of the table into which data is going to be added
    :raise KeyError: If key name does not match the pattern
    :return: None
    """
    
    for elem in data_set:
        data = elem['data']
        table = elem['table']
        field = elem['field']
        new_data, data = check_for_data_1_field(data, table, field)
        if new_data:
            add_1_field(data, table, field)

def check_for_data_1_field(data_:list, table_name:str, field_name:str)-> tuple[bool,list[str]]:
    """
    Skeleton function for checking if there is data inside each of one column tables.
    Ads data if there are any new entries, skips if no new data was found. 
    If DB is empty returns immediately.

    :param data_: List containing data to be checked and aded. Data is of str or int types.
    :param table_name: String representing name of the table into which data is going to be added
    :param field_name: String representing name of the field/ column name
    :return: A tuple containing bool for logic purposes, anbd the data set to be aded
    :rtype: tuple[bool, list[str/int]]
    """
    
    query = sql.SQL('SELECT {field} FROM {table}')
    cur.execute(
        query.format(
            table=sql.Identifier(table_name),
            field=sql.Identifier(field_name))
    )
    
    in_db = pd.DataFrame([elem[0] for elem in cur.fetchall()])
    in_db.rename(columns={0: field_name}, inplace=True)
    
    if len(in_db) == 0:
        return (True, data_)
    else:
        if field_name == 'date':
            in_db['date'] = pd.to_datetime(in_db['date'])
            in_db = list(in_db['date'])
        else: 
            in_db = list(in_db[field_name])
        
        if len(in_db) != 0 and table_name in avoid_adding:
            print(f'>>> Not adding to {table_name}. No new data found.')
            return (False, list(''))
        in_df = pd.DataFrame(data_)
        in_df = in_df.rename(columns={0: field_name})
        if field_name == 'date':
            in_df['date'] = pd.to_datetime(in_df['date'])
        
        # we check if df contains new data in comparison to DB
        new_data = in_df[~in_df.isin(in_db)].dropna()
        new_data = new_data[field_name]
        new_data = list(new_data)
        
        if len(new_data) != 0:
            print(f'>>> Adding to {table_name}. New data found.')
            return (True, new_data)
        else:
            return (False, list(''))

def add_3_fields(data_set:dict[pd.DataFrame,str,list])-> None:
    """
    Function adding data into mediums table, which consists of 3 columns.

    :param data_set: A dict contaning data to be added, table name, and field / column name.
    Data is a Pandas DataFrame, table name and field name are both strings.
    :raise KeyError: If key name does not match the pattern
    :raise psycopg.DataError: If table or field names doesn't match those in the DB
    :return: None
    """
    
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
    
def check_for_data_3_fields(fields:list[str], table_name: str, submediums: pd.DataFrame)-> tuple[bool,pd.DataFrame]:
    query = sql.SQL('SELECT id, {field1}, {field2}, {field3} FROM {table}')
    cur.execute(
        query.format(
            table=sql.Identifier(table_name),
            field1=sql.Identifier(fields[0]),
            field2=sql.Identifier(fields[1]),
            field3=sql.Identifier(fields[2]))
    )
    
    in_db = pd.DataFrame(cur.fetchall())
    in_db.rename(columns={0: 'id',1: fields[0], 2: fields[1], 3: fields[2]}, inplace=True)
    
    if len(in_db) == 0:
        return (True, submediums)
    else:
        in_db = list(in_db[fields[0]])
        in_df = submediums.copy()
        # we check if df contains new data in comparison to DB
        new_data = in_df[~in_df.isin(in_db)].dropna()
        
        if len(new_data) != 0 :
            print(f'>>> Adding to {table_name}. New data found.')
            return (True, new_data)
        print(f'>>> Not adding to {table_name}. No new data found.')
        return (False, submediums)

def add_8_fields(data_set:dict[pd.DataFrame,str,list[str]])-> None:
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

def add_10_fields(data_set:dict[pd.DataFrame,str,list[str]])-> None:
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

def get_id_for_submediums(fields:list[str], table:str)-> pd.DataFrame:
    submediums = df[['submedium', 'wydawca_nadawca', 'zasięg medium']].sort_values(by='submedium')
    submediums.drop_duplicates(subset=['submedium'], keep='first', inplace=True, ignore_index=True)
    
    if sum(submediums.value_counts()) != submediums.index.max() + 1:
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
    
    trigger, submediums = check_for_data_3_fields(fields, table, submediums)
    
    return (trigger, submediums)

def get_id_for_ad_time(fields: list[str], table_: str)-> tuple[bool,pd.DataFrame]:
    ad_time = df[['data', 'godzina_bloku_reklamowego', 'gg', 'mm', 'dl_mod', 'daypart', 'dł_ujednolicona', 'ad_time_details']]
    ad_time.index = ad_time.index + get_index_val(table_)

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
    
    trigger, ad_time = get_min_max_date(fields, table_, ad_time)
    
    return (trigger, ad_time)

def get_id_for_ads_desc(fields: list[str], table_: str)-> tuple[bool,pd.DataFrame]:
    ads_desc = df[['data', 'opis_reklamy', 'kod_reklamy', 'brand', 'submedium', 'ad_time_details', 'produkt(4)', 'koszt', 'l_emisji', 'typ_reklamy']]
    ads_desc.index = ads_desc.index + get_index_val(table_)

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

    ads_desc.loc[:, 'brand'] = ads_desc['brand'].map(brands_id)
    ads_desc.loc[:, 'submedium'] = ads_desc['submedium'].map(medium_id)
    ads_desc.loc[:, 'ad_time_details'] = ads_desc['ad_time_details'].map(ad_time_details_id)
    ads_desc.loc[:, 'produkt(4)'] = ads_desc['produkt(4)'].map(product_type_id)
    
    trigger, ads_desc = get_min_max_date(fields, table_, ads_desc)
    
    return (trigger, ads_desc)

def get_colum_names(table_name:str)->list[str]:
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

def get_index_val(table_name: str)-> int:
    
    query = sql.SQL('SELECT MAX(id) FROM {table};')
    cur.execute(query.format(table=sql.Identifier(table_name)))
    ind = cur.fetchone()
    if ind[0] == None:
        return 1
    else:
        return ind[0] + 1

def get_min_max_date(fields: list[str], table_: str, dataframe: pd.DataFrame)-> tuple[bool, pd.DataFrame]:
    
    # Get max date from DB
    query = sql.SQL('SELECT MAX({field1}) FROM {table};')
    cur.execute(
        query.format(
            table=sql.Identifier(table_),
            field1=sql.Identifier(fields[0])
        )
    )
    in_db_max = pd.Timestamp(cur.fetchone()[0])
    
    # Get min date from DB
    query = sql.SQL('SELECT MIN({field1}) FROM {table};')
    cur.execute(
        query.format(
            table=sql.Identifier(table_),
            field1=sql.Identifier(fields[0])
        )
    )
    in_db_min = pd.Timestamp(cur.fetchone()[0])
    
    # Get max and min date from DF
    in_df_max = dataframe['data'].max()
    in_df_min = dataframe['data'].min()
    
    # Check if min and max dates from DF are between range of dates from DB
    min_df_in_db_range = in_db_min <= in_df_min <= in_db_max
    max_df_in_db_range = in_db_min <= in_df_max <= in_db_max
    
    # Main logic add if empty or when dates not present in DB.
    if  pd.isnull(in_db_max) or pd.isnull(in_db_min) :
        return (True, dataframe)
    elif not min_df_in_db_range and not max_df_in_db_range:
        print(f'>>> Adding to {table_}. New data found.')
        return (True, dataframe)
    else:
        print(f'>>> Not adding to {table_}. One or more dates already in DB.')
        print(f'>>> Check the data you want to insert into DB.')
        return (False, dataframe)


m_start = time.time()
# Openes connection to the DB
print('Oppening connection.')
conn = psycopg.connect(
    f'''dbname={tools.conf.DB}
        user={tools.conf.USER}
        host={tools.conf.HOST}
        port={tools.conf.PORT}
    '''
)

cur = conn.cursor()

print('Creating DataFrame.')
df_start = time.time()
# Reads the dataframe
df = pd.read_csv('./data/baza2.csv', delimiter=';', thousands=',', dtype={'dł_ujednolicona': 'object'}, encoding='utf-8', parse_dates=['data'])
df.sort_values(by='data', axis=0, inplace=True)
df.reset_index(inplace=True)
df.drop('index', axis=1, inplace=True)
ind = df.index.values + get_index_val('ads_desc')
df['ad_time_details'] = df[['data', 'kod_reklamy']].apply(lambda x: f'{x["data"]} - {x["kod_reklamy"]} - {ind[x.name]}', axis=1)
df_end = time.time()
df_diff = df_end - df_start

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


# Inserting data into simple tables
ones_start = time.time()
print('Inserting data to one input tables.')
try:
    iter_over_inputs(data_set)
except psycopg.OperationalError as e:
    conn.close()
    print('Failed to input the data.')
    print(f'Error: {e}')
ones_end = time.time()
ones_diff = ones_end - ones_start

# Create and insert data into mediums table
three_start = time.time()
print('Inserting data to the three input table.')
fields = get_colum_names('mediums')
trigger, submediums = get_id_for_submediums(fields, 'mediums')
data_set2 = {'data': submediums, 'table': 'mediums', 'fields': fields}
if trigger:
    try:
        add_3_fields(data_set2)
    except psycopg.OperationalError as e:
        conn.close()
        print('Failed to input the data.')
        print(f'Error: {e}')
three_end = time.time()
three_diff = three_end - three_start

# Create and insert data into ad_time_details table
eight_start = time.time()
print('Inserting data to the eight input table.')
fields = get_colum_names('ad_time_details')
trigger, ad_time = get_id_for_ad_time(fields, 'ad_time_details')
data_set3 = {'data': ad_time, 'table': 'ad_time_details', 'fields': fields}
if trigger:
    try:
        add_8_fields(data_set3)
    except psycopg.OperationalError as e:
        conn.close()
        print('Failed to input the data.')
        print(f'Error: {e}')
eight_end = time.time()
eight_diff = eight_end - eight_start

# Create and insert data into ad_time_details table
ten_start = time.time()
print('Inserting data to the ten input table.')
fields = get_colum_names('ads_desc')
trigger, ads_desc = get_id_for_ads_desc(fields, 'ads_desc')
data_set4 = {'data': ads_desc, 'table': 'ads_desc', 'fields': fields}
if trigger:
    try:
        add_10_fields(data_set4)
    except psycopg.OperationalError as e:
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