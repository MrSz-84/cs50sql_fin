import sqlite3
import pandas as pd
import numpy as np
import time
import os
import sys


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

file_path = f'../data/live_3.csv'

def get_index_val(table_name: str, cur: sqlite3.Cursor)-> int:
    """
    Function gets max index value from the selected table and returns it as an integer increased by one.
    When the table is empty, returns 1

    :param table_name: Name of the table out of which the data is going to be pulled, 
    represented as a str
    :param cur: Is a cursor object created for con object
    :raise sqlite3.ProgrammingError: If there are any error raised by the DB-API
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: number representiung max index value of selected table icreased by 1
    :rtype: int
    """
    
    query = (f'SELECT MAX(id) FROM {table_name};')
    cur.execute(query)
    ind = cur.fetchone()
    if ind[0] == None:
        return 1
    else:
        return ind[0] + 1
    
def iter_over_inputs(data_set:list[dict[list,str,str]], con: sqlite3.Connection, 
                        cur: sqlite3.Cursor, avoid_adding: list[str])-> None:
    """
    Main loop for iteration over one column tables.

    :param data_set: List containing dicts with data, table name and field/column name.
    List contains strings or integers representing the data to be added into selected tables.
    :param table_name: String representing name of the table into which data is going to be added
    :param con: Is a connection object, pointing to a DB
    :param cur: Is a cursor object created for con object
    :param avoid_adding: List of tablet which doesn't need to be updated
    :raise KeyError: If key name does not match the pattern
    :raise sqlite3.ProgrammingError: If there are any error raised by the DB-API
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: None
    """
    
    for elem in data_set:
        data = elem['data']
        table = elem['table']
        field = elem['field']
        new_data, data = check_for_data_1_field(data, table, field, cur, avoid_adding)
        if new_data:
            add_1_field(data, table, field, con, cur)

def add_1_field(data:list, table_name:str, field_name: str, 
                con: sqlite3.Connection, cur: sqlite3.Cursor)-> None:
    """
    Skeleton function for adding data to single column tables.

    :param data: List of strings or integers representing table contents
    :param table_name: String reprexsenting name of the table into which data is going to be added
    :param con: Is a connection object, pointing to a DB
    :param cur: Is a cursor object created for con object
    :raise sqlite3.ProgrammingError: If there are any error raised by the DB-API
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: None
    """
    
    query = (f'INSERT INTO {table_name} ({field_name}) VALUES(:name);')
    for elem in data:
        to_add = {'name': str(elem)}
        cur.execute(query, to_add)
    con.commit()

def check_for_data_1_field(data_:list, table_name:str, field_name:str, 
                            cur: sqlite3.Cursor, avoid_adding: list[str])-> tuple[bool,list[str]]:
    """
    Skeleton function for checking if there is data inside each of one column tables.
    Ads data if there are any new entries, skips if no new data was found. 
    If DB is empty returns immediately.

    :param data_: List containing data to be checked and added. Data is of str or int types.
    :param table_name: String representing name of the table into which data is going to be added
    :param field_name: String representing name of the field/ column name
    :param cur: Is a cursor object created for con object
    :param avoid_adding: List of tablet which doesn't need to be updated
    :raise sqlite3.ProgrammingError: If there is an error raised by the DB-API
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: A tuple containing bool for logic purposes, anbd the data set to be added
    :rtype: tuple[bool, list[str/int]]
    """
    
    query = (f'SELECT {field_name} FROM {table_name}')
    cur.execute(query)
    
    in_db = pd.DataFrame([elem[0] for elem in cur.fetchall()])
    in_db.rename(columns={0: field_name}, inplace=True)
    
    if len(in_db) == 0:
        return (True, data_)
    else:
        if field_name == 'data':
            # in_db['data'] = pd.to_datetime(in_db['data'])
            in_db = list(in_db['data'])
        else: 
            in_db = list(in_db[field_name])
        
        if len(in_db) != 0 and table_name in avoid_adding:
            print(f'>>> Not adding to {table_name}. No new data found.')
            return (False, list(''))
        in_df = pd.DataFrame(data_)
        in_df = in_df.rename(columns={0: field_name})
        # if field_name == 'data':
        #     pass
            # in_df['data'] = pd.to_datetime(in_df['data'])
        
        # we check if df contains new data in comparison to DB
        new_data = in_df[~in_df.isin(in_db)].dropna()
        new_data = new_data[field_name]
        new_data = list(new_data)
        
        if len(new_data) != 0:
            print(f'>>> Adding to {table_name}. New data found.')
            return (True, new_data)
        else:
            print(f'>>> Not adding to {table_name}. No new data found.')
            return (False, list(''))
        
def add_3_fields(data_set:dict[pd.DataFrame,str,list], 
                 con: sqlite3.Connection, cur: sqlite3.Cursor)-> None:
    """
    Function adding data into mediums table, which consists of 3 columns.

    :param data_set: A dict contaning data to be added, table name, and field / column name.
    Data is a Pandas DataFrame, table name and field name are both strings.
    :raise KeyError: If key name does not match the pattern
    :raise sqlite3.ProgrammingError: If table or field names doesn't match those in the DB
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: None
    """
    
    # col = data_set['data'].columns.values.tolist()
    
    query = (f"""
             INSERT INTO {data_set['table']} 
             ({data_set['fields'][0]}, 
             {data_set['fields'][1]}, 
             {data_set['fields'][2]})
             VALUES
             (:{data_set['fields'][0]}, 
             :{data_set['fields'][1]}, 
             :{data_set['fields'][2]})
             """)

    for _, elem in data_set['data'].iterrows():
        # TODO do type casting before data insertion
        
        data = {f'{field}': value for field, value in zip(data_set['fields'], elem)}
        cur.execute(query, data)
        
    con.commit()

def get_column_names(table_name:str, 
                    cur: sqlite3.Cursor)-> list[str]:
    """
    A function which returns the names of selected table from the DB.

    :param table_name: Name of a table out of which colum names are extracted from
    :param con: A database connection object
    :param cur: A cursor database object
    :raise sqlite3.ProgrammingError: If column names does not match DB contents
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: List containing all the column names present in selected table. 
    :rtype: list[str]
    """
    
    query = ("SELECT name FROM pragma_table_info(:table_name);")
    data = {'table_name': table_name}
    cur.execute(query, data)
    table_data = cur.fetchall()
    temp = []
    for elem in table_data[1:]:
        temp.append(elem[0])
    table_data = temp
    
    return table_data

def get_id_for_submediums(fields_:list[str], table_:str, 
                          cur: sqlite3.Cursor)-> tuple[bool, pd.DataFrame]:
    """
    Gets IDs from reference tables to mediums table. 
    Mainly connects submediums with broadcaster and reach tables.
    Returns a bool for logic purposes and data to be added into mediums.

    :param fields_: A list containing field / column names represented as a str
    :param table_: Name of the table out of which the data is going to be pulled, 
    represented as a str
    :param con: A database connection object
    :param cur: A cursor database object
    :raise sqlite3.ProgrammingError: If column names don't fit into the table design
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: Tuple containing bool for logic purposes and a Pandas DataFrame 
    as data to be added into the DB during the update or initial DB fill.
    :rtype: tuple[bool, pd.DataFrame]
    """
    
    submedia = df[['Submedium', 'Wydawca/Nadawca', 'Zasięg medium']].sort_values(by='Submedium')
    submedia.drop_duplicates(subset=['Submedium'], keep='first', inplace=True, ignore_index=True)
    
    if sum(submedia.value_counts()) != submedia.index.max() + 1:
        exit('Max index different than the length of the list.')
    
    query1 = (f"SELECT nadawca, id FROM nadawcy;")
    cur.execute(query1)
    nadawcy = dict(cur.fetchall())

    query2 = ("SELECT zasieg, id FROM zasiegi;")
    cur.execute(query2)
    zasiegi = dict(cur.fetchall())

    submedia['Wydawca/Nadawca'] = submedia['Wydawca/Nadawca'].map(nadawcy)
    submedia['Zasięg medium'] = submedia['Zasięg medium'].map(zasiegi)
    
    trigger, submedia = check_for_data_3_fields(fields_, table_, submedia, cur)
    
    return (trigger, submedia)

def check_for_data_3_fields(fields:list[str], table_name: str, submedia: pd.DataFrame, 
                            cur: sqlite3.Cursor)-> tuple[bool,pd.DataFrame]:
    """
    Returns a bool for logic purposes and data to be added into mediums table.
    If DB is empty returns original DF. During data update process returns the data not present in the DB
    or indicates there is nothing to be added.

    :param fields: A list containing field / column names represented as a str
    :param table_name: Name of the table into which data is going to be added as a str
    :param submedia: Pandas DataFrame containing data to add.
    :param cur: A cursor database object
    :raise sqlite3.ProgrammingError: If column names don't fit into the table design
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: Tuple containing bool for logic purposes and a Pandas DataFrame 
    as data to be added into the DB during the update
    :rtype: tuple[bool, pd.DataFrame]
    """
    
    query = (f"SELECT id, {fields[0]}, {fields[1]}, {fields[2]} FROM {table_name};")
    cur.execute(query)
    
    in_db = pd.DataFrame(cur.fetchall())
    in_db.rename(columns={0: 'id',1: fields[0], 2: fields[1], 3: fields[2]}, inplace=True)
    
    if len(in_db) == 0:
        return (True, submedia)
    else:
        in_db = list(in_db[fields[0]])
        in_df = submedia.copy()
        # we check if df contains new data in comparison to DB
        new_data = in_df[~in_df.isin(in_db)].dropna()
        
        if len(new_data) != 0 :
            print(f'>>> Adding to {table_name}. New data found.')
            return (True, new_data)
        print(f'>>> Not adding to {table_name}. No new data found.')
        return (False, submedia)

def get_id_for_ad_time(fields: list[str], table_: str, 
                       cur: sqlite3.Cursor)-> tuple[bool,pd.DataFrame]:
    """
    Gets IDs from reference tables to ad_time_details table. 
    Mainly connects time details of singular ad emission with other tables containing details via IDs.
    This function populates one of two core tables in this DB.
    Returns a bool for logic purposes and data to be added into mediums.

    :param fields: A list containing field / column names represented as a str
    :param table_: Name of the table out of which the data is going to be pulled, 
    represented as a str
    :param cur: A cursor database object
    :raise sqlite3.ProgrammingError: If column names don't fit into the table design
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: Tuple containing bool for logic purposes and a Pandas DataFrame 
    as data to be added into the DB during the update or initial DB fill.
    :rtype: tuple[bool, pd.DataFrame]
    """
    
    czasy_reklam = df[['Data', 'Godzina bloku reklamowego', 'GG', 'MM', 'dł./mod.', 'Daypart', 'dł. Ujednolicona', 'Detale_kod_reklamy']]
    czasy_reklam.index = czasy_reklam.index + get_index_val(table_, cur)

    query1 = ("SELECT dl_ujednolicona, id FROM dl_ujednolicone;")
    cur.execute(query1)
    unified_lengths = dict(cur.fetchall())

    query2 = ("SELECT daypart, id FROM dayparty;")
    cur.execute(query2)
    dayparts = dict(cur.fetchall())

    czasy_reklam.loc[:, 'Daypart'] = czasy_reklam['Daypart'].map(dayparts)
    czasy_reklam.loc[:, 'dł. Ujednolicona'] = czasy_reklam['dł. Ujednolicona'].map(unified_lengths)

    trigger, czasy_reklam = get_min_max_date(fields, table_, czasy_reklam, cur)
    
    return (trigger, czasy_reklam)

def get_min_max_date(fields: list[str], table_: str, dataframe: pd.DataFrame, 
                     cur: sqlite3.Cursor)-> tuple[bool, pd.DataFrame]:
    """
    Gets max and min dates from selected table. Then checks if dates present in the DF passed as a parameter
    are outside of dates range. If so, allows data insertion into the DB, if not it informs the user, 
    and proceedes with the rest of the code.

    :param fields: A list containing field / column names represented as a str
    :param table_: Name of the table out of which the data is going to be pulled, 
    represented as a str
    :param dataframe: Pandas DataFrame with the new data to be checked if not present in selected table
    :param cur: A cursor database object
    :raise sqlite3.ProgrammingError: If column names don't fit into the table design
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: Tuple containing bool for logic purposes and a Pandas DataFrame 
    as data to be added into the DB during the update or initial DB fill.
    :rtype: tuple[bool, pd.DataFrame]
    """
    
    # Get dates range from DB
    query = (f"SELECT DISTINCT({fields[0]}) FROM {table_}")
    cur.execute(query)
    in_db = pd.DataFrame(cur.fetchall(), dtype='datetime64[ns]')
    in_db.rename(columns={0: 'Data'}, inplace=True)
    
    # # Filter out dates that are already in DB.
    if not in_db.empty:
        filtr = ~dataframe['Data'].isin(in_db['Data'])
        dataframe = dataframe.loc[filtr]
    
    # Main logic add if empty or when dates not present in DB.
    if in_db.empty:
        return (True, dataframe)
    elif dataframe.empty:
        print(f'>>> Not adding to {table_}. One or more dates already in DB.')
        print(f'>>> Check the data you want to insert into DB.')
        return (False, dataframe)
    else:
        print(f'>>> Adding to {table_}. New data found.')
        return (True, dataframe)

def add_8_fields(data_set:dict[pd.DataFrame,str,list[str]], 
                 con: sqlite3.Connection, cur: sqlite3.Cursor)-> None:
    """
    Function adding data into ad_time_details table, which consists of 8 columns.

    :param data_set: A dict contaning data to be added, table name, and field / column name.
    Data is a Pandas DataFrame, table name and field name are both strings.
    :raise KeyError: If key name does not match the pattern
    :param con: A database connection object
    :param cur: A cursor database object
    :raise KeyError: If key name does not match the pattern
    :raise sqlite3.ProgrammingError: If column names don't fit into the table design
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: None
    """
    
    query = (f"""
             INSERT INTO {data_set['table']} (
                {data_set['fields'][0]},
                {data_set['fields'][1]},
                {data_set['fields'][2]},
                {data_set['fields'][3]},
                {data_set['fields'][4]},
                {data_set['fields'][5]},
                {data_set['fields'][6]},
                {data_set['fields'][7]})
            VALUES(
                :{data_set['fields'][0]},
                :{data_set['fields'][1]},
                :{data_set['fields'][2]},
                :{data_set['fields'][3]},
                :{data_set['fields'][4]},
                :{data_set['fields'][5]},
                :{data_set['fields'][6]},
                :{data_set['fields'][7]})
             """)

    for _, elem in data_set['data'].iterrows():
        elem.Data = elem.Data.strftime('%Y-%m-%d')
        data = {f'{field}': value for field, value in zip(data_set['fields'], elem)}
        cur.execute(query, data)
        # con.commit()
    
    con.commit()

def add_10_fields(data_set:dict[pd.DataFrame,str,list[str]],
                  con: sqlite3.Connection, cur: sqlite3.Cursor)-> None:
    """
    Function adding data into ad_time_details table, which consists of 10 columns.

    :param data_set: A dict contaning data to be added, table name, and field / column name.
    Data is a Pandas DataFrame, table name and field name are both strings.
    :param con: A database connection object
    :param cur: A cursor database object
    :raise KeyError: If key name does not match the pattern
    :raise sqlite3.ProgrammingError: If column names don't fit into the table design
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: None
    """
    
    query = (f"""
             INSERT INTO {data_set['table']} (
                {data_set['fields'][0]},
                {data_set['fields'][1]},
                {data_set['fields'][2]},
                {data_set['fields'][3]},
                {data_set['fields'][4]},
                {data_set['fields'][5]},
                {data_set['fields'][6]},
                {data_set['fields'][7]},
                {data_set['fields'][8]},
                {data_set['fields'][9]})
             VALUES(
                :{data_set['fields'][0]},
                :{data_set['fields'][1]},
                :{data_set['fields'][2]},
                :{data_set['fields'][3]},
                :{data_set['fields'][4]},
                :{data_set['fields'][5]},
                :{data_set['fields'][6]},
                :{data_set['fields'][7]},
                :{data_set['fields'][8]},
                :{data_set['fields'][9]})
             ;""")

    for _, elem in data_set['data'].iterrows():
        elem.Data = elem.Data.strftime('%Y-%m-%d')
        data = {field: elem for field, elem in zip(data_set['fields'], elem)}
        cur.execute(query, data)
        
    con.commit()
    
def get_id_for_ads_desc(fields: list[str], table_: str,
                        cur: sqlite3.Cursor)-> tuple[bool,pd.DataFrame]:
    """
    Gets IDs from reference tables to ads_desc table. 
    Mainly connects other tables and data of singular ad emission via IDs with other tables.
    This function populates one of two core tables in this DB.
    Returns a bool for logic purposes and data to be added into mediums.

    :param fields: A list containing field / column names represented as a str
    :param table_: Name of the table out of which the data is going to be pulled, 
    represented as a str
    :param cur: A cursor database object
    :raise sqlite3.ProgrammingError: If column names don't fit into the table design
    :daise sqlite3.OperationalError: If any eceptions on the DB side are raised, i.g. DB being locked
    :return: Tuple containing bool for logic purposes and a Pandas DataFrame 
    as data to be added into the DB during the update or initial DB fill.
    :rtype: tuple[bool, pd.DataFrame]
    """
    
    spoty = df[['Data', 'Opis Reklamy', 'Kod Reklamy', 'Brand', 'Submedium', 'Detale_kod_reklamy', 'Produkt(4)', 'Koszt [zł]', 'L.emisji', 'Typ reklamy']]
    spoty.index = spoty.index + get_index_val(table_, cur)

    query1 = ("SELECT brand, id FROM brandy;")
    cur.execute(query1)
    brand_id = dict(cur.fetchall())

    query2 =("SELECT submedium, id FROM submedia;")
    cur.execute(query2)
    submedium_id = dict(cur.fetchall())

    query3 = ("SELECT kod_rek, id FROM czasy_reklam;")
    cur.execute(query3)
    czas_reklamy_id = dict(cur.fetchall())

    query4 = ("SELECT typ_produktu, id FROM typy_produktu;")
    cur.execute(query4)
    typ_produktu_id = dict(cur.fetchall())

    spoty.loc[:, 'Brand'] = spoty['Brand'].map(brand_id)
    spoty.loc[:, 'Submedium'] = spoty['Submedium'].map(submedium_id)
    spoty.loc[:, 'Detale_kod_reklamy'] = spoty['Detale_kod_reklamy'].map(czas_reklamy_id)
    spoty.loc[:, 'Produkt(4)'] = spoty['Produkt(4)'].map(typ_produktu_id)
    
    trigger, spoty = get_min_max_date(fields, table_, spoty, cur)

    return (trigger, spoty)


main_dir = os.path.split(os.path.abspath(__file__))[0]
sys.path.append(main_dir)

m_start = time.time()
# Openes connection to the DB
print('Oppening connection.')
con = sqlite3.Connection('./radio_ads.db')
cur = con.cursor()


print('Creating DataFrame.')
df_start = time.time()
# Reads the dataframe
df = pd.read_csv(file_path, delimiter=';', thousands=',',
                 dtype={'Dzień': 'category', 'Dzień tygodnia': 'category', 
                        'Nr. tyg.': 'category', 'Rok': 'category',
                        'Miesiąc': 'category', 'Zasięg medium': 'category',
                        'Brand': 'object', 'Produkt(4)': 'object',
                        'Kod Reklamy': 'int32', 'Opis Reklamy': 'object',
                        'Typ reklamy': 'category', 'Wydawca/Nadawca': 'category',
                        'Submedium': 'object', 'dł./mod.': 'int8',
                        'GG': 'int8', 'MM': 'int8', 'Koszt [zł]': 'Int32',
                        'L.emisji': 'int8', 'dł. Ujednolicona': 'Int8', 
                        'Godzina bloku reklamowego': 'category'}, 
                 encoding='utf-8', parse_dates=['Data'], low_memory=False
                 )
df.sort_values(by='Data', axis=0, inplace=True)
df.reset_index(inplace=True)
df.drop('index', axis=1, inplace=True)
ind = df.index.values + get_index_val('spoty', cur)
df2 = df[['Data', 'Kod Reklamy']].copy()
df2['Data_str'] = df2['Data'].dt.strftime('%Y-%m-%d')
df['Detale_kod_reklamy'] = df2[['Data_str', 'Kod Reklamy']].apply(lambda x: f'{x["Data_str"]} - {x["Kod Reklamy"]} - {ind[x.name]}', axis=1)
del df2
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
dates = df['Data'].dt.strftime('%Y-%m-%d').unique()
brands = df['Brand'].sort_values().unique()
lengths = ['10', '15', '20', '30', '45', '60',]
dayparts = df['Daypart'].unique()
product_types = df['Produkt(4)'].sort_values().unique()
broadcasters = df['Wydawca/Nadawca'].sort_values().unique()
reaches = df['Zasięg medium'].unique()

data_set = [{'data': dow2, 'table': 'dni_tyg', 'field': 'dzien_tyg'},
            {'data': months, 'table': 'miesiace', 'field': 'miesiac'},
            {'data': dates, 'table': 'data_czas', 'field': 'data'},
            {'data': brands, 'table': 'brandy', 'field': 'brand'},
            {'data': lengths, 'table': 'dl_ujednolicone', 'field': 'dl_ujednolicona'},
            {'data': dayparts, 'table': 'dayparty', 'field': 'daypart'},
            {'data': product_types, 'table': 'typy_produktu', 'field': 'typ_produktu'},
            {'data': broadcasters, 'table': 'nadawcy', 'field': 'nadawca'},
            {'data': reaches, 'table': 'zasiegi', 'field': 'zasieg'},
            ]

avoid_adding = ['dni_tyg', 'miesiace', 'dl_ujednolicone', 'dayparty', 'zasiegi']



# Inserting data into simple tables
ones_start = time.time()
print('Inserting data to one input tables.')
try:
    iter_over_inputs(data_set, con, cur, avoid_adding)
except sqlite3.ProgrammingError as e:
    con.close()
    print('Failed to input the data.')
    print(f'Error: {e}')
except sqlite3.OperationalError as e:
    con.close()
    print('Failed to input the data.')
    print(f'Error: {e}')
    exit()
ones_end = time.time()
ones_diff = ones_end - ones_start

# Create and insert data into mediums table
three_start = time.time()
print('Inserting data to the three input table.')
fields = get_column_names('submedia', cur)
trigger, submedia = get_id_for_submediums(fields, 'submedia', cur)
data_set2 = {'data': submedia, 'table': 'submedia', 'fields': fields}
if trigger:
    try:
        add_3_fields(data_set2, con, cur)
    except sqlite3.ProgrammingError as e:
        con.close()
        print('Failed to input the data.')
        print(f'Error: {e}')
    except sqlite3.OperationalError as e:
        con.close()
        print('Failed to input the data.')
        print(f'Error: {e}')
        exit()
three_end = time.time()
three_diff = three_end - three_start

# Create and insert data into ad_time_details table
eight_start = time.time()
print('Inserting data to the eight input table.')
fields = get_column_names('czasy_reklam', cur)
trigger, czasy_reklam = get_id_for_ad_time(fields, 'czasy_reklam', cur)
data_set3 = {'data': czasy_reklam, 'table': 'czasy_reklam', 'fields': fields}
if trigger:
    try:
        add_8_fields(data_set3, con, cur)
    except sqlite3.ProgrammingError as e:
        con.close()
        print('Failed to input the data.')
        print(f'Error: {e}')
        exit()
    except sqlite3.OperationalError as e:
        con.close()
        print('Failed to input the data.')
        print(f'Error: {e}')
        exit()
eight_end = time.time()
eight_diff = eight_end - eight_start

# Create and insert data into ad_time_details table
ten_start = time.time()
print('Inserting data to the ten input table.')
fields = get_column_names('spoty', cur)
trigger, spoty = get_id_for_ads_desc(fields, 'spoty', cur)
data_set4 = {'data': spoty, 'table': 'spoty', 'fields': fields}
if trigger:
    try:
        add_10_fields(data_set4, con, cur)
    except sqlite3.ProgrammingError as e:
        con.close()
        print('Failed to input the data.')
        print(f'Error: {e}')
        exit()
    except sqlite3.OperationalError as e:
        con.close()
        print('Failed to input the data.')
        print(f'Error: {e}')
        exit()
ten_end = time.time()
ten_diff = ten_end - ten_start


print('Closing connection.')
con.close()
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
""")
