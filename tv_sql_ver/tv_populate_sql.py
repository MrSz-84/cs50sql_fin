import config.config as cf
import sqlite3
import pandas as pd
import numpy as np
import os
import sys
import time

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

file_path = "../data/tv/all.xlsx"

names = {
    "NEONET.PL": "NEONET AGD RTV",
    "EURO.COM.PL": "EURO RTV AGD",
}
dayparts = {"Off stacji": "off", "Prime stacji": "prime"}


def iter_over_inputs(
    data_set: list[dict[list, str, str, str]],
    con: sqlite3.Connection,
    cur: sqlite3.Cursor,
) -> None:
    """
    Main loop for iteration over one column tables.

    :param data_set: List containing dicts with data, table name and field/column name.
    List contains strings or integers representing the data to be added into selected tables.
    :param table_name: String representing name of the table into which data is going to be added
    :param con: Is a connection object, pointing to a DB
    :param cur: Is a cursor object created for con object
    :raise KeyError: If key name does not match the pattern
    :raise sqlite3.ProgrammingError: If there are any error raised by the DB-API
    :raise sqlite3.OperationalError: If any exceptions on the DB side are raised, i.g. DB being locked
    :return: None
    """

    for elem in data_set:
        data = elem["data"]
        table = elem["table"]
        field = elem["field"]
        table_type = elem["type"]
        data_type = elem["dtype"]
        new_data, data = check_for_data_1_field(data, table, field, data_type, cur)
        if new_data:
            add_1_field(data, table, field, table_type, data_type, con, cur)


def add_1_field(
    data: list,
    table_name: str,
    field_name: str,
    type_: str,
    dtype: str,
    con: sqlite3.Connection,
    cur: sqlite3.Cursor,
) -> None:
    """
    Skeleton function for adding data to up to three column tables.

    :param data: List of strings or integers representing table contents
    :param table_name: String representing name of the table into which data is going to be added
    :param type_: String representing type of target where to add the data. Available table or view
    :param dtype: String representing to what type data should be converted before upload
    :param con: Is a connection object, pointing to a DB
    :param cur: Is a cursor object created for con object
    :raise sqlite3.ProgrammingError: If there are any error raised by the DB-API
    :raise sqlite3.OperationalError: If any exceptions on the DB side are raised, i.g. DB being locked
    :return: None
    """

    if type_ == "table":
        query = f"INSERT INTO {table_name} ({field_name}) VALUES(:name);"
    else:
        if table_name == "brandy":
            query = f"INSERT INTO podziel_brandy_view ({field_name}) VALUES(:name);"
        else:
            query = f"INSERT INTO podziel_kanaly_view ({field_name}) VALUES(:name);"
    for elem in data:
        if dtype.startswith("int"):
            to_add = {"name": int(elem)}
        else:
            to_add = {"name": str(elem)}

        cur.execute(query, to_add)
    con.commit()


def check_for_data_1_field(
    data_: list | pd.DataFrame,
    table_name: str,
    field_name: str,
    dtype: str,
    cur: sqlite3.Cursor,
) -> tuple[bool, list[str | int]]:
    """
    Skeleton function for checking if there is data inside each of one column tables.
    Ads data if there are any new entries, skips if no new data was found.
    If DB is empty returns immediately.

    :param data_: List containing data to be checked and added. Data is of str or int types.
    :param table_name: String representing name of the table into which data is going to be added
    :param field_name: String representing name of the field/ column name
    :param dtype: String representing to what type data should be converted before upload
    :param cur: Is a cursor object created for con object
    :param avoid_adding: List of tablet which doesn't need to be updated
    :raise sqlite3.ProgrammingError: If there is an error raised by the DB-API
    :raise sqlite3.OperationalError: If any exceptions on the DB side are raised, i.g. DB being locked
    :return: A tuple containing bool for logic purposes, and the data set to be added
    :rtype: tuple[bool, list[str|int]]
    """

    if table_name != "kody_rek":
        query = f"SELECT {field_name} FROM {table_name};"
    else:
        query = f"SELECT kod_rek FROM {table_name};"
    cur.execute(query)

    in_db = pd.DataFrame([elem[0] for elem in cur.fetchall()])
    in_db.rename(columns={0: field_name}, inplace=True)
    if table_name == "kody_rek":
        in_db = in_db.astype("int32")  # new
    else:
        in_db = in_db.astype(dtype)  # new

    if len(in_db) == 0 and table_name in ("kody_rek", "brandy", "kanaly"):
        data_ = data_.iloc[:, -1]
        return (True, data_)
    elif len(in_db) == 0:
        return (True, data_)

    if table_name not in ("kody_rek", "brandy", "kanaly"):
        data_ = pd.DataFrame(data_)
        data_ = data_.rename(columns={0: field_name})
        data_ = data_.astype({field_name: dtype})
    else:  # new
        cols = data_.columns.to_list()  # new
        if table_name == "kody_rek":
            data_ = data_.astype({cols[0]: "int32"})
        else:
            data_ = data_.astype({cols[0]: dtype})  # new

    # we check if df contains new data in comparison to DB
    if table_name in ("kody_rek", "brandy", "kanaly"):
        filtr = ~data_[cols[0]].isin(in_db[field_name])
        new_data = data_[filtr].dropna()
        new_data = new_data.rename({cols[-1]: field_name}, axis=1)
        new_data = new_data[field_name]
    else:
        filtr = ~data_[field_name].isin(in_db[field_name])
        new_data = data_[filtr].dropna()
        new_data = new_data[field_name]

    new_data = list(new_data)

    if len(new_data) != 0:
        print(f">>> Adding to {table_name}. New data found.")
        return (True, new_data)
    else:
        print(f">>> Not adding to {table_name}. No new data found.")
        return (False, list(""))


def get_column_names(table_name: str, cur: sqlite3.Cursor) -> list[str]:
    """
    A function which returns the names of selected table from the DB.

    :param table_name: Name of a table out of which colum names are extracted from
    :param con: A database connection object
    :param cur: A cursor database object
    :raise sqlite3.ProgrammingError: If column names does not match DB contents
    :raise sqlite3.OperationalError: If any exceptions on the DB side are raised, i.g. DB being locked
    :return: List containing all the column names present in selected table.
    :rtype: list[str]
    """

    query = "SELECT name FROM pragma_table_info(:table_name);"
    data = {"table_name": table_name}
    cur.execute(query, data)
    table_data = cur.fetchall()
    temp = []
    for elem in table_data[1:]:
        temp.append(elem[0])
    table_data = temp

    return table_data


def add_16_fields(
    data_set: dict[pd.DataFrame, str, list[str]],
    con: sqlite3.Connection,
    cur: sqlite3.Cursor,
) -> None:
    """
    Function adding data into ad_time_details table, which consists of 14 columns.

    :param data_set: A dict contaning data to be added, table name, and field / column name.
    Data is a Pandas DataFrame, table name and field name are both strings.
    :param con: A database connection object
    :param cur: A cursor database object
    :raise KeyError: If key name does not match the pattern
    :raise sqlite3.ProgrammingError: If column names don't fit into the table design
    :raise sqlite3.OperationalError: If any exceptions on the DB side are raised, i.g. DB being locked
    :return: None
    """

    query = f"""
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
                {data_set['fields'][9]},
                {data_set['fields'][10]},
                {data_set['fields'][11]},
                {data_set['fields'][12]},
                {data_set['fields'][13]},
                {data_set['fields'][14]},
                {data_set['fields'][15]})
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
                :{data_set['fields'][9]},
                :{data_set['fields'][10]},
                :{data_set['fields'][11]},
                :{data_set['fields'][12]},
                :{data_set['fields'][13]},
                :{data_set['fields'][14]},
                :{data_set['fields'][15]})
             ;"""

    for _, elem in data_set["data"].iterrows():
        elem.Date = elem.Date.strftime("%Y-%m-%d")
        elem.Time = elem.Time.strftime("%H:%M:%S")
        data = {field: elem for field, elem in zip(data_set["fields"], elem)}
        cur.execute(query, data)

    con.commit()


def get_id_for_spoty(
    fields: list[str], table_: str, cur: sqlite3.Cursor
) -> tuple[bool, pd.DataFrame]:
    """
    Gets IDs from reference tables to aspoty table.
    Mainly connects other tables and data of singular ad emission via IDs with other tables.
    This function populates one of two core tables in this DB.
    Returns a bool for logic purposes and data to be added into mediums.

    :param fields: A list containing field / column names represented as a str
    :param table_: Name of the table out of which the data is going to be pulled,
    represented as a str
    :param cur: A cursor database object
    :raise sqlite3.ProgrammingError: If column names don't fit into the table design
    :raise sqlite3.OperationalError: If any exceptions on the DB side are raised, i.g. DB being locked
    :return: Tuple containing bool for logic purposes and a Pandas DataFrame
    as data to be added into the DB during the update or initial DB fill.
    :rtype: tuple[bool, pd.DataFrame]
    """

    spoty = df_tv[
        [
            "Date",
            "Time",
            "PIB pos",
            "PIB count",
            "PIB (real) rel",
            "Spot Class",
            "Block Code",
            "Dayparts",
            "GRP",
            "Channel",
            "Brand",
            "Dur rounded,sp",
            "Film Code/2",
            "Prog Campaign",
            "Prog Before",
            "Prog After",
            "Zlepek",
        ]
    ]
    spoty = spoty.astype({"Film Code/2": "int32"})

    query1 = "SELECT pib_real_rel, id FROM pib_real_rels;"
    cur.execute(query1)
    pib_real_rel_id = dict(cur.fetchall())
    query2 = "SELECT klasa_spotu, id FROM klasy_spotu;"
    cur.execute(query2)
    klasa_spotu_id = dict(cur.fetchall())
    query3 = "SELECT kod_bloku, id FROM kody_bloku;"
    cur.execute(query3)
    kod_bloku_id = dict(cur.fetchall())
    query4 = "SELECT daypart, id FROM dayparty;"
    cur.execute(query4)
    daypart_id = dict(cur.fetchall())
    query5 = "SELECT kanal, id FROM kanaly;"
    cur.execute(query5)
    kanal_id = dict(cur.fetchall())
    query6 = "SELECT brand, id FROM brandy;"
    cur.execute(query6)
    brand_id = dict(cur.fetchall())
    query7 = "SELECT kod_rek, id FROM kody_rek;"
    cur.execute(query7)
    kod_rek_id = dict(cur.fetchall())
    query8 = "SELECT prog_kampania, id FROM prog_kampanie;"
    cur.execute(query8)
    prog_kampania_id = dict(cur.fetchall())
    query9 = "SELECT dlugosc, id FROM dlugosci;"
    cur.execute(query9)
    dlugosc = dict(cur.fetchall())
    query10 = "SELECT program_przed, id FROM programy_przed;"
    cur.execute(query10)
    program_przed_id = dict(cur.fetchall())
    query11 = "SELECT program_po, id FROM programy_po;"
    cur.execute(query11)
    program_po_id = dict(cur.fetchall())

    spoty.loc[:, "PIB (real) rel"] = spoty["PIB (real) rel"].map(pib_real_rel_id)
    spoty.loc[:, "Spot Class"] = spoty["Spot Class"].map(klasa_spotu_id)
    spoty.loc[:, "Block Code"] = spoty["Block Code"].map(kod_bloku_id)
    spoty.loc[:, "Dayparts"] = spoty["Dayparts"].map(daypart_id)
    spoty.loc[:, "Channel"] = spoty["Channel"].map(kanal_id)
    spoty.loc[:, "Brand"] = spoty["Brand"].map(brand_id)
    spoty.loc[:, "Film Code/2"] = spoty["Film Code/2"].map(kod_rek_id).astype("int32")
    spoty.loc[:, "Prog Campaign"] = spoty["Prog Campaign"].map(prog_kampania_id)
    spoty.loc[:, "Dur rounded,sp"] = spoty["Dur rounded,sp"].map(dlugosc).astype("int8")
    spoty.loc[:, "Prog Before"] = spoty["Prog Before"].map(program_przed_id)
    spoty.loc[:, "Prog After"] = spoty["Prog After"].map(program_po_id)

    trigger, spoty = get_unique_record(fields, table_, spoty, cur)

    return (trigger, spoty)


def get_unique_record(
    fields: list[str], table_: str, dataframe: pd.DataFrame, cur: sqlite3.Cursor
) -> tuple[bool, pd.DataFrame]:
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
    :raise sqlite3.OperationalError: If any exceptions on the DB side are raised, i.g. DB being locked
    :return: Tuple containing bool for logic purposes and a Pandas DataFrame
    as data to be added into the DB during the update or initial DB fill.
    :rtype: tuple[bool, pd.DataFrame]
    """

    # Get dates range from DB
    query = f"""
                SELECT {fields[0]} || {fields[1]} || brand || kanal || kod_rek 
                FROM {table_}
                JOIN kody_rek ON kody_rek.id = spoty.kod_rek_id
                JOIN kanaly ON spoty.kanal_id = kanaly.id
                JOIN brandy ON spoty.brand_id = brandy.id;
            """
    cur.execute(query)
    in_db = pd.DataFrame(cur.fetchall())
    in_db.rename(columns={0: "Zlepek"}, inplace=True)

    # # Filter out dates that are already in DB.
    if not in_db.empty:
        filtr = ~dataframe["Zlepek"].isin(in_db["Zlepek"])
        dataframe = dataframe.loc[filtr]

    # Main logic add if empty or when dates not present in DB.
    if in_db.empty:
        return (True, dataframe)
    elif dataframe.empty:
        print(f">>> Not adding to {table_}. One or more dates already in DB.")
        print(f">>> Check the data you want to insert into DB.")
        return (False, dataframe)
    else:
        print(f">>> Adding to {table_}. New data found.")
        return (True, dataframe)


main_dir = os.path.split(os.path.abspath(__file__))[0]
sys.path.append(main_dir)

m_start = time.time()
# Opens connection to the DB
print("Oppening connection.")
con = sqlite3.Connection(cf.DB_PATH)
cur = con.cursor()


print("Creating DataFrame.")
df_start = time.time()
# Reads the dataframe from CVS
# df_tv = pd.read_csv(
#     file_path,
#     delimiter=";",
#     thousands=" ",
#     decimal=",",
#     dtype={
#         "Month": "category",
#         "Week": "category",
#         "Weekday": "category",
#         "Dayparts": "object",
#         "Channel Groups": "object",
#         "Channel": "object",
#         "PIB pos": "int8",
#         "PIB (real) rel": "object",
#         "PIB count": "int16",
#         "Dur rounded,sp": "int8",
#         "Spot Class": "object",
#         "Block Code": "object",
#         "Syndicate": "object",
#         "Producer": "object",
#         "Brand": "object",
#         "Film Code": "object",
#         "Prog Campaign": "object",
#         "Prog Before": "object",
#         "Prog After": "object",
#         "Film Code/2": "object",
#         "TRP All 40+ cities 200- (i wsie) Nat[TSV2]": "float64",
#     },
#     parse_dates=["Date", "Time"],
#     date_format="%d.%m.%Y",
# )
# df_tv.rename(
#     columns={"TRP All 40+ cities 200- (i wsie) Nat[TSV2]": "GRP"}, inplace=True
# )
# df_tv["Brand"] = df_tv["Brand"].str.strip()
# df_tv["Brand"] = df_tv["Brand"].str.upper()
# df_tv["Producer"] = df_tv["Producer"].str.upper()
# df_tv["Syndicate"] = df_tv["Syndicate"].str.upper()
# df_tv["Brand"] = df_tv["Brand"].map(names).fillna(df_tv["Brand"])
# df_tv["Dayparts"] = df_tv["Dayparts"].map(dayparts).fillna(df_tv["Dayparts"])
# # df_tv["DateTime"] = df_tv["Date"].astype("str") + " " + df_tv["Time"]
# df_tv["Kod Opis"] = df_tv["Film Code/2"] + "@|@" + df_tv["Film Code"]
# df_tv["Kanal Grupa"] = df_tv["Channel"] + "@|@" + df_tv["Channel Groups"]
# df_tv["Brand Prod Synd"] = (
#     df_tv["Brand"] + "@|@" + df_tv["Producer"] + "#|#" + df_tv["Syndicate"]
# )
# df_tv["Zlepek"] = (
#     df_tv["Date"].dt.strftime("%Y-%m-%d")
#     + df_tv["Time"]
#     + df_tv["Brand"]
#     + df_tv["Channel"]
#     + df_tv["Film Code/2"]
# )
# # df_tv['DateTime'] = pd.to_datetime(df_tv['DateTime'], format='ISO8601')
# df_tv[["Dayparts", "Prog Before", "Prog After"]] = df_tv[
#     ["Dayparts", "Prog Before", "Prog After"]
# ].fillna("brak danych", axis=1)
# df_tv.sort_values(by="Date", inplace=True, axis=0)
# df_tv.reset_index(inplace=True)

# Reads the dataframe from EXCEL
dfs = pd.read_excel(
    file_path,
    sheet_name=None,
    thousands=" ",
    decimal=",",
    dtype={
        "Month": "category",
        "Week": "category",
        "Weekday": "category",
        "Dayparts": "object",
        "Channel Groups": "object",
        "Channel": "object",
        "PIB pos": "int8",
        "PIB (real) rel": "object",
        "PIB count": "int16",
        "Dur rounded,sp": "int8",
        "Spot Class": "object",
        "Block Code": "object",
        "Syndicate": "object",
        "Producer": "object",
        "Brand": "object",
        "Film Code": "object",
        "Prog Campaign": "object",
        "Prog Before": "object",
        "Prog After": "object",
        "Film Code/2": "object",
        "TRP All 40+ cities 200- (i wsie) Nat[TSV2]": "float64",
    },
    parse_dates=["Date"],
    date_format="%d.%m.%Y",
)
df_tv = pd.concat(dfs, ignore_index=True)
df_tv["Date"] = pd.to_datetime(dfs["Date"], dayfirst=True, format="mixed")
del dfs
df_tv.rename(
    columns={"TRP All 40+ cities 200- (i wsie) Nat[TSV2]": "GRP"}, inplace=True
)
df_tv["Brand"] = df_tv["Brand"].str.strip()
df_tv["Brand"] = df_tv["Brand"].str.upper()
df_tv["Producer"] = df_tv["Producer"].str.upper()
df_tv["Syndicate"] = df_tv["Syndicate"].str.upper()
df_tv["Brand"] = df_tv["Brand"].map(names).fillna(df_tv["Brand"])
df_tv["Dayparts"] = df_tv["Dayparts"].map(dayparts).fillna(df_tv["Dayparts"])
# df_tv["DateTime"] = df_tv["Date"].astype("str") + " " + df_tv["Time"]
df_tv["Kod Opis"] = df_tv["Film Code/2"].astype("str") + "@|@" + df_tv["Film Code"]
df_tv["Kanal Grupa"] = df_tv["Channel"] + "@|@" + df_tv["Channel Groups"]
df_tv["Brand Prod Synd"] = (
    df_tv["Brand"] + "@|@" + df_tv["Producer"] + "#|#" + df_tv["Syndicate"]
)
df_tv["Zlepek"] = (
    df_tv["Date"].dt.strftime("%Y-%m-%d").astype("str")
    + df_tv["Time"].astype("str")
    + df_tv["Brand"]
    + df_tv["Channel"]
    + df_tv["Film Code/2"].astype("str")
)
# df_tv['DateTime'] = pd.to_datetime(df_tv['DateTime'], format='ISO8601')
df_tv[["Dayparts", "Prog Before", "Prog After"]] = df_tv[
    ["Dayparts", "Prog Before", "Prog After"]
].fillna("brak danych", axis=1)
df_tv.sort_values(by="Date", inplace=True, axis=0)
df_tv.reset_index(inplace=True)
df_end = time.time()
df_diff = df_end - df_start

# Create datasets for simple tables
dates = df_tv["Date"].dt.strftime("%Y-%m-%d").unique()
ad_codes = df_tv.loc[:, ["Film Code/2", "Kod Opis"]].drop_duplicates(
    subset=["Film Code/2"], keep="first", ignore_index=True
)
brands = df_tv.loc[:, ["Brand", "Brand Prod Synd"]].drop_duplicates(
    subset=["Brand"], keep="first", ignore_index=True
)
channels = df_tv.loc[:, ["Channel", "Kanal Grupa"]].drop_duplicates(
    subset=["Channel"], keep="first", ignore_index=True
)
dayparts = df_tv["Dayparts"].unique()
pib_real_rels = df_tv["PIB (real) rel"].unique()
durations = df_tv["Dur rounded,sp"].unique()
spot_classes = df_tv["Spot Class"].unique()
block_codes = df_tv["Block Code"].unique()
prog_campaign = df_tv["Prog Campaign"].unique()
prog_before = df_tv["Prog Before"].unique()
prog_after = df_tv["Prog After"].unique()

data_set = [
    {"data": dates, **cf.DATES},
    {"data": dayparts, **cf.DAYPARTS},
    {"data": pib_real_rels, **cf.PIB_R},
    {"data": durations, **cf.DUR},
    {"data": spot_classes, **cf.SPOT_CLASS},
    {"data": block_codes, **cf.BLOCK_CODE},
    {"data": prog_campaign, **cf.PR_CAMP},
    {"data": prog_before, **cf.PR_BEF},
    {"data": prog_after, **cf.PR_AFT},
    {"data": ad_codes, **cf.AD_CODE},
    {"data": brands, **cf.BRANDS},
    {"data": channels, **cf.CHANNELS},
]


# Inserting data into simple tables
ones_start = time.time()
print("Inserting data to one input tables.")
try:
    iter_over_inputs(data_set, con, cur)
except sqlite3.ProgrammingError as e:
    con.close()
    print("Failed to input the data.")
    print(f"Error: {e}")
except sqlite3.OperationalError as e:
    con.close()
    print("Failed to input the data.")
    print(f"Error: {e}")
    exit()
except sqlite3.IntegrityError as e:
    con.close()
    print("Failed to input the data.")
    print(f"Error: {e}")
    exit()
ones_end = time.time()
ones_diff = ones_end - ones_start

# Create and insert data into spoty table
sixteen_start = time.time()
print("Inserting data to the sixteen input table.")
fields = get_column_names("spoty", cur)
trigger, spoty = get_id_for_spoty(fields, "spoty", cur)
data_set2 = {"data": spoty, "table": "spoty", "fields": fields}
if trigger:
    try:
        add_16_fields(data_set2, con, cur)
    except sqlite3.ProgrammingError as e:
        con.close()
        print("Failed to input the data.")
        print(f"Error: {e}")
        exit()
    except sqlite3.OperationalError as e:
        con.close()
        print("Failed to input the data.")
        print(f"Error: {e}")
        exit()
    except sqlite3.IntegrityError as e:
        con.close()
        print("Failed to input the data.")
        print(f"Error: {e}")
        exit()
sixteen_end = time.time()
sixteen_diff = sixteen_end - sixteen_start

print("Closing connection.")
con.close()
m_end = time.time()
m_diff = m_end - m_start

print("Program has finished.")
print(
    f"""
Total time              : {m_diff:.2f}
DF creation             : {df_diff:.2f}
Ones processing time    : {ones_diff:.2f}
Sixteen processing time : {sixteen_diff:.2f}
"""
)
