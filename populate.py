import tools.conf
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np


conn = psycopg2.connect(
    f'''dbname={tools.conf.DB} 
        user={tools.conf.USER} 
        host={tools.conf.HOST}
        port={tools.conf.PORT}
    '''
    )

cur = conn.cursor()

cur.execute('SELECT * FROM pl_dow_names;')
print(cur.fetchone())

conn.close()
