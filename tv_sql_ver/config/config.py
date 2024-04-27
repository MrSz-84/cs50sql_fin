DB_PATH = './tv_ads.db'
DATES = {'table': 'data_czas', 'field': 'data', 'type': 'table', "dtype": 'object'}
AD_CODE = {'table': 'kody_rek', 'field': 'opis', 'type': 'table', "dtype": 'object'}
BRANDS = {'table': 'brandy', 'field': 'brand', 'type': 'view', "dtype": 'object'}
CHANNELS = {'table': 'kanaly', 'field': 'kanal', 'type': 'view', "dtype": 'object'}
DAYPARTS = {'table': 'dayparty', 'field': 'daypart', 'type': 'table', "dtype": 'object'}
PIB_R = {'table': 'pib_real_rels', 'field': 'pib_real_rel', 'type': 'table', "dtype": 'object'}
DUR = {'table': 'dlugosci', 'field': 'dlugosc', 'type': 'table', "dtype": 'int32'}
SPOT_CLASS = {'table': 'klasy_spotu', 'field': 'klasa_spotu', 'type': 'table', "dtype": 'object'}
BLOCK_CODE = {'table': 'kody_bloku', 'field': 'kod_bloku', 'type': 'table', "dtype": 'object'}
PR_CAMP = {'table': 'prog_kampanie', 'field': 'prog_kampania', 'type': 'table', "dtype": 'object'}
PR_BEF = {'table': 'programy_przed', 'field': 'program_przed', 'type': 'table', "dtype": 'object'}
PR_AFT = {'table': 'programy_po', 'field': 'program_po', 'type': 'table', "dtype": 'object'}