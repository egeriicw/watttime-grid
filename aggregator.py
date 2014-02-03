from pandas.io import sql
import csv
import sqlite3

regions = {
    'all': ['al', 'az', 'ar', 'ca', 'co', 'ct', 'dc',
            'de', 'fl', 'ga', 'id', 'il', 'in', 'ia',
            'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi',
            'mn', 'mi', 'mo', 'mt', 'ne', 'nv', 'nh',
            'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok',
            'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx',
            'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy'],
    }


conn = sqlite3.connect('watttime-grid.db')

ba_abbrevs = [r[0] for r in conn.execute('SELECT distinct "BA_ABBREV" FROM facilities')]

for ba in ba_abbrevs:
    if ba is not None:
        queries = ['SELECT facilities.BA_ABBREV, epa.OP_DATE, epa.OP_HOUR,',
                   'total(epa."GLOAD_(MW)") as RF_MW,',
                   'count(epa."GLOAD_(MW)") as N_UNITS_ON,',
                   'total(epa."CO2_MASS_(tons)")*2000 as RE_lb',
                   'FROM facilities JOIN epa',
                   'ON epa.ORISPL_CODE = facilities."_Facility_ID_(ORISPL)"',
                   'WHERE facilities.BA_ABBREV="%s"' % ba,
                   'GROUP BY epa.OP_DATE, epa.OP_HOUR'
                   #'LIMIT 10'
                 ]
        query = ' '.join(queries)
        print ba
        df = sql.read_frame(query, conn)
        df.to_csv('data/original/%s_aggregate_raw.csv' % ba, quoting=csv.QUOTE_NONNUMERIC)