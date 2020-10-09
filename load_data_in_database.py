############################################
# NO check for errors is done in this file #
############################################
import sys
from io import StringIO
import json
import psycopg2 as pg

from config import default_info

def get_value(value, type_cast):
    try:
        return type_cast(value)
    except:
        return None

def _load_postgres_js_with_tool(info):
    conn = pg.connect(
        host=info['host'],
        port=info['port'],
        user=info['user'],
        password=info['password'],
        database=info['database']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    collection_name = info['collection_name']
    try:
       cursor.execute(
           'DROP TABLE "{}"'
               .format(collection_name, collection_name)
       )
       conn.commit()
    except:  pass
    cursor.execute(
        'CREATE TABLE "{}" ({} JSONB)'
               .format(collection_name, collection_name)
    )
    conn.commit()
    with open(info['filename'], 'r', encoding='utf-8') as f:
        print(' * Reading records', file=sys.stderr)
        buffer = StringIO()
        for n, r in enumerate(f):
            tk = r.strip().split('|')
            js = {
                   "Number_of_Records": get_value(tk[0], int),
                   "activity_sec": get_value(tk[0], int),
                   "application": get_value(tk[2], str),
                   "device": get_value(tk[3], str),
                   "subscribers": get_value(tk[4], int),
                   "volume_total_bytes": get_value(tk[5], float)
            }
            buffer.write(json.dumps(js) + '\n')
            if (n % 1000) == 0:
                print("Inserting record {}\r".format(n), end='', file=sys.stderr)
                sys.stderr.flush()
                buffer.seek(0)
                cursor.copy_from(buffer, '"{}"'.format(collection_name))
                conn.commit()
                buffer = StringIO()
        if buffer:
            buffer.seek(0)
            cursor.copy_from(buffer, '"{}"'.format(collection_name))
            conn.commit()


if __name__ == '__main__':
    _load_postgres_js_with_tool(default_info)
