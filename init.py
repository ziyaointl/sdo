import sqlite3

conn = sqlite3.connect('sdo.db')
c = conn.cursor()
c.execute('CREATE TABLE retries (stage VARCHAR NOT NULL, times INT)')
conn.commit()
conn.close()
