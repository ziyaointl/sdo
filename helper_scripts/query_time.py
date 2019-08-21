import sqlite3

conn = sqlite3.connect('../sdo.db')
c = conn.cursor()
for row in c.execute('SELECT * FROM jobs'):
    print(row)
conn.close()
