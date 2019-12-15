from SQLiteDB import SQLiteDB

sql = SQLiteDB("../database.db")
curs = sql.csr()

curs.execute('SELECT * from funcDep')
for row in curs.fetchall():
    print(row)