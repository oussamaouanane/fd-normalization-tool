from sqlite3 import connect


class SQLiteDB:
    """
    SQLite3 methods reimplemented to shorten the process
    """

    cnx = None

    def __init__(self, database_name):
        self.file(database_name)

    def file(self, database_name):
        self.cnx = connect(database_name)

    def csr(self):
        return self.cnx.cursor()

    def save(self):
        self.cnx.commit()

    def cancel(self):
        self.cnx.rollback()

    def close(self):
        self.csr().close()
        self.cnx.close()
