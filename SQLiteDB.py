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

    def get_relations(self):
        rel = []
        relations = self.csr().execute("SELECT name FROM sqlite_master WHERE type='table';")
        for relation in relations:
            rel.append(relation[0])
        return rel

    def get_attributes(self, relation):
        att = []
        attributes = self.csr().execute("pragma table_info({})".format(relation))
        for attribute in attributes:
            att.append(attribute[1])
        return att

    def close(self):
        self.csr().close()
        self.cnx.close()
