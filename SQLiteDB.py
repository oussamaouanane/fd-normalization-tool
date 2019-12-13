from sqlite3 import connect

"""
Classe permettant de modéliser une base de donnée SQLite
"""


class SQLiteDB:
    cnx = None

    # def __init__(self, database_name):
    #     self.file(database_name)

    # def file(self, database_name):
    #     self.cnx = connect(database_name)
    #
    # def csr(self):
    #     return self.cnx.cursor()
    #
    # # Méthode permettant d'exécuter des commandes SQL
    # def exec(self, command):
    #     self.csr().execute(command)
    # # Méthode permettant d'enregistrer les changements
    # def save(self):
    #     self.cnx.commit()
    # # Méthode permettant d'annuler les derniers changements
    # def cancel(self):
    #     self.cnx.rollback()
