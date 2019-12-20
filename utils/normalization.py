from SQLiteDB import SQLiteDB
from utils.fdmanagement import *
import sqlite3


class Normalization:

    def __init__(self, fdmanagement: FDManagement):
        self.__fd_mng = fdmanagement
        self.__db = fdmanagement.get_db()

    """
    Returns whether a relation is in 3NF form or not
    :param relation: str that represents the relation name
    """

    def is_3nf(self, relation):
        res = True
        fd = self.__fd_mng.get_fd()
        keys_list = self.__fd_mng.get_candidate_keys(relation, self.__fd_mng.get_super_keys(relation))
        keys = []
        if len(fd) != 0:
            for key in keys_list:
                for a in key:
                    if a not in keys:
                        keys.append(a)

            for fdbis in fd:
                if fdbis.get_relation() == relation:
                    if not ((self.__fd_mng.closure(relation, fdbis.get_attributes_a()) == self.__fd_mng.get_db().get_attributes(
                            relation)) or fdbis.get_attributes_b() in keys):
                        res = False
        return res

    """
    Returns whether a relation is in BCNF form or not
    :param relation: str that represents the relation name
    """

    def is_bcnf(self, relation):
        res = True
        fd = self.__fd_mng.get_fd()
        super_keys = self.__fd_mng.get_super_keys(relation)
        if len(fd) != 0:
            for fdbis in fd:
                if fdbis.get_relation() == relation:
                    if not (self.__fd_mng.closure(relation, fdbis.get_attributes_a()) == self.__fd_mng.get_db().get_attributes(
                            relation)):
                        res = False
        return res

    """
    Creates a new database normalized in 3NF form
    :param relation: str that represents the relation name
    :param database_name: str that represents the name of the new database, decomposition by default
    """

    def decompostion_3nf(self, relation, database_name="decomposition.db"):
        if relation != "FuncDep":
            keys_list = self.__fd_mng.get_candidate_keys(relation, self.__fd_mng.get_super_keys(relation))
            fd = self.__fd_mng.get_fd()
            attributes = {}
            for fdbis in fd:
                att = fdbis.get_attributes_a()
                if att not in attributes:
                    attributes[att] = self.__fd_mng.direct_closure(relation, att)
            new_attributes = copy.deepcopy(attributes)
            tmp_size = 0
            lhs = []
            for att in fd:
                tmp = att.get_attributes_a()
                lhs.append(tmp)
            while len(lhs) != tmp_size:
                tmp_size = len(lhs)
                for a in lhs:
                    for b in lhs:
                        if set(b).issubset(set(a)):
                            new_attributes[a] = new_attributes[a] + " " + new_attributes[b]
                            del new_attributes[b]
                            lhs.remove(b)
                            break
                    break
            new_db = sqlite3.connect(database_name)
            cursor = new_db.cursor()

            cursor.execute("""INSERT INTO {} VALUES {}""".format("main",
                                                                       self.__db.execute("SELECT {} FROM {}").format(
                                                                           ",".join(keys_list[0]), relation)))
            for table in new_attributes.values():
                cursor.executes("""INSERT INTO {} VALUES {}""".format(table, self.__db.execute(
                    "SELECT {} FROM {}").format(table + new_attributes[table], relation)))
                for fdd in fd:
                    tmp = fdd.get_attributes_a()
                    if set(tmp).issubset(set(table)):
                        cursor.execute(
                            "INSERT INTO FuncDep VALUES('{}','{}','{}')".format(table, " ".join(fdd.get_attributes_a()),
                                                                                " ".join(fdd.get_attributes_b())))
                        fd.remove(fdd)
            new_db.commit()
