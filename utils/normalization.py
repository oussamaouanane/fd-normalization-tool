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
                    closure =self.__fd_mng.closure(relation, fdbis.get_attributes_a())
                    closure.sort()
                    rhs=(",".join(fdbis.get_attributes_b()))
                    if not ((closure == self.__fd_mng.get_db().get_attributes(relation))
                            or rhs in keys):
                        res = False
        return res

    """
    Returns whether a relation is in BCNF form or not
    :param relation: str that represents the relation name
    """

    def is_bcnf(self, relation):
        res = True
        fd = self.__fd_mng.get_fd()
        if len(fd) != 0:
            for fdbis in fd:
                if fdbis.get_relation() == relation:
                    closure = self.__fd_mng.closure(relation, fdbis.get_attributes_a())
                    closure.sort()
                    if not (closure== self.__fd_mng.get_db().get_attributes(
                            relation)):
                        res = False
        return res

    """
    Creates a new database normalized in 3NF form
    :param relation: str that represents the relation name
    :param database_name: str that represents the name of the new database, decomposition by default
    """

    def decompostion_3nf(self, relation):
        if relation != "FuncDep":
            fd = self.__fd_mng.get_fd()
            keys_list = self.__fd_mng.get_candidate_keys(relation, self.__fd_mng.get_super_keys(relation))
            if len(fd)!=0:
                attributes = {}
                for fdbis in fd:
                    if fdbis.get_relation()==relation:
                        att = ",".join(fdbis.get_attributes_a())
                        if att not in attributes:
                            attributes[att] = self.__fd_mng.direct_closure(relation, att)

                    else:
                        fd.remove(fdbis)
            new_attributes = copy.deepcopy(attributes)
            tmp_size = 0
            lhs = []
            for att in fd:
                if fdbis.get_relation() == relation:
                    tmp = ",".join(att.get_attributes_a())
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



            print("""La table main est composé de {}""".format( ",".join(keys_list[0])))

            for table in new_attributes.values():
                print("""La table: {} est composé de {}""".format(table,table + new_attributes[table]))
                for fdd in fd:
                    tmp = fdd.get_attributes_a()
                    if set(tmp).issubset(set(table)):
                        cursor.execute(
                            "La fd de la table :{} est {} --> {})".format(table, ",".join(fdd.get_attributes_a()),
                                                                                ",".join(fdd.get_attributes_b())))
                        fd.remove(fdd)
