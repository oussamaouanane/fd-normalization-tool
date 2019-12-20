from SQLiteDB import SQLiteDB
from utils.fdmanagement import *
import sqlite3


class Normalization:

    def __init__(self, fdmanagement: FDManagement):
        self.__fdmanagement = fdmanagement
        self.__db = fdmanagement.get_db()

    """
    Returns whether a relation is in 3NF form or not
    :param relation: str that represents the relation name
    """

    def is_3NF(self, relation):
        res = True
        fd = self.__fdmanagement.get_fd()
        keys_list = self.__fdmanagement.get_candidate_keys(relation)
        keys = []
        if len(fd) != 0:
            for key in keys_list:
                for a in key:
                    if a not in keys:
                        keys.append(a)

            for fdbis in fd:
                if fdbis.get_relation() == relation:
                    if not ((closure(relation,fdbis.get_attributes_a())==fd.get_db().get_attributes(relation)) or fdbis.get_attributes_b() in keys):
                        res = False
        return res

    """
    Returns whether a relation is in BCNF form or not
    :param relation: str that represents the relation name
    """

    def is_BCNF(self, relation):

        res = True
        fd = self.__fdmanagement.get_fd()
        super_keys = self.__fdmanagement.get_super_keys(relation)
        if len(fd) != 0:
            for fdbis in fd:
                if fdbis.get_relation() == relation:
                    if not (closure(relation,fdbis.get_attributes_a())==fd.get_db().get_attributes(relation)):
                        res = False
        return res

    """
    create a new database in 3NF form
    :param relation: str that represents the relation name
    :param database_name: str tha represents the name of the nieuw database, decomposition by default
    """

    def decompostion_3NF(self,relation,database_name="decomposition.db"):
        if relation!="FuncDep":
            keys_list=self.__fdmanagement.get_candidate_keys(relation)
            fd=self.__fdmanagement.get_fd()
            attributes={}
            for fdbis in fd:
                att=fdbis.get_attributes_a()
                if att not in attributes:
                    attributes[att]=direct_closure(relation,att)
            new_attributes=attributes.deepcopy()
            tmpSize=0
            lhs=[]
            for att in fd:
                tmp=att.get_attributes_a()
                lhs.append(tmp)
            while len(lhs) !=tmpSize:
                tmpSize=len(lhs)
                for a in lhs:
                    for b in lhs:
                        if set(b).issubset(set(a)):
                            new_attributes[a]=new_attributes[a]+" "+new_attributes[b]
                            del new_attributes[b]
                            lhs.remove(b)
                            break
                    break
            new_db=sqlite3.connect(database_name)
            cursor=new_db.cursor()

            cursor.executescript("""INSERT INTO {} VALUES {}""".format("main",self.__db.execute("SELECT {} FROM {}").format(",".join(keys_list[0]), relation)))
            for table in new_attributes.values():
                cursor.executescript("""INSERT INTO {} VALUES {}""".format(table,self.__db.execute("SELECT {} FROM {}").format(table+new_attributes[table],relation)))
                for fdd in fd:
                    tmp=fdd.get_attributes_a()
                    if set(tmp).issubset(set(table)):
                        cursor.execute("INSERT INTO FuncDep VALUES('{}','{}','{}')".format(table," ".join(fdd.get_attributes_a()), " ".join(fdd.get_attributes_b())))
                        fd.remove(fdd)