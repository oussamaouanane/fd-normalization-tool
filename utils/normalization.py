from SQLiteDB import SQLiteDB
from utils.fdmanagement import *


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
        if(relation!="FuncDep"):
            cursor = self.connection.cursor()
            keys_list=self.__fdmanagement.get_candidate_keys(relation)
            """keys_list[0] est le premier tableau de la base de données """
            fd=self.__fdmanagement.get_fd()
            attributes=[]
            for fdbis in fd:
                att=fdbis.get_attributes_a()
                if att not in attributes:
                    attributes.append(att)
            compo_table=[]
            for att in attributes:
                compo_table=direct_closure(relation,att)
            """fd.get_db().csr().execute("INSERT INTO {} SELECT {} FROM {}".format(nouvelle_relation, attributes+fd.direct_closure(relation, attributes), relation))
            crée et met les valeurs de l'ancienne table dans la nouvelle
            """


