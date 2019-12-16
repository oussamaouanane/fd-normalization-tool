from SQLiteDB import SQLiteDB
from utils.FDManagement import *

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
        super_keys = self.__fdmanagement.get_super_keys(relation)
        keys = []
        if len(fd) != 0:
            for key in keys_list:
                for a in keys_list:
                    if a not in keys:
                        keys.append(a)

            for fdbis in fd:
                if fdbis.get_relation() == relation:
                    if not ((fdbis.get_attributes_a() in super_keys) or fdbis.get_attributes_b() in keys):
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
                    if not (fdbis.get_attributes_a() in super_keys):
                        res = False
        return res

