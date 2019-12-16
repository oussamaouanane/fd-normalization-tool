from SQLiteDB import SQLiteDB
from utils.FDManagement import *

class Normalization:

    def __init__(self,fdmanagement: FDManagement,db: SQLiteDB):
        self.__fdmanagement= fdmanagement
        self.__db=db

    def is3NF(self,relation):
        """

        """
        res=True
        fd=self.__fdmanagement.get_fd()
        keys_list = self.__fdmanagement.get_candidate_keys(relation)
        super_keys = self.__fdmanagement.get_super_keys(relation)
        keys=[]
        if (len(fd)!=0):
            for key in keys_list:
                for a in keys_list:
                    if a not in keys:
                        keys.append(a)

            for fdbis in fd:
                if(fdbis.get_relation()==relation):
                    if(not((fdbis.get_attributes_a() in super_keys) or fdbis.get_attributes_b() in keys)):
                        res=False
        else:
            res=True
        return res

    def isBNCF(self,relation):
        """

        """
        res = True
        fd = self.__fdmanagement.get_fd()
        super_keys = self.__fdmanagement.get_super_keys(relation)
        if (len(fd) != 0):
            for fdbis in fd:
                if (fdbis.get_relation() == relation):
                    if (not (fdbis.get_attributes_a() in super_keys)):
                        res = False
        else:
            res = True
        return res
