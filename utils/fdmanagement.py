import copy

from SQLiteDB import SQLiteDB
from itertools import combinations


class FunctionalDependency:
    """
    Model a functional dependency A --> B in a R relation where (A,B,R) are represented by (attributesA, attributesB,
    relation) and A,B are sets
    """

    def __init__(self, row):
        self.__relation = row[0]
        self.__attributesA = row[1].split(" ")
        self.__attributesB = row[2].split(" ")

    def get_relation(self) -> str:
        return self.__relation

    def get_attributes_a(self) -> list:
        return self.__attributesA

    def get_attributes_b(self) -> list:
        return self.__attributesB

    def __str__(self):
        return "Relation: " + self.get_relation() + " FD: " + ",".join(self.get_attributes_a()) + " --> " + "".join(
            self.get_attributes_b()) + "\n"


"""
Compare two FunctionalDependency objects to check if they are equal.
:return: Whether the functional dependencies are equals or not.
"""


def compare_fd(fd1: FunctionalDependency, fd2: FunctionalDependency) -> bool:
    return (sorted(fd1.get_attributes_a()) == sorted(fd2.get_attributes_a())) and (
            sorted(fd1.get_attributes_b()) == sorted(fd2.get_attributes_b())) and (
                   fd2.get_relation() == fd1.get_relation())


class FDManagement:
    """
    Model a set of functional dependencies (cf. FunctionalDependency)
    """

    # Private list that contains the FunctionalDependency objects
    __fdObjects = []

    def __init__(self, fd, db: SQLiteDB):
        self.__fd = fd
        self.__db = db
        self.generate_fd()

    """
    Turns an array of functional dependencies into an array of FunctionalDependency objects and delete the 
    duplicates (cf. check_duplicates).
    """

    def generate_fd(self):
        for df in self.__fd:
            self.__fdObjects.append(FunctionalDependency(df))

    def get_fd(self):
        return self.__fdObjects

    def get_db(self):
        return self.__db

    """
    Finds the direct closure of a set of attributes
    :param attributes: str that represents the set of attributes
    """

    def direct_closure(self, relation, attributes):
        fd_in_relation = []
        for fd in self.get_fd():
            lhs = fd.get_attributes_a()
            rhs = fd.get_attributes_b()
            if lhs == attributes and fd.get_relation == relation:
                fd_in_relation.append(rhs)
        return "," + ",".join(fd_in_relation)

    """
    Finds the closure of a set of attributes
    :param attributes: str that represents the set of attributes
    """

    def closure(self, relation, attributes):
        old_dep = set()
        # {A_1,A_2,...}
        new_dep = set(attributes)
        f = set()

        for fd in self.get_fd():
            if relation == fd.get_relation():
                f.add(fd)

        while new_dep != old_dep:
            old_dep = new_dep
            for f_d in f:
                if new_dep.issuperset(set(f_d.get_attributes_a())):
                    new_dep = new_dep.union(set(f_d.get_attributes_b()))
        return list(new_dep)

    """
    Finds all the super keys.
    :return: Array of arrays that represents a set of super keys.
    """

    def get_super_keys(self, relation):
        super_keys = []
        super_keys_return = []
        all_possible_combinations = []

        attributes = self.get_db().get_attributes(relation)
        attributes.sort()

        for index in range(0, len(attributes) + 1):
            for comb in list(combinations(attributes, index + 1)):
                all_possible_combinations.append(list(comb))

        for attribute in all_possible_combinations:
            closure = self.closure(relation, attribute)
            closure.sort()
            if closure == attributes:
                super_keys.append(attribute)

        for attr in super_keys:
            super_keys_return.append(attr)
        return super_keys_return

    """
    Finds all the candidates keys.
    :return: Array of arrays that represents a set of candidates keys.
    """

    def get_candidate_keys(self, relation, super_keys):
        candidate_keys = super_keys
        for k in candidate_keys:
            k.sort()
        candidate_keys.sort()
        tmp = 0
        while tmp != len(candidate_keys):
            tmp = len(candidate_keys)
            for key in candidate_keys:
                for key_2 in candidate_keys:
                    check = True
                    for attribute in key:
                        if (attribute not in key_2) and check:
                            check = False
                    if check:
                        if len(list(key)) > len(list(key_2)):
                            candidate_keys.remove(key)
                        elif len(list(key_2)) > len(list(key)):
                            candidate_keys.remove(key_2)
            candidate_keys.reverse()
        return candidate_keys

    """
    Removes all the duplicates FunctionalDependency objects in __fdObjects and from the relation.
    """

    def remove_duplicates(self):
        delete: list[FunctionalDependency] = []
        for i in range(len(self.get_fd())):
            for j in range(len(self.get_fd())):
                if i == j:
                    continue
                if compare_fd(self.get_fd()[i], self.get_fd()[j]) and (
                        self.get_fd()[i] not in delete and self.get_fd()[j] not in delete):
                    delete.append(self.get_fd()[j])
        for fd in delete:
            self.remove_fd(fd)

    """
    Removes all the useless (X->Y where Y is a subset of X or empties) FunctionalDependency objects in __fdObjects and from 
    the relation FuncDep. 
    """

    def remove_useless(self):
        delete: list[FunctionalDependency] = []
        for fd in self.get_fd():
            if fd.get_attributes_b() in fd.get_attributes_a() or (
                    fd.get_attributes_a() == "" or fd.get_attributes_b() == ""):
                delete += fd
        for fd in delete:
            self.remove_fd(fd)

    """
    Removes all the transitive FunctionalDependency objects in __fdObjects and from the relation FuncDep.
    """

    def remove_transitive(self, relation):
        transitive = []
        for fd in self.get_fd():
            for fd2 in self.get_fd():
                if fd2.get_attributes_a() == fd.get_attributes_b() and fd2.get_relation() == fd.get_relation() == relation:
                    transitive.append(FunctionalDependency(
                        (relation, " ".join(fd.get_attributes_a()), " ".join(fd2.get_attributes_b()))))

        # Remove duplicate
        transitive = set(transitive)
        for fd in self.get_fd():
            for f_d in transitive:
                if compare_fd(f_d, fd):
                    self.remove_fd(fd)

    def remove_all_transitive(self, relations):
        for relation in relations:
            self.remove_transitive(relation)

    """
    Adds a functional dependency in __fdObjects and in the FuncDep relation.
    :param fd: FunctionalDependency object that will be add.
    """

    def add_fd(self, fd: FunctionalDependency):
        self.get_fd().append(fd)
        self.get_db().csr().execute(
            "INSERT INTO FuncDep VALUES('{}','{}','{}')".format(fd.get_relation(),
                                                                " ".join(fd.get_attributes_a()),
                                                                " ".join(fd.get_attributes_b())))

    """
    Replaces a functional dependency by another in __fdObjects and in the FuncDep relation.
    :param fd: FunctionalDependency object that will be add.
    """

    def modify_fd(self, fd1, fd2):
        for fd in self.get_fd():
            if compare_fd(fd, fd1):
                fd1 = fd
                break

        self.get_fd()[self.get_fd().index(fd1)] = fd2
        self.get_db().csr().execute("UPDATE FuncDep SET table_name='{}',lhs='{}',rhs='{}' WHERE table_name='{}' AND "
                                    "lhs='{}' AND rhs='{}'".format(fd2.get_relation(),
                                                                   " ".join(fd2.get_attributes_a()),
                                                                   " ".join(fd2.get_attributes_b()),
                                                                   fd1.get_relation,
                                                                   " ".join(fd1.get_attributes_a()),
                                                                   " ".join(fd1.get_attributes_b())))

    def remove_fd(self, fd: FunctionalDependency):
        for fd1 in self.get_fd():
            if compare_fd(fd, fd1):
                fd = fd1
                break

        self.get_fd().remove(fd)
        self.get_db().csr().execute(
            "DELETE FROM FuncDep WHERE table_name='{}' AND lhs='{}' AND  rhs='{}'".format(fd.get_relation(),
                                                                                          " ".join(
                                                                                              fd.get_attributes_a()),
                                                                                          " ".join(
                                                                                              fd.get_attributes_b())))

    def __str__(self):
        tmp = "Here is the list of the functional dependencies:\n\n"
        for df in self.get_fd():
            tmp += df.__str__()
        return tmp
