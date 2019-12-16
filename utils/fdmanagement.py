from SQLiteDB import SQLiteDB


class FunctionalDependency:
    """
    Model a functional dependency A --> B in a R relation where (A,B,R) are represented by (attributesA, attributesB,
    relation) and A,B are sets
    """

    def __init__(self, row):
        self.__relation = row[0]
        self.__attributesA = row[1]
        self.__attributesB = row[2]

    def get_relation(self) -> str:
        return self.__relation

    def get_attributes_a(self) -> str:
        return self.__attributesA

    def get_attributes_b(self) -> str:
        return self.__attributesB

    def __str__(self):
        return "".join(self.__attributesA) + " --> " + "".join(self.__attributesB) + "\n"


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
    Finds the closure of a set of attributes
    :param attributes: str that represents the set of attributes
    """

    def closure(self, relation, attributes):
        fdinrelation = []
        for fd in self.get_fd():
            if fd.get_relation == relation:
                fdinrelation.append(fd)

        # Will help to find the state where attr stays still
        attr_compare = []
        attr = list(attributes)

        while attr_compare != attr:
            attr_compare = attr
            for fd in fdinrelation:
                if all(x in attr for x in fd.get_attributes_a):
                    attr = list(set(attr).union(set(fd.get_attributes_b)))
        return attr

    """
    Finds all the super keys.
    :return: Array of arrays that represents a set of super keys.
    """

    def get_super_keys(self, relation):
        pass

    """
    Finds all the candidates keys.
    :return: Array of arrays that represents a set of candidates keys.
    """

    def get_candidate_keys(self):
        pass

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
            if fd.get_attributes_b in fd.get_attributes_a or (fd.get_attributes_a == "" or fd.get_attributes_b == ""):
                delete += fd
        for fd in delete:
            self.remove_fd(fd)

    """
    Removes all the transitive FunctionalDependency objects in __fdObjects and from the relation FuncDep.
    """

    def remove_transitive(self, relation):
        delete: list[FunctionalDependency] = []
        for fd in self.get_fd():
            closure = self.closure(relation, fd.get_attributes_a)
            if all(x in closure for x in fd.get_attributes_b):
                delete.append(fd)
        for fd in delete:
            self.remove_fd(fd)

    def remove_all_transitive(self, relations):
        for relation in relations:
            self.remove_transitive(relation)

    """
    Finds all the non functional dependencies in the FuncDep relation.
    :return: Array of FunctionalDependency objects that are not functional dependencies.
    """

    def all_non_fd(self):
        nonfd = []
        existent = {}
        for fd in self.get_fd():
            self.get_db().csr().execute("SELECT" + ",".join(list(fd.get_attributes_a() + fd.get_attributes_b)) + " FROM " + fd.get_relation)
            for row in self.get_db().csr().fetchall():
                concata = ""
                concatb = ""
                for i in range(len(fd.get_attributes_a())):
                    concata += row[i]
                for j in range(len(fd.get_attributes_a()),len(row)-1):
                    concatb += row[j]
                if concata in existent:
                    if existent.get(concata) != concatb and fd not in nonfd:
                        nonfd.append(FunctionalDependency(fd))
                        break
                else:
                    existent[concata] = concatb
        return nonfd

    """
    Adds a functional dependency in __fdObjects and in the FuncDep relation.
    :param fd: FunctionalDependency object that will be add.
    """

    def add_fd(self, fd: FunctionalDependency):
        self.get_fd().append(fd)
        self.get_db().csr().execute(
            "INSERT INTO FuncDep VALUES('{}','{}','{}')".format(fd.get_relation(),
                                                                fd.get_attributes_a(),
                                                                fd.get_attributes_b()))

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
                                    "lhs='{}' AND rhs='{}'".format(fd2.get_relation,
                                                                   fd2.get_attributes_a(),
                                                                   fd2.get_attributes_b(),
                                                                   fd1.get_relation,
                                                                   fd1.get_attributes_a(),
                                                                   fd1.get_attributes_b()))

    def remove_fd(self, fd: FunctionalDependency):
        for fd1 in self.get_fd():
            if compare_fd(fd, fd1):
                fd = fd1
                break

        self.get_fd().remove(fd)
        self.get_db().csr().execute(
            "DELETE FROM FuncDep WHERE table_name='{}' AND lhs='{}' AND  rhs='{}'".format(fd.get_relation,
                                                                                          fd.get_attributes_a(),
                                                                                          fd.get_attributes_b()))

    def __str__(self):
        tmp = "Here is the list of the functional dependencies:\n\n"
        for df in self.get_fd():
            tmp += df.__str__()
        return tmp
