import argparse
import os.path
from SQLiteDB import SQLiteDB
from utils.fdmanagement import *
from utils.normalization import *

"""
Let the user to execute the different functions of the project from a list of options.
"""

database = None
working = True
check = [False, False]


def check_done():
    if check == [True, True]:
        return True
    return False


"""
Gets the user's database name after the parameter '-f' w/ argparse.
:return: Name of the user's database.
"""


def get_database_name():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", type=str, required=True, help="name of the .db file")
    return parser.parse_args().file


"""
Checks if the username forgot the '.db' extension at the end of his input and if the file exists.
:return: Name of the user's database if it exists.
"""


def get_database():
    db = get_database_name()
    if not (db.lower().endswith('.db')):
        db += ".db"
    if os.path.isfile(db):
        print("File found.\n")
    else:
        print("File not found.\n")
        raise SystemExit
    return db


"""
Executes the user action.
:param choice: Integer that represents the user action.
:return: The function associated with the user action.
"""


def switch_choices(choice, fd, csr):
    switch = {
        1: display_fd,
        2: remove_non_fd,
        3: display_keys,
        4: add_fd,
        5: modify_fd,
        6: remove_fd,
        7: remove_equivalence_transitive_fd,
        8: commit_changes,
        9: check_BCNF,
        10: export_3NF,
        11: leave
    }
    if choice == 2:
        return switch.get(choice)(fd, csr)
    if choice != 11:
        return switch.get(choice)(fd)
    return switch.get(choice)()


"""
Transforms the rows of the FuncDep relation into an array of array(s) that represents each row.
:param sql_db: SQLiteDB object.
:return: Array of array(s) that represents each rows.
"""


def fd_from_db(csr):
    df = []
    csr.execute('SELECT * FROM FuncDep')
    for row in csr.fetchall():
        df.append(row)
    return df


# All the actions

"""
Displays all the functional dependencies of the FuncDep relation.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def display_fd(fd: FDManagement):
    print(fd)


"""
Displays all the keys (super and candidates) of a relation.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def display_keys(fd: FDManagement):
    if check_done():
        relation = input("Enter the name of the relation: ")
        super_keys = fd.get_super_keys(relation)
        print("Number of super keys:", len(super_keys))
        print(super_keys)
        print("Number of candidate keys:", len(fd.get_candidate_keys(relation, super_keys)))
        print(fd.get_candidate_keys(relation, super_keys))
    print("Make sure you remove trivial fd and non-fd first!")

"""
Finds all the non functional dependencies in the FuncDep relation.
:return: Array of FunctionalDependency objects that are not functional dependencies.
"""


def all_non_fd(fd: FDManagement, csr):
    non_fd = []
    existent = {}
    for f_d in fd.get_fd():
        csr.execute(
            "SELECT {} FROM {}".format(",".join(f_d.get_attributes_a() + f_d.get_attributes_b()), f_d.get_relation()))
        for row in csr.fetchall():
            concat_a = ""
            concat_b = ""
            for X in range(len(f_d.get_attributes_a())):
                # T[X]
                concat_a += str(row[X])
            tmp = len(f_d.get_attributes_a())
            tmp2 = len(row)
            for Y in range(tmp, tmp2):
                # T[Y]
                concat_b += str(row[Y])
            if concat_a in existent:
                if existent.get(concat_a) != concat_b and f_d not in non_fd:
                    non_fd.append(f_d)
            else:
                existent[concat_a] = concat_b
    return non_fd


"""
Displays all the non-functional dependencies of the FuncDep relation and lets the user to delete them.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def remove_non_fd(fd: FDManagement, csr):
    non_fd = all_non_fd(fd, csr)
    if len(non_fd) == 0:
        print("No non functional dependencies")
    else:
        for f_d in non_fd:
            print(f_d)
        remove = input("Remove all? ")
        if remove.lower() == "yes" or remove.lower() == "y":
            for f_d in non_fd:
                fd.remove_fd(f_d)
            check[0] = True


"""
Adds a functional dependency to the FuncDep.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def add_fd(fd: FDManagement):
    func_dep = FunctionalDependency(tuple(input("Enter your functional dependency with this syntax. table_name,lhs,"
                                                "rhs: ").split(",")))
    fd.add_fd(func_dep)
    display_fd(fd)


"""
Modifies a functional dependency in the FuncDep by another.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def modify_fd(fd: FDManagement):
    func_dep1 = FunctionalDependency(tuple(input("Enter your existing functional dependency with this syntax. "
                                                 "table_name,lhs, rhs: ").split(",")))
    func_dep2 = FunctionalDependency(tuple(input("Enter your new functional dependency with this syntax. "
                                                 "table_name,lhs, rhs: ").split(",")))
    fd.modify_fd(func_dep1, func_dep2)
    display_fd(fd)


"""
Removes a functional dependency of the FuncDep.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def remove_fd(fd: FDManagement):
    func_dep = FunctionalDependency(tuple(input("Enter your functional dependency with this syntax. table_name,lhs,"
                                                "rhs: ").split(",")))
    fd.remove_fd(func_dep)
    display_fd(fd)


"""
Removes useless functional dependencies.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def remove_equivalence_transitive_fd(fd: FDManagement):
    fd.remove_duplicates()
    fd.remove_useless()
    fd.remove_all_transitive(fd.get_db().get_relations())
    display_fd(fd)
    check[1] = True


"""
Save the changes did to the FuncDep.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def commit_changes(fd: FDManagement):
    sqli = fd.get_db()
    sqli.save()


def check_BCNF(fd: FDManagement):
    if check_done():
        norm = Normalization(fd)
        relation = input("Enter the name of the relation: ")
        check_3NF(norm, relation)
    print("Make sure you remove trivial fd and non-fd first!")


def check_3NF(normalization: Normalization, relation) -> bool:
    return normalization.is_3NF(relation)


def export_3NF(fd: FDManagement):
    if check_done():
        norm = Normalization(fd)
        normalized = True
        for relation in fd.get_db().get_relations():
            if not check_3NF(fd, norm, relation):
                normalized = False
        if normalized:
            print("Already in 3NF")
        else:
            # TODO: Call the 3NF decomposition function
            pass
    else:
        print("Make sure you remove trivial fd and non-fd first!")


"""
Exit the application
"""


def leave():
    raise SystemExit


def choices(fd, csr):
    while working:
        print("Choose an action\n-------------------------------\n")
        print("1. Display the FuncDep relation")
        print("2. Check if all the fields of FuncDep are functional dependencies")
        print("3. Display the keys")
        print("4. Add a functional dependency")
        print("5. Modify a functional dependency")
        print("6. Remove a functional dependency")
        print("7. Remove transitive (trivial) and equivalent functional dependencies (unnecessary)")
        print("8. Commit all the changes in the database")
        print("9. Check if the relation is in BCNF or 3NF")
        print("10. Export relation in 3NF form")
        print("11. Leave the application\n")

        choice = 0
        while not (0 < choice < 12):
            choice = int(input("Enter an action: "))
            if not 0 < choice < 12:
                print("Unknown action")
        switch_choices(choice, fd, csr)


if __name__ == '__main__':
    database = get_database()
    sql_db = SQLiteDB(database)
    cursor = sql_db.csr()
    FDmg = FDManagement(fd_from_db(cursor), sql_db)
    choices(FDmg, cursor)
    sql_db.close()
