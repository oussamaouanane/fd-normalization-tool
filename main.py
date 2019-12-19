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


def switch_choices(choice, fd):
    switch = {
        1: display_fd,
        2: display_keys,
        3: remove_non_fd,
        4: add_fd,
        5: modify_fd,
        6: remove_fd,
        7: remove_equivalence_transitive_fd,
        8: commit_changes,
        9: check_BCNF,
        10: export_3NF,
        11: leave
    }
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
    relation = input("Enter the name of the relation: ")
    print("The super keys of the relation {} are: ".format(relation), end="")
    super_keys = fd.get_super_keys(relation)
    print(super_keys)
    print("The candidates keys of the relation {} are: ".format(relation), end="")
    print(fd.get_candidate_keys(relation, super_keys))


"""
Displays all the non-functional dependencies of the FuncDep relation and lets the user to delete them.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def remove_non_fd(fd: FDManagement):
    non_fd = fd.all_non_fd()
    if len(non_fd) == 0:
        print("No non functional dependencies")
    else:
        for f_d in non_fd:
            print(f_d)
        remove = input("Remove all? ")
        if remove.lower() == "yes" or remove.lower() == "y":
            for f_d in non_fd:
                fd.remove_fd(f_d)


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


"""
Save the changes did to the FuncDep.
:param fd: FDManagement object that contains all the functional dependencies.
"""


def commit_changes(fd: FDManagement):
    sqli = fd.get_db()
    sqli.save()


def check_BCNF(fd: FDManagement):
    norm = Normalization(fd)
    print()
    check_3NF(fd, norm)


def check_3NF(fd: FDManagement, normalization: Normalization, relation) -> bool:
    return normalization.is_3NF(relation)


def export_3NF(fd: FDManagement):
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


"""
Exit the application
"""


def leave():
    working = False
    raise SystemExit


def choices(fd):
    while working:
        print("Choose an action\n-------------------------------\n")
        print("1. Display the FuncDep relation")
        print("2. Display the keys")
        print("3. Check if all the fields of FuncDep are functional dependencies")
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
        switch_choices(choice, fd)


if __name__ == '__main__':
    database = get_database()
    sql_db = SQLiteDB(database)
    cursor = sql_db.csr()
    FDmg = FDManagement(fd_from_db(cursor), sql_db)
    choices(FDmg)
    sql_db.close()
