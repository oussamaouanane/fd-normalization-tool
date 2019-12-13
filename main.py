import argparse
import os.path
import SQLiteDB

"""
Interface CLI permettant d'effectuer à l'utilisateur de pouvoir exécuter toutes les fonctions interactivement.
"""

status = True

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", type=str, required=True, help="name of the .db file")
database = parser.parse_args().file

# Rajoute l'extension .db si l'utilisateur l'oublie
if not (database.lower().endswith('.db')):
    database += ".db"

# Vérifie que le fichier existe
if os.path.isfile(database):
    print("Fichier trouvé\n")
else:
    print("Fichier non retrouvé")
    raise SystemExit

 #sqlDb = SQLiteDB(database)

def choices(integer):
    switch = {
        1: execute,
        8: exit()
    }

    return switch.get(integer, "Cette option n'existe pas")()

def execute(command):
    print()
    #sqlDb.execute(command)
def quit():
    status = False

while(status):
    print("Sélectionner une action\n-------------------------------\n")
    print("1. Exécuter une commande SQL")
    print("8. Quitter l'application")

    choice = 0
    while not (0 < choice < 10):
        choice = int(input())
        if not 0 < choice < 10:
         print("Action inconnue")
    choices(choice)






