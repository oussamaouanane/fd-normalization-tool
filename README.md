# Functional dependencies and normalization management tool

Multitasking tool that allows you to manipulate databases (.db format). You must include within your database a 'FuncDep' relation where the attributes are table_name,lhs,rhs, each row represents a singular functional dependency (separate your elements in lhs with a space).

This project was made for the Databases I course at the University of Mons.

## Usage

```bash
python main.py -f nameofyourfile(.db)
```

The tool will automatically read your database and lets you manipulate it. Deleting the unnecessary functionnal and the non-functionnal dependencies is needed to display the keys or check the 3NF/BCNF normalization/export in 3NF normalized form.

## Source

- D. Maier: The Theory of Relational Databases

## Problems

The decomposition doesn't work as wanted, the algorithm is correct but we had issues in the export part with SQLite.

## Authors

Written by Oussama Ouanane and Guillaume Kerckhofs
