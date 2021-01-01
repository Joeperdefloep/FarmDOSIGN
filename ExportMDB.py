#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 14:46:54 2020
based on https://stackoverflow.com/a/42226003
requires mdbtools to be installed. `sudo apt install mdbtools`
@author: joempie
"""
import sys, subprocess, os
from io import StringIO
import pandas as pd
from pathlib import Path
VERBOSE = True

def mdb_to_pandas(database_path):
    subprocess.call(["mdb-schema", database_path, "mysql"])
    # Get the list of table names with "mdb-tables"
    table_names = subprocess.Popen(["mdb-tables", "-1", database_path],
                                   stdout=subprocess.PIPE).communicate()[0]
    tables = table_names.splitlines()
    sys.stdout.flush()
    # Dump each table as a stringio using "mdb-export",
    out_tables = {}
    for rtable in tables:
        table = rtable.decode()
        if VERBOSE: print('running table:',table)
        if table != '':
            if VERBOSE: print("Dumping " + table)
            contents = subprocess.Popen(["mdb-export", database_path, table],
                                        stdout=subprocess.PIPE).communicate()[0]
            temp_io = StringIO(contents.decode())
            print(table, temp_io)
            out_tables[table] = pd.read_csv(temp_io)
    return out_tables

inputDB = mdb_to_pandas(Path('./Input/FarmDB.mdb'))

print(inputDB)