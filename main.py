from load import *
from transform import *
from extract import *
import petl as etl
import pandas as pd
import sys


def main():
    sys.setrecursionlimit(10000)
    
    #path = "/home/tamssaout/Bureau/data/new_data/BDD Rotations 2018-2019.xlsx"
    #print("data extraction")
    #data, sheets = extract_data_from_file(path)
    #table = transform_rotation_data(data, sheets)
    #etl.topickle(table, "out/rotations_table2.pkl")
    
    table = etl.frompickle("out/rotations_table2.pkl")
    print(etl.nrows(table))
    load_rotations(table)
    print("end")


if __name__ == "__main__":
    main()