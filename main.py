from load import *
from transform import *
from extract import *
import petl as etl
import pandas as pd
import sys


def main():
    sys.setrecursionlimit(10000)
    
    path = "D:\PFE M2\data\\new_data\BDD Rotations 2018-2019.xlsx"
    #data, sheets = extract_data_from_file(path)
    #table = transform_rotation_data(data, sheets)
    #etl.topickle(table, "out/rotations_table.pkl")
    table = etl.frompickle("out/rotations_table.pkl")
    
    print(etl.nrows(table))
    load_rotations(table)
    print("end")


if __name__ == "__main__":
    main()