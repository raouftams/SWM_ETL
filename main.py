from load import *
from transform import *
from extract import *
import petl as etl
import pandas as pd
import sys
import os


def main():
    sys.setrecursionlimit(10000)
    
    #path = "/home/tamssaout/Bureau/data/new_data/BDD Rotations 2018-2019.xlsx"
    #print("data extraction")
    #data, sheets = extract_data_from_file(path)
    #table = transform_rotation_data(data, sheets)
    #etl.topickle(table, "out/test.pkl")
    #print("end")

    #table = etl.frompickle("out/2018-2019.pkl")
    #print(etl.nrows(table))
    #load_rotations(table)
    #print("end")

    #rotations_table = []
    #dirs = ["/home/tamssaout/Bureau/data/2017/HAMICI/", "/home/tamssaout/Bureau/data/2017/CORSO/"]
    #for dir in dirs:
    #    if dirs.index(dir) == 0:
    #        cet = "HAMICI"
    #    else:
    #        cet = "CORSO"
    #    for file in os.listdir(dir):
    #        path = dir + file
    #        data, sheets = extract_data_from_file(path)
    #        table = transform_rotation_data(data, sheets)
    #        table = etl.convert(table, 'cet', {"nan": cet})
    #        rotations_table = concat_table(rotations_table, table, rotations_table_header)
    #etl.topickle(rotations_table, "out/2017.pkl")
    #print("end")

    #table = etl.frompickle("out/2017.pkl")
    #print(etl.nrows(table))

    #path = "/home/tamssaout/Bureau/data/new_data/Parc extranet.xlsx"
    #data, sheets = extract_data_from_file(path)
    #table = structure_vehicles_data(data, sheets)
    #load_vehicles(table)
    #print("end")

    



if __name__ == "__main__":
    main()