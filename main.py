from load import *
from transform import *
from extract import *
import petl as etl
import pandas as pd
import sys
import os
from config.database import connect
import distance

def main():
    sys.setrecursionlimit(10000)
    #table = etl.frompickle("out/2016.pkl")
    #print(etl.nrows(etl.select(table, lambda row: row["town_code"] == "C030" and row["date"] == "2016-07-12")))
    
    #db_connection = connect()
    #path = "/home/tamssaout/Bureau/data/new_data/BDD Rotations 2018-2019.xlsx"
    #print("data extraction")
    #data, sheets = extract_data_from_file(path)
    #table = transform_rotation_data(data, sheets)
    #etl.topickle(table, "out/2018-20191.pkl")
    #print("end")
    for file in os.listdir("out/"):
        path = "out/" + file
        print(path)
        table = etl.frompickle(path)
        print(etl.nrows(table))
        load_rotations(table)
    print("end")

    #rotations_table = []
    #dirs = ["/home/tamssaout/Téléchargements/2021/corso/", "/home/tamssaout/Téléchargements/2021/hamici/"]
    #for dir in dirs:
    #    if dirs.index(dir) == 0:
    #        cet = "CORSO"
    #    else:
    #        cet = "HAMICI"
    #    for file in os.listdir(dir):
    #        print(file)
    #        path = dir + file
    #        data, sheets = extract_data_from_file(path)
    #        table = transform_rotation_data(data, sheets)
    #        table = etl.convert(table, 'cet', {"nan": cet})
    #        rotations_table = concat_table(rotations_table, table, rotations_table_header)
    #        
    #etl.topickle(rotations_table, "out/20211.pkl")
    #print("end")
    
    #table = etl.frompickle("out/20211.pkl")
    #print(etl.nrows(table))
    #etl.tocsv(table, "out/data_2020.csv")

    #path = "/home/tamssaout/Bureau/data/new_data/Parc extranet.xlsx"
    #data, sheets = extract_data_from_file(path)
    #table = structure_vehicles_data(data, sheets)
    #load_vehicles(table)
    #print("end")

    



if __name__ == "__main__":
    main()