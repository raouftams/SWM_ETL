from load import *
from transform import *
from extract import *

def main():
    path = "D:\PFE M2\data\\new_data\Parc extranet.xlsx"
    df, sheets = extract_data_from_file(path)
    table = structure_vehicles_data(df, sheets)
    load_vehicles(table)
    print("end")


if __name__ == "__main__":
    main()