from load import *
from transform import *
from extract import *

import petl as etl

def main():
    path = "out/test.csv"
    table = etl.fromcsv(path, encoding="utf-8")
    load_rotations(table)
    print("end")


if __name__ == "__main__":
    main()