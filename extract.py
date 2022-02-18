import pandas as pd
import os


#reads an excel file and returns a dict of dataframes
#each dataframe is a sheet
def read_xlsx_file(path: str):
    """
    Reads an excel file with pandas and returns dict of dataframes.
    """
    df = pd.read_excel(path, sheet_name=None, header=0, dtype=str)
    return df

#returns a list of sheets names of a dict of dataframes 
#each dataframe is a sheet
def get_sheets(data):
    """
    Returns the keys of a dict of dataframes.
    """
    return data.keys()

#reads csv file and returns a dict of dataframes
#each dataframe is a sheet
def read_csv_file(path: str):
    """
    Reads a csv file with pandas and returns dataframe.
    """
    df = pd.read_csv(path, header=0)
    return df

# this function is called when the extraction module is executed.
def extract_data_from_file(path):
    """
    Arguments
    path: path to the file
    columns_types: dict of column names and their types
    Purpose
    This function extracts data from a file and returns a dataframe or a dict of dataframes.
    """
    filename, file_extension = os.path.splitext(path)
    if file_extension == ".xlsx" or file_extension == ".xls":
        data = read_xlsx_file(path)
        sheets = get_sheets(data)
    if file_extension == ".csv":
        data = read_csv_file(path)
        sheets = None
    
    return data, sheets
