from extract import *
import petl as etl
import numpy as np
from utilities import *


# pandas dataframe to petl
def df_to_petl(df):
    return etl.fromdataframe(df) 

#Concatenate two petl tables with a fixed set of fields 
def concat_table(table_a, table_b, header):
    """
    Arguments
    table_a: a petl table
    table_b: a petl table
    header: a list of column names
    Purpose
    This function concatenates two petl tables and returns a new petl table
    """
    if table_a != []:
        return etl.cat(table_a, table_b, header=header)
    return table_b

#Remove duplicates from a petl table
def remove_duplicates(table, header):
    """
    Arguments
    table: petl table
    Purpose
    This function removes duplicates from a petl table
    """
    return etl.distinct(table, header)

def change_columns_type(df, columns_types):
    """
    Arguments
    df: pandas dataframe
    columns_types: dict of types 
    """
    return df.astype(columns_types, errors = 'ignore')

def add_missing_columns(df, header):
    """
    Arguments
    df: pandas dataframe
    header: list of column names
    Purpose
    This function adds missing columns to a pandas dataframe
    """
    for column in header:
        if column not in df.columns:
            df[column] = np.nan
    return df

#This function transforms the data and returns a petl table
def transform_rotations_data(data, sheets):
    """
    Arguments
    data: dict of Pandas dataframes
    sheets: list of sheets names
    Purpose
    This function transforms the data and returns a petl table
    """
    #initialize a petl table
    table = []
    # transform the data
    for sheet_name in sheets:
        #these two sheets are not used
        if sheet_name != "TOTAL" and sheet_name != "CET":
            #get the current sheet
            df = data[sheet_name]
            if not df.empty:
                #remove whitespaces from column names 
                df.columns = df.columns.str.strip()
                #remove whitespaces from string values
                for column in df.columns:
                    try:
                        df[column] = df[column].str.strip()
                    except:
                        pass

                #lower case all the column names
                df.columns = map(str.lower, df.columns)
                #rename columns
                df.rename(columns=rotations_table_renaming, inplace=True)
                #add missing columns
                df = add_missing_columns(df, rotations_table_header)
                #drop useless columns
                df = df.drop(df.columns.difference(rotations_table_header), axis=1)
                #drop rows with nan value on vehicle column, they represent total values of previous rows
                df.dropna(subset = ["vehicle"], inplace=True)
                
                #change the type of the columns
                df = change_columns_type(df, rotations_table_types)
                #convert dataframe to petl table
                petl_table = df_to_petl(df)
                #concatenate the petl table with the previous table
                table = concat_table(table, petl_table, rotations_table_header)
                
    #remove duplicates
    table = remove_duplicates(table, rotations_table_header)
    return table

#write petl table to csv file
def write_petl_table_to_csv(table, path):
    """
    Arguments
    table: petl table
    path: path to the file
    Purpose
    This function writes a petl table to a csv file
    """
    if os.path.exists(path):
        etl.appendcsv(table, path)
    else:
        etl.tocsv(table, path)  

def main():
    # read the data
    dir_path = "D:\PFE M2\data\\2019\HAMICI"
    for filename in os.listdir(dir_path):
        path = os.path.join(dir_path, filename)
        print(path)
        data, sheets = extract_data_from_file(path)
        # transform the data
        table = transform_rotations_data(data, sheets)
        # write the data
        write_petl_table_to_csv(table, "rotations2019.csv")
        
if __name__ == "__main__":
    main()