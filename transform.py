from extract import *
from utilities import *
import petl as etl
import numpy as np
import distance


"""---------------------- Transformation to standard structure ----------------------"""
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

#change the type of the columns in a pandas dataframe
def change_columns_type(df, columns_types):
    """
    Arguments
    df: pandas dataframe
    columns_types: dict of types 
    """
    return df.astype(columns_types, errors = 'ignore')

#add missing columns to a pandas dataframe
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
def structure_rotations_data(data, sheets):
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
                df.dropna(subset = ["vehicle", "ticket"], how="all", inplace=True)
                
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

def get_structured_data(dir_path, out_path, header):
    data = []
    for file in os.listdir(dir_path):
        path = os.path.join(dir_path, file)
        df, sheets = extract_data_from_file(path)
        table = structure_rotations_data(df, sheets)
        data = concat_table(data, table, header)
    
    write_petl_table_to_csv(data, out_path)

def merge_rotation_data(dir_path, out_path):
    data = []
    for file in os.listdir(dir_path):
        path = os.path.join(dir_path, file)
        table = etl.fromcsv(path)
        data = concat_table(data, table, rotations_table_header)
    
    write_petl_table_to_csv(data, out_path)


"""---------------------- Cleaning data ----------------------"""
#changing the towns names
def change_towns_names(table, towns_names, town_abbreviations=None, stop_words=None):
    """
    Arguments
    table: petl table
    towns_names: list of appropriate towns names
    town_abbreviations: dict of some abbreviations of towns names (abbreviation: town name), abbreviations are in lower case
    stop_words: list of some common stop words
    Purpose
    This function changes the towns names
    """
    #dict of towns names (old name: new name)
    towns_names_dict = {}
    #extract only data with distinct values on town column
    data = etl.distinct(table, "town")
    #get town distinct values
    old_names = etl.values(data, "town")

    #iterate over the towns old names
    for old_name in old_names:
        lower_old_name = str.lower(old_name)
        #removing stop words from the old name
        if stop_words != None:
            for stop_word in stop_words:
                if str.lower(stop_word) in lower_old_name:
                    lower_old_name = lower_old_name.replace(str.lower(stop_word), "").strip()

        if lower_old_name != "" and lower_old_name != "nan":
            #check if old name is an abbreviation
            if town_abbreviations != None and lower_old_name in town_abbreviations.keys():
                towns_names_dict[old_name] = town_abbreviations[lower_old_name]
            
            else:
                for i in range(len(towns_names)):
                    #check if a substring of the old name is in the towns names
                    if compare_strings_with_substrings(lower_old_name, towns_names[i]) > 1:
                        #if there are more than one match, the town is not changed
                        towns_names_dict[old_name] = towns_names[i]
                        
                #initializing the nearest string in towns_names to the string at position -1
                nearest = 0
                min_distance = distance.levenshtein(lower_old_name, str.lower(towns_names[0]))
                for i in range(len(towns_names)):
                    #calculate the distance between the old name and the current town name
                    current_distance = distance.levenshtein(lower_old_name, str.lower(towns_names[i]))
                    if current_distance < min_distance:
                        min_distance = current_distance
                        nearest = i

                #check the which one is the nearest by number of matches in their substrings before assigning the town name
                if old_name in towns_names_dict.keys():
                    if compare_strings_with_substrings(old_name, towns_names[nearest]) > compare_strings_with_substrings(old_name, towns_names_dict[old_name]):
                        towns_names_dict[old_name] = towns_names[nearest]
                else:
                    if compare_strings_with_substrings(old_name, towns_names[nearest]) == 0:
                        towns_names_dict[old_name] = None
                    else:
                        towns_names_dict[old_name] = towns_names[nearest]
                
        else: # lower_old_name = ""
            towns_names_dict[old_name] = None

    print(towns_names_dict)
    #changing the town names in table
    for old_name in towns_names_dict:
        table = etl.convert(table, "town", old_name, towns_names_dict[old_name])
    
    return table


#comparing strings with number of matches
def compare_strings_with_substrings(string1, string2):
    """
    Arguments
    string1: string
    string2: string
    Purpose
    This function compares two strings and returns the number of matches in their substrings
    """
    first_substrings = str.lower(string1).split(" ")
    second_substrings = str.lower(string2).split(" ")
    #initialize the number of matches
    matches = 0
    for fst_sub in first_substrings:
        for sec_sub in second_substrings:
            if fst_sub == sec_sub:
                matches += 1
    return matches


def main():
    '''dir_paths = {
        "D:\PFE M2\data\\2016\CORSO": "2016",
        "D:\PFE M2\data\\2016\HAMICI": "2016",
        "D:\PFE M2\data\\2017\CORSO": "2017",
        "D:\PFE M2\data\\2017\HAMICI": "2017",
        "D:\PFE M2\data\\2018\CORSO": "2018",
        "D:\PFE M2\data\\2018\HAMICI": "2018",
        "D:\PFE M2\data\\2019\CORSO": "2019",
        "D:\PFE M2\data\\2019\HAMICI": "2019",
    }

    for path in dir_paths.keys():
        out_path = os.path.join("out/", dir_paths[path] + ".csv")
        get_structured_data(path, out_path, rotations_table_header)
    
    merge_rotation_data("out/", "out/rotations.csv")
    '''

    table = etl.fromcsv("out/2016.csv")
    table = change_towns_names(table, towns_list, town_abbreviations, town_stop_words)
    


if __name__ == "__main__":
    main()