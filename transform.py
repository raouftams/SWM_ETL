from extract import *
from utilities import *
from config.database import connect
import petl as etl
import numpy as np
import distance
import math
import random

#used global variables
db_connection = connect()
town_codes, town_names = get_registred_towns_data(db_connection)
vehicles_codes, vehicles_mats = get_registred_vehicles_data(db_connection)

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
def remove_rotation_data_duplicates(table, header):
    """
    Arguments
    table: petl table
    Purpose
    This function removes duplicates from a petl table
    """
    table = etl.distinct(table, header)

    return table

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


#this function transforms df of rotation data to standard structure and return petl table
def structure_rotation_df(df):
    petl_table = []
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
        #drop rows with nan values
        df.dropna(subset = ["town_code", "town"], how="all", inplace=True)
        df.dropna(subset = ["date", "date_hijri"], how="all", inplace=True)

        #change the type of the columns
        df = change_columns_type(df, rotations_table_types)
        #convert dataframe to petl table
        petl_table = df_to_petl(df)

    return petl_table


#This function transforms the data and returns a petl table
def structure_rotations_data(data, sheets=None):
    """
    Arguments
    data: dict of Pandas dataframes
    sheets: list of sheets names

    if sheets != None: data is a dict of pandas dataframes
    else: data is a pandas dataframe

    Purpose
    This function transforms the data and returns a petl table
    """
    if sheets != None:
        #initialize a petl table
        table = []
        # transform the data
        for sheet_name in sheets:
            #these two sheets are not used
            if sheet_name != "TOTAL" and sheet_name != "CET":
                #get the current sheet
                df = data[sheet_name]
                if not df.empty:
                    petl_table = structure_rotation_df(df)

                    #concatenate the petl table with the previous table
                    table = concat_table(table, petl_table, rotations_table_header)
    else:
        table = structure_rotation_df(data)

    #remove duplicates
    table = remove_rotation_data_duplicates(table, rotations_table_header)
    return table


#structure vehicles data
def structure_vehicles_data(data, sheets):
    """
    Arguments: 
        data: dict of pandas dataframe
        sheets: list of keys
    purpose:
        transform vehicles data to a standard structure 
    """
    #initialize petl table
    table = []
    #iterate through dataframes
    for key in sheets:
        if str.lower(key) == "parc":
            df = data[key]
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
                df.rename(columns=vehicles_table_renaming, inplace=True)
                #add missing columns
                df = add_missing_columns(df, vehicles_table_header)
                #drop useless columns
                df = df.drop(df.columns.difference(vehicles_table_header), axis=1)
                #drop rows with nan values on town_code, matirucule, code
                df.dropna(subset = ["code_commune", "nouveau_matricule", "code"], how="any", inplace=True)
                #convert dataframe to petl table
                petl_table = df_to_petl(df)
                #concatenate the petl table with the previous table
                table = concat_table(table, petl_table, vehicles_table_header)
    
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
        etl.appendcsv(table, path, encoding='utf-8')
    else:
        etl.tocsv(table, path, encoding='utf-8')


"""---------------------- Cleaning data ----------------------"""
#changing the towns names
def transform_towns_names(table, town_abbreviations=None, stop_words=None):
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
        if not str.upper(old_name).strip() in town_names:
            lower_old_name = str.lower(old_name).strip()
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
                    for i in range(len(town_names)):
                        #check if a substring of the old name is in the towns names
                        if compare_strings_with_substrings(lower_old_name, str.lower(town_names[i])) > 1:
                            #if there are more than one match, the town is not changed
                            towns_names_dict[old_name] = town_names[i]

                    #initializing the nearest string in towns_names to the string at position 0
                    nearest = 0
                    min_distance = distance.levenshtein(lower_old_name, str.lower(town_names[0]))
                    for i in range(len(town_names)):
                        #calculate the distance between the old name and the current town name
                        current_distance = distance.levenshtein(lower_old_name, str.lower(town_names[i]))
                        if current_distance < min_distance:
                            min_distance = current_distance
                            nearest = i

                    #check the which one is the nearest by number of matches in their substrings before assigning the town name
                    if old_name in towns_names_dict.keys():
                        if compare_strings_with_substrings(lower_old_name, str.lower(town_names[nearest])) >= compare_strings_with_substrings(old_name, towns_names_dict[old_name]):
                            towns_names_dict[old_name] = town_names[nearest]
                    else:
                        if compare_strings_with_substrings(lower_old_name, str.lower(town_names[nearest])) == 0 and distance.levenshtein(lower_old_name, str.lower(town_names[nearest])) > len(lower_old_name)/2:
                            towns_names_dict[old_name] = 'nan'
                        else:
                            towns_names_dict[old_name] = town_names[nearest]

            else: # lower_old_name = ""
                towns_names_dict[old_name] = 'nan'

        else:#town name not in db
            towns_names_dict[old_name] = str.upper(old_name)

    #changing the town names in table
    for old_name in towns_names_dict.keys():
        if old_name != 'nan':
            table = etl.convert(table, "town", {old_name: towns_names_dict[old_name]})
    
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

#petl function for handling missing values of town code
def get_missing_town_value(val, row):
    """
    args
        val: town code in current row
        row: current row
    """
    if val not in town_codes:
        if row["town"] == "nan" or row["town"].upper() not in town_names:
            return "nan"
        else:
            index = town_names.index(row['town'].upper())        
            return town_codes[index]
    return val

#handle missing values on town code and town name
def transform_missing_town_values(table):
    """
    args table: petl table of rotations data
    puprose: remove inexistent town data in database and handle nan values
    """
    table = etl.convert(table, 'town_code', get_missing_town_value, pass_row=True)
    table = etl.selectisnot(table, 'town_code', 'nan')
    return table

#Tranfrom towns data (handle names, missing values, etc.)
def transform_towns_data(table):
    """
    args: table: petl table of rotations data
    """

    #transform towns names
    table = transform_towns_names(table, towns_abbreviations, town_stop_words)

    #handle missing values and inexisting values in database
    table = transform_missing_town_values(table)

    return table


#transform vehicle's matricule to correct format
def format_matricule(mat):
    #check if the matricule is not already formated
    if "-" not in mat:
        #check if length is a least 5 caracters
        if len(mat) >= 5:
            mat_end = "-" + mat[len(mat)-2:]
            if mat[len(mat)-4:len(mat)-2] == "00":
                middle_mat = "-" + mat[len(mat)-4:len(mat)-2]
                mat_start = mat[:len(mat)-4]
            else:
                middle_mat = "-" + mat[len(mat)-5:len(mat)-2]
                mat_start = mat[:len(mat)-5]

            return mat_start + middle_mat + mat_end

    return mat


#petl lambda function to check the existence of a vehicle id
def check_vehicle_id(value, row):
    """
    value: current vehicle id value
    row: current table row
    purpose: check if value exists in db and replace it with "nan_id" if not
    """

    #check if vehicle id doesn't exist in db
    if value not in vehicles_codes:
        #check if matricule exists
        if row["vehicle_mat"] == "nan" or row["vehicle_mat"] not in vehicles_mats:
            return "unknown" #this is a primary key in vehicle table (db) for unknown vehicles
        else: #matricule exists in db
            #get the correspondant id of the matricule
            #vehicles_mats array contains old and new matricules of the vehicle
            index = vehicles_mats.index(row["vehicle_mat"])
            return vehicles_codes[int(index/2)]
    return value

#Transfrom vehicles data for rotations table
def transform_rotation_vehicle_data(table):
    """
    Args: table of rotations data
    return: table of rotations data
    purpose: remove rotations with inexisting vehicle code in db
    """
    #check if vehicle's matricule is in correct format and format it if not
    table = etl.convert(table, "vehicle_mat", format_matricule)
    #first step is to convert inexistent vehicle id to "nan_id"
    table = etl.convert(table, "vehicle_id", check_vehicle_id, pass_row=True)
    #remove all rows with "nan_id" value on vehicle_id column
    table = etl.selectisnot(table, "vehicle_id", "nan_id")
    table = etl.selectnotnone(table, "vehicle_id")

    return table        


#Transform ticket code nan values
def transform_ticket(table):
    table = etl.convert(table, 'ticket', lambda x: random.randint(1000000, 9999999), where=lambda row: row["ticket"] == 'nan')

    return table

#petl function to get gregorian date from hijri date
def get_gregorian_date(value, row):
    """
    params: value: the value of the gregorian date column in current row
            row: the row of the table
    purpose:
        This function returns the gregorian date from the hijri date
    """
    #check if the value is in any way a correct format of date
    if is_date(value):
        return format_date(value)
    else: #the value is not a correct date
        #get the hijri date from the current row
        current_hijri_date = row["date_hijri"]
        #check if the hijri date is in any way a correct format of date
        if is_date(current_hijri_date):
            #get the gregorian date from the hijri date 
            return hijri_to_gregorian_date(current_hijri_date)
    #if the value is not a correct date and the hijri date is not a correct date
    return "nan"

#petl function to get hijri date from gregorian date
def get_hijri_date(value, row):
    """
    params: value: the value of the hijri date column in current row
            row: the row of the table
    purpose:
        This function returns the hijri date from the gregorian date
    """
    #check if the value is in any way a correct format of date
    if is_date(value):
        return format_date(value)
    else: #the value is not a correct date
        #get the gregorian date from the current row
        current_gregorian_date = row["date"]
        #check if the gregorian date is in any way a correct format of date
        if is_date(current_gregorian_date):
            #get the hijri date from the gregorian date 
            return gregorian_to_hijri_date(current_gregorian_date)
    #if the value is not a correct date and the hijri date is not a correct date
    return "nan"

#hijri to gregorian date and vice versa
def transform_dates(table):
    """
    parms:
        table: a petl table
    purpose:
        This function transforms the dates (hijri to gregorian and gregorian to hijri)
    """

    #transform the hijri to gregorian dates
    table = etl.convert(table, "date", get_gregorian_date, pass_row=True)
    #transform the gregorian to hijri dates
    table = etl.convert(table, "date_hijri", get_hijri_date, pass_row=True)
    #remove the rows with nan values on date column
    table = etl.select(table, lambda row: is_date(row["date"]) != False)
    #remove the rows with nan values on date_hijri column
    table = etl.select(table, lambda row: is_date(row["date_hijri"]) != False)
    
    #transform all dates to same format (dd-mm-yyyy)
    table = etl.convert(table, "date", lambda v: str(v).replace("/", "-").split(" ")[0])
    table = etl.convert(table, "date_hijri", lambda v: str(v).replace("/", "-").split(" ")[0])
    return table


#petl function to get time
def get_time(value, row):
    """
    params: value: the value of the time column in current row
            row: the row of the table
    purpose:
        This function returns the time 
    """
    #check if the value is in any way a correct format of time
    if is_time(value):
        return value
    return "nan"

#transfrom time data
def transform_time(table):
    """
    args: table: petl table of rotations data
    purpose:
        This function transforms the time data
    """
    #transform the time data
    table = etl.convert(table, "time", get_time, pass_row=True)	
    #remove the rows with nan values on time column
    #table = etl.selectisnot(table, "time", "nan")
    #table = etl.selectisnot(table, "time", None)
    return table

#petl function for replacing net_cet value with net_extra if net_cet is nan
def net_extra_to_net_cet(val, row):
    """
    params: val: the value of the net_cet column in current row
            row: the row of the table
    purpose:
        This function replaces the net_cet value with net_extra if net_cet is nan
    """
    if not math.isnan(row["net_extra"]):
        return row["net_extra"]
    if math.isnan(val) or val == 'nan':
        if math.isnan(row["net_extra"]):
            return 0
        return row["net_extra"]
    
    return val

#petl function for calculating the gap value
def get_tare_weight(val, row):
    """
    params: val: the value of the net_cet column in current row
            row: the row of the table
    purpose:
        This function calculates the tare values if it's nan
    """
    #check if the current value is nan
    if math.isnan(val):
        #check if brute value is nan
        #the net_cet value can't be nan because the nan values are already deleted
        if not math.isnan(row['brute']):
            return row['brute'] - row['net_cet']
        return 0
    return val

#petl function for calculating the brute weight value
def get_brute_weight(val, row):
    """
    params: val: the value of the net_cet column in current row
            row: the row of the table
    purpose:
        This function calculates the brute values if it's nan
    """
    #check if the current value is nan
    if math.isnan(val):
        #check if tare value is nan
        #the net_cet value can't be nan because the nan values are already deleted
        if not math.isnan(row['tare']):
            return row['net_cet'] + row['tare']
        return 0
    if val == None:
        return 0
    return val

#clean net weight
def transform_weights(table):
    #replace net_cet nan values with net_extra values
    table = etl.convert(table,"net_cet", net_extra_to_net_cet,pass_row=True)

    #remove rows with nan value on net_cet
    table = etl.selectisnot(table, "net_cet", 0)
    table = etl.selectisnot(table, "net_cet", 'nan')

    #fill missing brute values
    table = etl.convert(table, "brute",get_brute_weight,pass_row = True)
    table = etl.convert(table, "brute", lambda x: x if x != None else 0)
    #calculate the tare 
    table = etl.convert(table, "tare",get_tare_weight,pass_row = True)
    
    return table

#general function for transforming rotation data
def transform_rotation_data(data, sheets=None):
    """
    params: 
        data: a dict of pandas dataframes
        sheets: list of keys
    """
    #transform to a standard structure
    table = structure_rotations_data(data, sheets)
    etl.convert(table, rotations_table_types)

    #Transforming towns names in the table
    #towns_list, towns_abbreviations and town_stop_words ar in the utilities file
    table = transform_towns_data(table)
    
    #transform vehicles codes
    table = transform_rotation_vehicle_data(table)

    #transform tickets codes
    table = transform_ticket(table)

    #transfroming dates
    table = transform_dates(table)

    #transfroming time
    table = transform_time(table)

    #transforming weights
    table = transform_weights(table)


    return table



def main():
    '''
    dir_paths = {
        "D:\PFE M2\data\\2016\CORSO": "2016",
        "D:\PFE M2\data\\2016\HAMICI": "2016",
        "D:\PFE M2\data\\2017\CORSO": "2017",
        "D:\PFE M2\data\\2017\HAMICI": "2017",
        "D:\PFE M2\data\\2018\CORSO": "2018",
        "D:\PFE M2\data\\2018\HAMICI": "2018",
        "D:\PFE M2\data\\2019\CORSO": "2019",
        "D:\PFE M2\data\\2019\HAMICI": "2019",
    }
    
    sys.setrecursionlimit(10000)
    path = "D:\PFE M2\data\\new_data\Parc extranet.xlsx"
    df, sheets = extract_data_from_file(path)
    table = structure_vehicles_data(df, sheets)
    print(table)
    '''


if __name__ == "__main__":
    main()