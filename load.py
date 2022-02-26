from config.database import connect
from table.RotationTable import RotationTable
from table.TicketTable import TicketTable
from table.VehicleTable import VehicleTable
from table.TownTable import TownTable
from table.UnityTable import UnityTable
import petl as etl
import numpy as np
import math
from utilities import *
from transform import *


#insert rotations to database
def insert_rotations(data, db_connection):
    """
    Args
        data: petl dicts of rotations data
        db_connection: psycopg2 db connection instance
    Purpose:
        insert rotations data to database
    """

    rotation_header = ["date", "vehicle_id", "ticket", "town_code", "time", "cet", "date_hijri"]

    rotation_header_rename = {"vehicle_id": "id_vehicle", "ticket": "code_ticket", "town_code": "code_town", "time": "heure"}

    #select rotation table header and rename it
    table = etl.cut(data, rotation_header)
    table = etl.rename(table, rotation_header_rename)

    #get rows without nan values on time columns
    time_table = etl.select(table, lambda r: is_time(r["heure"]) != False)
    #remove time coulmns because there is only nan values
    nan_time_table = etl.select(table, lambda r: is_time(r["heure"]) == False)
    nan_time_table = etl.cutout(nan_time_table, "heure")
    

    #insert data
    etl.appenddb(time_table, db_connection, "rotation")
    #insert nan time values
    etl.appenddb(nan_time_table, db_connection, "rotation")

#insert ticket to database
def insert_ticket(data, db_connection):
    """
    Arguments:
        data: dict from petl table
    Purpose:
        insert ticket data to database
    """

    #create ticket table instance
    ticket = TicketTable()

    #check net wieghts
    net = data['net_cet']
    if math.isnan(net):
        net = data['net_extra']

    #transform dict 
    ticket_dict = {
        "code": data['ticket'],
        "brute": data['brute'],
        "net": net,
        "date": data['date'],
        "heure": data['time'],
    }
    #insert data
    ticket.insert(ticket_dict, db_connection)

#insert ticket data to database
def insert_ticket_table(table, db_connection):
    """
    Args:
        table: petl table of ticket data
        db_connection: psycopg2 db connection instance
    Purpose:
        insert ticket data to database
    """    

    ticket_table_header_rename = {"ticket": "code", "net_cet":"net","time": "heure"}

    #remove rows with date = none
    table = etl.rename(table, ticket_table_header_rename)
    table = etl.convert(table, "heure", str)

    #get rows without nan values on time columns
    time_table = etl.select(table, lambda r: is_time(r["heure"]) != False)
    #remove time coulmns because there is only nan values
    nan_time_table = etl.select(table, lambda r: is_time(r["heure"]) == False)
    nan_time_table = etl.cutout(nan_time_table, "heure")
    

    #insert data
    etl.appenddb(time_table, db_connection, "ticket")
    #insert nan time values
    etl.appenddb(nan_time_table, db_connection, "ticket")

    
#insert vehicles to the database (vehicle table)
def check_vehicle(table, db_connection):
    """
    Arguments:
        table: petl table of rotations data
    Purpose:
        check if vehicles exist in database
    """
    table = etl.convert(table, "vehicle_id", check_vehicle_id, pass_row=True)
    table = etl.select(table, "vehicle_id", lambda v: v != "nan_id" and v != "nan")
    return table

#insert tickets to the database (ticket table)
def check_ticket(table, db_connection):
    """
    Arguments: 
        table: petl table
    Purpose:
        check if ticket exists in database else add to db
    """

    #ticket table
    ticket_table = []

    #create ticket table instance
    ticket = TicketTable()

    #extract tickets data from table 
    tickets_array = etl.cut(table, "ticket", "brute", "net_cet", "date", "time", "cet")
    
    insert_ticket_table(tickets_array, db_connection)
    return table
    

#insert towns to the database (commune table)
def check_town(table, db_connection):
    """
    Arguments:
        table: petl table
        db_connection: database connection
    Purpose:
        check if town exists in database else remove it from petl table
    """

    #create town table instance
    town = TownTable()

    #extract towns data 
    towns_table = etl.cut(table, "town_code", "town")

    #extract towns data from table and transform into numpy array
    towns_array = etl.toarray(towns_table)

    towns_array = np.unique(towns_array, axis=0)
    #remove rows with code_commune = nan or does not exist in the database
    for row in towns_array:
        code_c = row[0]
        name_c = row[1]
        #check if town data is nan
        if code_c == "nan" and name_c == "nan":
            #remove row
            table = etl.selectisnot(table, {"town_code": code_c, "town": name_c})
        else:
            #check if the commune exists in database
            #check if code exists
            if code_c != "nan" and not town.exists(code_c, db_connection):
                #remove rows
                print("removed 1")
                table = etl.select(table, "town_code", lambda v: v != code_c)
            else:
                #check if name exists
                if name_c != "nan" and code_c == "nan" and not town.exists_name(name_c, db_connection):
                    #remove rows
                    print("removed 2")
                    table = etl.selectisnot(table, "town", name_c)
    return table

""" data enrichement """
#enrich vehicles data
def enrich_vehicle_data(table, db_connection):
    """
    Args: 
        table: petl table of rotation data
        db_connection: psycopg2 db connection instance
    purpose:
        insert vehicle code if nan based on vehicle mat
    """
    #create vehicle table instance
    vehicle = VehicleTable()

    table = etl.convert(
        table, "vehicle_id", 
        lambda v, row: vehicle.get_code_from_mat(row["vehicle_mat"], db_connection),
        where=lambda r: r["vehicle_id"] == "nan"
    )

    table = etl.selectisnot(table, "vehicle_id", "nan")
    
    return table

#enrich towns data
def enrich_town_data(table, db_connection):
    """
    Args: 
        table: petl table of rotation data
        db_connection: psycopg2 db connection instance
    purpose:
        insert vehicle code if nan based on vehicle mat
    """
    #create town table instance
    town = TownTable()

    table = etl.convert(
        table, "town_code", 
        lambda v, row: town.get_code_from_name(row["town_name"], db_connection), 
        where=lambda r: r["town_code"] == "nan"
    )

    table = etl.selectisnot(table, "town_code", "nan")
    
    return table


""" Load data """
#load rotations data to the database
def load_rotations(rotation_table):
    """
    param: rotation_table: petl table
    purpose: load rotations data to the database
    """

    #initialize db connection
    db_connection = connect()

    #change columns types
    rotation_table = etl.convert(rotation_table, rotations_table_types)

    #select data with distinct ticket_code, date and cet
    rotation_table = etl.distinct(rotation_table, ["ticket", "date", "cet"])

    print("Checking ticket data...")
    rotation_table = check_ticket(rotation_table, db_connection)

    #load rotations data to the database
    print("Loading rotations data to the database...")
    insert_rotations(rotation_table, db_connection)


#load vehicles data to database
def load_vehicles(data):
    """
    Arguments:
        data: petl table
    Purpose:
        load data to database
    """
    #change columns types
    data = etl.convert(data, vehicles_table_types)

    #initialize db connection
    db_connection = connect()

    #create vehicle table instance
    vehicle = VehicleTable()

    #create town table instance
    town = TownTable()

    #extract vehicles data from table and transform into numpy array
    towns_array = etl.values(data, "code_commune")
    #remove rows with code_commune = nan or does not exist in the database
    for code_c in towns_array:
        #check if the commune exists in database
        if code_c != "nan" and not town.exists(code_c, db_connection):
            #remove rows
            data = etl.selectisnot(data, "code_commune", code_c)

    #remove data header
    data = data[1:]
    for row in data:
        #init dict
        data_dict = {
            "code": row[0],
            "ancien_matricule": row[3],
            "nouveau_matricule": row[4],
            "num_chassie": row[5],
            "tare": row[10],
            "capacite": row[11],
            "marque": row[2],
            "genre": row[1],
            "volume": row[6],
            "puissance": row[7],
            "mise_en_marche": row[8],
            "code_commune": row[9]
        }
        if not vehicle.exists(row[0], db_connection):
            #insert data
            vehicle.insert(data_dict, db_connection)

