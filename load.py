from config.database import connect
from table.RotationTable import RotationTable
from table.TicketTable import TicketTable
from table.VehicleTable import VehicleTable
from table.TownTable import TownTable
from table.UnityTable import UnityTable
import petl as etl
import numpy as np
from utilities import rotations_table_types, vehicles_table_types


#insert a rotation to the database (rotation table)
def insert_rotation(data, db_con):
   pass

#insert a vehicle to the database (vehicle table)
def insert_vehicle(row):
    pass

#insert a ticket to the database (ticket table)
def insert_ticket(row):
    pass

#insert a town to the database (commune table)
def insert_town(row):
    pass

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

    #extract unities data from table and transform into numpy array
    unities_array = etl.toarray(etl.distinct(rotation_table, ["unit", "unit_code"]))

    #create unity table instance
    unity = UnityTable()

    for row in unities_array:
        if row[0] != "nan" and row[1] != "nan" and not unity.exists(row[1]): 
            #init dict
            data_dict = {
                "unit": row[0], 
                "unit_code": row[1]
                }
            #insert data
            unity.insert(data_dict, db_connection)
    

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

