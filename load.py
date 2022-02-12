from config.database import connect
from table.RotationTable import RotationTable
from table.TicketTable import TicketTable
from table.VehicleTable import VehicleTable
from table.TownTable import TownTable
from table.UnityTable import UnityTable

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
def load_rotations(rotation_data):
    """
    param: rotation_data: petl table
    purpose: load rotations data to the database
    """
    #split data
    pass

town = UnityTable()
data = {
    "code": "U0123",
    "name": "Dar El beida"
}

db_con = connect()
print(town.exists("U0123", db_con))
print("fin")