from config.database import connect

#insert a rotation to the database (rotation table)
def insert_rotation(data, db_con):
    """
    arguments: 
        data: dict of data
        db_conn: psycopg2 db connection instance
    """
    #create a cursor
    cursor = db_con.cursor()
    #execute the query
    cursor.execute()
    #commit changes
    db_con.commit()
    #close cursor
    cursor.close()


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
    pass