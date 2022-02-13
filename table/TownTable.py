from table.table import Table

class TownTable(Table):

    def __init__(self) -> None:
        super().__init__("commune")
    

    #check if town name exists
    def exists_name(self, name, db_connection):
        """
        Args
            name: town name
            db_connection: pysoppg2 instance
        purpose
            check if town name exists in database
        """

        #create cursor
        cursor = db_connection.cursor()

        #execute query
        cursor.execute("SELECT * FROM commune WHERE name = '{}'".format(name.upper()))
        #get result
        result = cursor.fetchall()
        #close cursor
        cursor.close()
        #check if result is empty
        if result != []:
            return True
        
        return False

    #get code from name
    def get_code_from_name(self, name, db_connection):
        """
        Args:
            name: town name
            db_connection: psycopg2 db connection instance
        Get town's code using town's name
        """
        #create cursor
        cursor = db_connection.cursor()
        #execute query
        cursor.execute("SELECT code FROM commune WHERE name = '{}'".format(name.upper()))
        #get result
        result = cursor.fetchone()
        #close cursor
        cursor.close()
        #check if result is empty
        if result != []:
            return result[0]
        
        return None