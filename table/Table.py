from psycopg2.extensions import AsIs

class Table:

    def __init__(self, table_name) -> None:
        self.table_name = table_name.lower()
    
    def insert(self, data, db_connection):
        """
        Arguments:
            data: dict 
            db_connection: psycopg2 db connection instance
        """
        #create a cursor
        cursor = db_connection.cursor()
        #create statement
        statement = "INSERT INTO {self.table_name} (%s) VALUES (%s)"
        #execute statement
        cursor.execute(statement, (AsIs(','.join(data.keys())), tuple(data.values())))
        #commit changes
        db_connection.commit()
        #close cursor
        cursor.close()
