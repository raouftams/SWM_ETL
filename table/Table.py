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
        #execute the query
        cursor.execute(f"INSERT INTO {self.table_name} (%s) VALUES (%s)" % (', '.join(data.keys()), ', '.join('%s' for _ in data.values())), tuple(data.values()))
        #commit changes
        db_connection.commit()
        #close cursor
        cursor.close()