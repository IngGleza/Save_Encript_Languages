from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, Float, MetaData, BLOB

# Global Variables
SQLITE = 'sqlite'
# Table Names
Countries = 'countries'


class MyDatabase:

    DB_ENGINE = {
        SQLITE: 'sqlite:///{DB}'
    }

    # Main DB Connection Ref Obj
    db_engine = None

    def __init__(self, dbtype, username='user', password='pruebaTecnica', dbname='pruebatecnica'):
        dbtype = dbtype.lower()
        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=dbname)
            self.db_engine = create_engine(engine_url)
        else:
            print("DBType is not found in DB_ENGINE")

    def create_db_tables(self):
        metadata = MetaData()
        countries = Table(Countries, metadata,
                          Column('id', Integer, primary_key=True),
                          Column('minTime', Float),
                          Column('maxTime', Float),
                          Column('promTime', Float),
                          Column('totalTime', Float),
                          Column('dataOnJson', BLOB),
                          Column('dataDfToJson', BLOB),
                           sqlite_autoincrement=True
                          )
        try:
            metadata.create_all(self.db_engine)
        except Exception as e:
            print("Error occurred during Table creation!")
            print(e)

    def execute_query(self, tuple, query=''):
        if query == '':
            return
        with self.db_engine.connect() as connection:
            try:
                connection.execute(query, tuple)
            except Exception as e:
                print(e)
