import mysql.connector as dbapi_connector
# from mysql.connector import Error as dbapi_error
from  mysql.connector.pooling import MySQLConnectionPool as dbapi_pool

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dataset-db")
logger.setLevel(logging.INFO)


class DBManager:

    def __init__(self,
                 db_con:dbapi_connector = None,
                 db_pool:dbapi_pool = None,
                 db_host:str = None, db_user:str = None, db_password:str = None, db_name:str = None):

        if db_con:
            self.db_con:dbapi_connector = db_con
            self.db_type=1
        elif db_pool:
            self.db_pool:dbapi_pool = db_pool
            self.db_type = 2
        else:
            self.dbconfig = {
                "host": db_host,
                "user" : db_user,
                "password": db_password,
                "database": db_name
            }

            self.db_con = dbapi_connector.connect(**self.dbconfig)

        # self.dbapi_pooling = dbapi_pooling.MySQLConnectionPool(pool_name="o_pool", pool_size=3, **self.dbconfig)

        self.db_con.autocommit = True

    def get_con(self):
        pass

    def get_max_datetime_to_sec(self) -> str:
        return '9999-12-31 23:59:59.0'

    def close_db_con(self):
        pass

        if self.db_type == 1: # external con
            return
        elif self.db_type == 2: # external pool
            return
        else:  # let user explicitly close internally managed con
            if self.db_con and self.db_con.is_connected:
                self.db_con.close()
                self.db_con = None
            else:
                self.db_con = None

    def __get_db_con(self) -> dbapi_connector.connection:
        pass

        if self.db_type == 1: # an outside controlled con
            self.db_con.autocommit=True
            return self.db_con
        elif self.db_type == 2: # an outside con pool
            con = self.db_pool.get_connection()
            con.autocommit = True
            return con
        else: # db params and internally managed con
            if self.db_con is None:
                self.db_con = dbapi_connector.connect(**self.dbconfig)
                self.db_con.autocommit=True
                return self.db_con
            elif self.db_con.is_connected():
                self.db_con.autocommit = True
                return self.db_con
            else: # for some reason the DB con was disconnected, so create a new one
                self.db_con = dbapi_connector.connect(**self.dbconfig)
                self.db_con.autocommit = True
                return self.db_con

    def __close_db_con(self, conn:dbapi_connector):
        pass

        if self.db_type == 1:
            return
        elif self.db_type == 2:
            conn.close() # return to the pool
        else: # do not close internally managed con
            return