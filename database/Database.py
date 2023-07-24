import cx_Oracle
import time

class Database:
    def __init__(self, user_name, senha, local_host):
        self.user_name = user_name
        self.senha = senha
        self.local_host = local_host
        self.create_connection() #open con

    def create_connection(self):
        self.__start_time = time.time()
        self.connect_database = cx_Oracle.connect(
                            user = self.user_name,
                            password = self.senha,
                            dsn = self.local_host,
                            encoding = "UTF-8"
                            )
        self.cursor = self.connect_database.cursor()
        print("Connecting time:")
        print("--- %s seconds ---" % (time.time() - self.__start_time))

    def execute_query(self, query, data):
        self.__start_time_query = time.time()

        for i, row in data.iterrows():
            self.cursor.execute(query, tuple(row))
            self.connect_database.commit()

        print("Executing query:")
        print("--- %s seconds ---" % (time.time() - self.__start_time_query))
