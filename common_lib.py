import logging
import os
import json
import requests
import csv

class Logger: 
    def __init__(self, logger_name = "root", log_enable = True):
        self.log_enable = log_enable

        logging.basicConfig(
            filename="logs.log",
            filemode="w",
            level=logging.DEBUG,
            format="%(asctime)s - %(name)-15s - %(levelname)-8s  - %(message)s",
            datefmt="%d-%m-%Y %I:%M:%S %p"
        )

        self.logger = logging.getLogger(logger_name)

    def debug(self, msg):
        if self.log_enable:
            self.logger.debug(msg)

    def info(self, msg):
        if self.log_enable:
            self.logger.info(msg)

    def warning(self, msg):
        if self.log_enable:
            self.logger.warning(msg)

    def error(self, msg):
        if self.log_enable:
            self.logger.error(msg)

    def critical(self, msg):
        if self.log_enable:
            self.logger.critical(msg)


class file_handling:
    def __init__(self):
        self.file_handling_log = Logger('FILE HANDLING')
        pass

    def file_exist(self, file):
        self.file_handling_log.debug(f"START: FILE CHECKUP")
        self.file_handling_log.debug(f"RECIVED: {file}")

        if os.path.isfile(file):
            self.file_handling_log.info(f"FILE FOUND {file}")
            self.file_handling_log.debug(f"END: FILE CHECKUP")
            return True
        else:
            self.file_handling_log.warning(f"FILE NOT FOUND: {file}")
            self.file_handling_log.debug(f"END: FILE CHECKUP")
            return False
        
    def read_json(self, file):
        if self.file_exist(file):
            try:
                with open(file, 'r') as json_file:
                    data = json.load(json_file)
                return data
            except Exception as e:
                raise Exception(f"Failed to load data")
        else:
            raise FileNotFoundError(f"Given File {file} is not found")

    def read_json_url(self, url):
        response = requests.get(url)
        return response.json()
    
    def store_to_json(self, data, location):
        with open(location, 'w') as json_file:
            json.dump(data, json_file, indent=4)

    def append_json(self, data, file):
        """appending json"""
        try:
            with open(file, 'r') as json_file:
                ed = json.load(json_file)
            ed.extend(data)
            self.store_to_json(ed, file)
        except:
            self.store_to_json(data, file)

    def read_csv(self, file):
        with open(file, newline='') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)
            rows = list(reader)
            
        return rows






import mysql.connector
class Database:
    def __init__(self, host, user, password, port = 3306,log_enable = True):

        self.log_enable = log_enable # get log status

        self.db_log = Logger("DATABASE", log_enable=log_enable)

        self.db_log.info(f"CREATING CONNECTION FOR {user}@{host}.")
        self.db_log.debug("INIT: DATABASE CONNECTION.")

        try:
            # Creating Connection
            self.connection = mysql.connector.connect(
                host = host,
                user = user,
                password = password,
                port = port
            )
            self.db_log.info(f"CONNECTION ESTABLISHED FOR {user}@{host}")
            # Creating Cursor
            self.cursor = self.connection.cursor()
            self.db_log.info("CURSOR CREATED")

        except Exception as e:
            self.db_log.critical("CONNECTION FAILUER")
            self.db_log.error(e)
            raise(e)

    def close(self):
        """
        Closing cursor and connection
        """
        self.db_log.debug("START: CLOSE CONNECTION")
        self.cursor.close()
        self.db_log.info("CURSOR CLOSE SUCCESS")
        self.connection.close()
        self.db_log.info("CONNECTION CLOSE SUCCESS")
        self.db_log.debug("END: CLOSE CONNECTION")

    
    def is_table_exist(self, name):
        """Find weather table is exists or not """

        self.cursor.execute(f'SHOW TABLES LIKE "{name}"')
        result = self.cursor.fetchone()

        if result : 
            print("table is exists")
        else:
            print("table not exists")


    def commit_query(self, query):
        """Create table using query"""

        self.db_log.info(F"RECIVED QUERRY: {query} ")

        try:
            self.db_log.debug('EXECUTING RECIVED QUERRY')
            self.cursor.execute(query)
            self.db_log.info('EXECUTION SUCCESS')
            self.db_log.debug('INITITATE COMMIT')
            self.connection.commit()
            self.db_log.info(f"SUCCESS COMMIT: {query}")

        except Exception as e:
            self.db_log.critical('FAILED TO EXECUTE QUERRY')
            self.db_log.error(e)
            raise Exception(e)

    def drop_table(self, name):
        """Droping table"""
        self.db_log.info(f"START: DROPPING TABLE {name}")
        query = f'DROP TABLE {name}'
        self.commit_query(query)
        self.db_log.info(f"END: DROPPING TABLE {name}")



    def fetch_query(self, query):
        """Fetching the Query"""
        self.db_log.debug(f"RECIVED: FETCH QUERY {query}")
        try:
            self.cursor.execute(query)
            self.db_log.info('FETCHING SUCCESS')
            return self.cursor.fetchall()
        
        except Exception as e: 
            self.db_log.critical('FETCHING UNSUCCESS')
            self.db_log.error(e)

    def fetch_one(self, query):
        """Fetching the Query"""
        self.db_log.debug(f"RECIVED: FETCH QUERY {query}")
        try:
            self.cursor.execute(query)
            self.db_log.info('FETCHING SUCCESS')
            return self.cursor.fetchone()[0]
        
        except Exception as e: 
            self.db_log.critical('FETCHING UNSUCCESS')
            self.db_log.error(e)



# self instance
file_handling = file_handling()