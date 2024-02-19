import common_lib
import os
import random
import time

class logic:
    def __init__(self):
        self.log = common_lib.Logger("LOGIC HANDLER")
        
        ## variables
        self.user_name = "Kinjal"

        ## creating temp folder
        try:
            os.makedirs("inserted_data")
            self.log.debug('temp FOLDER CREATED')
        except:
            self.log.debug('temp folder exists')


    def validate_inputs(self, args):
        """
        VALIDATE USER ARGUMENTS
        """
        self.log.info("START: VALIDATION OF USER INPUT")

        # fetching arguments into variables
        try:
            self.start_year = args.start_year
            self.end_year = args.end_year
            self.sleep_time = args.sleep_time
            self.log.debug("SUCCESS: FETCHING ARGUMENTS VALUES")

        except Exception as e:
            print("we are facing issue to fetch arguments")
            self.log.critical("ARGUMENTS ARE NOT VALID")
            self.log.error(e)

        # LOGGING RECIVED VALUES
        self.log.debug(f"PROVIDED VALUES ARE: START_YEAR = {self.start_year}, END_YEAR = {self.end_year}, SLEEP_TIME = {self.sleep_time}")

        # VALIDATING YEAR
        if self.end_year < self.start_year:
            self.log.warning(f"{self.end_year} is bigger then {self.start_year}")
            self.log.info("END: VALIDATION OF USER INPUT")
            raise Exception(f"{self.end_year} is bigger then {self.start_year}")

        self.log.info("END: VALIDATION OF USER INPUT")

    def setConfigMode(self, mode):
        """Setting config mode according provided env file"""

        ## Cheacking file avilablity
        file = 'config/config.json'

        try:
            data = common_lib.file_handling.read_json(file)
            self.log.info("CONFIG FILE LOADED")
        except FileNotFoundError as e:
            self.log.error(e)
            print("Configration file not found!!")
            raise Exception("Configration file not found!!")
        except Exception as e:
            self.log.error(e)
            print("We are facing issue to load config file.")
            raise Exception("unable to load config file")
        
        self.config = data[mode]


    def connect_db(self):
        """
        This method helps to connect to mysql and also create database if not exists.
        """
        self.db = common_lib.Database(
            host=self.config['mysql_config']['host'],
            user=self.config['mysql_config']['user'],
            password=self.config['mysql_config']['password'],
            port=self.config['mysql_config']['port'],
            log_enable=self.config['logger_config']['is_ddl_print']
        )


        self.log.info(f'CREATING DATABASE {self.config['mysql_config']['database']}')
        try:
            self.db.commit_query(f"CREATE DATABASE IF NOT EXISTS {self.config['mysql_config']['database']}")
            self.db.commit_query(f"USE {self.config['mysql_config']['database']}")
            print('INFO: DATABASE CONNECTED')
        except Exception:
            print("DATABASE CREATION FAIELD")

    def ingestion(self):
        ## creating required tables
        self.__create_required_tables()

        ## fetching json file 
        self.__get_cites_data()

        ## inserting data into database
        while True:
            print("INFO: INSERTING DATA INTO TABLES")
            self.insert_into_city_table()
            self.insert_into_project_table()
            self.insert_into_employee_table()
            print(f"sleeping for {self.sleep_time}")
            time.sleep(self.sleep_time)

    
    def __create_required_tables(self):
        """CREATING REQUIRED TABLES IF NOT EXISTS"""

        self.log.info("CREATING REQUIRED TABLES IF NOT EXISTS")
        try:
            # cities
            self.log.debug("CREATING cities TABLE")
            query = "CREATE TABLE IF NOT EXISTS cities (id INT AUTO_INCREMENT PRIMARY KEY,name VARCHAR(255),state_id INT,state_code VARCHAR(10),state_name VARCHAR(255),country_name VARCHAR(255),created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,created_by VARCHAR(255))"
            self.db.commit_query(query)
            self.log.info("CREATED: cities")

            # project
            self.log.debug("CREATING project TABLE")
            query = """CREATE TABLE IF NOT EXISTS project ( project_id INT AUTO_INCREMENT PRIMARY KEY, project_name VARCHAR(255), location VARCHAR(255), budget VARCHAR(50), StartDate DATE, EndDate DATE, project_duration INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, created_by VARCHAR(255))"""
            self.db.commit_query(query)
            self.log.info("CREATED: project")

            # employee
            self.log.debug("CREATING employee TABLE")
            query = """CREATE TABLE IF NOT EXISTS employee ( id INT AUTO_INCREMENT PRIMARY KEY, FirstName VARCHAR(255), LastName VARCHAR(255), FullName VARCHAR(510), Email VARCHAR(255), Salary DECIMAL(10, 2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, created_by VARCHAR(255))"""
            self.db.commit_query(query)
            self.log.info("CREATED: employee")

            print("INFO: TABLES CREATED")
        except:
            print("FACING ISSUE TO CREATE TABLES")
            exit()

    def __get_cites_data(self):
        """Fetch cites data from url"""

        ## fetching url to dictionary
        url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/cities.json"
        self.log.info(f"recived url is : {url}")

        try:
            self.log.debug("START: STORING URL JSON INTO DICT")
            print("INFO: FETCHING URL")
            self.cites_data = common_lib.file_handling.read_json_url(url)
            print("INFO: FETCHING URL COMPLETED")
            self.log.debug("END: STORING URL JSON INTO DICT")
        except Exception as e:
            print("URL FETCHING FAIELD ")
            self.log.critical("Facing issue to fetch url")
            self.log.error(e)
            exit() 

        self.max_len_cites_data = len(self.cites_data)

    def insert_into_city_table(self):
        """inserting random data into city table"""
        rd = self.cites_data[random.randint(0, self.max_len_cites_data)]
        data = {"id": "", "name" : rd['name'],"state_id":  rd['state_id'], "state_code": rd['state_code'], "state_name": rd['state_name'], "country_name" : rd['country_name']}

        self.log.debug("FETCHED RANDOM DATA FROM JSON")
        query = f"""INSERT INTO cities (name, state_id, state_code, state_name, country_name, created_by) VALUES ('{data['name']}', {data['state_id']}, '{data['state_code']}', '{data['state_name']}', '{data['country_name']}', '{self.user_name}')"""
        self.db.commit_query(query)

        ## storing inserted data into json file
        data['id'] = self.db.fetch_one("SELECT max(id) FROM cities")
        common_lib.file_handling.append_json([data], "inserted_data/city.json")

        ## STORING LAST ID INTO GLOABAL VAR
        self.last_city_data = data


    def insert_into_project_table(self):
        """Inserting data into project table"""

        ## inital variables
        names = ["home", "building", "SmallBuilding", "Big Building", "Infra 2", "Infra 1"]
        data = {"id": ""}
        data["project_name"] = random.choice(names)
        data["start_year"] = random.randint(self.start_year, self.end_year-2)
        data["end_year"] = random.randint(data["start_year"]+1, self.end_year)
        data["start_date"] = time.strftime("%d-%m-", time.localtime()) + str(data["start_year"])
        data["end_date"] = time.strftime("%d-%m-", time.localtime()) + str(data["end_year"])
        data["duration"] = data["end_year"] - data["start_year"]
        data["budget"] = random.randint(1250000,1550000)
        data["location"] = self.last_city_data["name"] + ", " + self.last_city_data["state_name"] + ", " + self.last_city_data["country_name"] + "-" + str(self.last_city_data["state_id"])

        query = f"""INSERT INTO project (project_name, location, budget, StartDate, EndDate, project_duration, created_by) VALUES ('{data["project_name"]}', '{data["location"]}', '{data["budget"]}', STR_TO_DATE('{data["start_date"]}', '%d-%m-%Y'), STR_TO_DATE('{data["end_date"]}', '%d-%m-%Y'), {data["duration"]}, '{self.user_name}')"""
        self.db.commit_query(query)

        data['id'] = self.db.fetch_one("SELECT max(project_id) FROM project")
        common_lib.file_handling.append_json([data], "inserted_data/project.json")

    def insert_into_employee_table(self):
        """Inserting random data into emplyee table"""

        ## initial parameters
        random_name = random.choice(common_lib.file_handling.read_csv("assets/customers.csv"))
        domains = ["gmail","yahoo","aol","hotmail"]

        data = {}
        data["id"] = ""
        data["FirstName"] = random_name[1]
        data["LastName"] = random_name[2]
        data["FullName"] = random_name[1] + " " + random_name[2]
        data["Email"] = random_name[1] + "_" + random_name[2] + ".@" + random.choice(domains) + ".com"
        data["Salary"] = random.randint(50000,5000000)
        data["created_by"] = self.user_name


        query = f"""INSERT INTO employee (FirstName, LastName, FullName, Email, Salary, created_by) VALUES ('{data["FirstName"]}', '{data["LastName"]}', '{data["FullName"]}', '{data["Email"]}', {data["Salary"]}, '{data["created_by"]}')"""
        self.db.commit_query(query)
        data['id'] = self.db.fetch_one("SELECT max(id) FROM employee")
        common_lib.file_handling.append_json([data], "inserted_data/employee.json")


        

    def close_db(self):
        """CLOSING THE MYSQL DATABASE CONNECTION"""
        self.db.close()

if __name__ == "__main__":
    """TESTING"""
    lo = logic()
    lo.setConfigMode()