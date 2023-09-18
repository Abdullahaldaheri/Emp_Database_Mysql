
import mysql.connector
from mysql.connector import Error

# # 2. Connects to the MySQL database server:
def create_connection(host_name, user_name, user_password ,db_name,unix_socket):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            unix_socket=unix_socket
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


connection = create_connection("localhost", "root", "root","empDB1", "/Applications/MAMP/tmp/mysql/mysql.sock")

# 3. Create the database:
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
#
#
# create_database_query = "CREATE DATABASE empDB1"
# create_database(connection, create_database_query)
#

# 4. Create function to excute queries:
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
# # # 5. Create queries for creating tables:
create_Employees_table = """
CREATE TABLE IF NOT EXISTS Employees (
  id INT AUTO_INCREMENT,
  name TEXT NOT NULL,
  age INT,
  gender TEXT,
  nationality TEXT,
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""
#

create_Positions_table = """
CREATE TABLE IF NOT EXISTS Positions (
  id INT AUTO_INCREMENT,
  name TEXT NOT NULL,
  Salary TEXT,
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""

create_Department_table = """
CREATE TABLE IF NOT EXISTS Department (
  id INT AUTO_INCREMENT,
  name TEXT NOT NULL,
  description TEXT NOT NULL,
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""

create_Empl_Postion_table = """
 CREATE TABLE IF NOT EXISTS Empl_Postion (
   id INT AUTO_INCREMENT,
   emply_id INTEGER NOT NULL,
   postion_id INTEGER NOT NULL,
   FOREIGN KEY fk_emply_id (emply_id) REFERENCES Employees(id),
   FOREIGN KEY fk_postion_id (postion_id) REFERENCES Positions(id),
   PRIMARY KEY (id)
) ENGINE = InnoDB
"""

create_Empl_Depart_table = """
 CREATE TABLE IF NOT EXISTS Empl_Depart (
  id INT AUTO_INCREMENT,
   emply_id INTEGER NOT NULL,
   dep_id INTEGER NOT NULL,
   FOREIGN KEY fk_emply_id (emply_id) REFERENCES Employees(id),
   FOREIGN KEY fk_dep_id (dep_id) REFERENCES Department(id),
   PRIMARY KEY (id)
 ) ENGINE = InnoDB
 """

create_Emply_rate_table = """
#  CREATE TABLE IF NOT EXISTS Empl_rate (
#    id INT AUTO_INCREMENT,
#    rate INTEGER NOT NULL,
#    emply_id INTEGER NOT NULL,
#    FOREIGN KEY fk_emply_id (emply_id) REFERENCES Employees(id),
#    PRIMARY KEY (id)
#  ) ENGINE = InnoDB
#  """

execute_query(connection, create_Employees_table)
execute_query(connection, create_Positions_table)
execute_query(connection, create_Department_table)
execute_query(connection, create_Empl_Postion_table)
execute_query(connection, create_Empl_Depart_table)
execute_query(connection, create_Emply_rate_table)
# -----------------------------------------------------------------------------------------------------------------



# # 6.  INSERT queries:
create_Employees_query = "INSERT INTO Employees (name, age, gender, nationality) VALUES (%s, %s, %s, %s)"
Employees_val = [('Abdullah', 25, 'male', 'KSA'),
             ('Mohammed', 33, 'male', 'UAE'),
             ('Ahmed', 35, 'male', 'KSA'),
             ('Sara', 40, 'female', 'KSA'),
             ('Noura', 21, 'female', 'KSA')]

cursor = connection.cursor()
cursor.executemany(create_Employees_query, Employees_val)
connection.commit()



create_Positions_query = "INSERT INTO Positions (name, Salary) VALUES (%s, %s)"
Positions_val = [("Database ",20000),
             ("finance ",11000),
             ("Data analysis ",25000),
             ("payroll expert ",9000),
             ("admin ",9000)]

cursor = connection.cursor()
cursor.executemany(create_Positions_query, Positions_val)
connection.commit()

create_Department_query = "INSERT INTO Department (name, description) VALUES (%s,%s)"
Department_val = [("finance", "finance department plans and manages company money making sure a business can access cash in sustainable ways"),
             ("IT", "department ensures that the organization's systems networks  data and applications all connect and function properly"),
             ("HR", "the division of a business responsible for finding recruiting screening and training job applicants"),]

cursor = connection.cursor()
cursor.executemany(create_Department_query,Department_val)
connection.commit()

create_Empl_Postion_query = "INSERT INTO Empl_Postion (emply_id,postion_id) VALUES (%s, %s)"
Empl_Postion_val = [(1,1),
             (2,4),
             (3,3),
             (4,2),
             (5,5)]

cursor = connection.cursor()
cursor.executemany(create_Empl_Postion_query, Empl_Postion_val)
connection.commit()

create_Empl_Depart_query = "INSERT INTO Empl_Depart (emply_id,dep_id) VALUES (%s, %s)"
Empl_Depart_val = [(1,1),
             (2,2),
             (3,2),
             (4,1),
             (5,3)]

cursor = connection.cursor()
cursor.executemany(create_Empl_Depart_query,Empl_Depart_val)
connection.commit()

# # __________________________________________
#
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")






#
# __________________________________________
#
#
# # 7. Create  queries:

# Select with where

select_Department_description = "SELECT name,description  FROM Department WHERE id = 3"

Department_description = execute_read_query(connection, select_Department_description)

for Department_description in Department_description:
      print(Department_description)

#join with condition
select_Positions= """
SELECT
   positions.id,
  positions.name,
   employees.name
FROM
  Positions
   inner JOIN Empl_Postion on Empl_Postion.postion_id=Positions.id inner join
 employees ON employees.id = Empl_Postion.emply_id
 where positions.name = 'payroll expert' OR  positions.name ='Data analysis'
 """
Positions = execute_read_query(connection, select_Positions)

for Positions in Positions:
    print(Positions)





# # ___________________Update_______________________

update_Department_description = """
 UPDATE
   Department
 SET
   description = "finance department have lot of  money "
 WHERE
   id = 1
 """

execute_query(connection,update_Department_description)

#--------------------delete --------------------
delete_Empl_Depart = "DELETE from Empl_Depart WHERE id =1"
execute_query(connection,delete_Empl_Depart)
# #