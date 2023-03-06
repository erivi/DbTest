import csv
import sqlite3
import psycopg2

csv_file_name = "./data.csv"
database_table_name = "Posts"

# simple in memory database using sqlite
db_connection = sqlite3.connect(":memory:")
cur = db_connection.cursor()
cur.execute('SELECT sqlite_version()')
print("Sqlite version: {}".format(cur.fetchone()))
create_table = True

# connect to postgresql database
#db_connection = psycopg2.connect(
#    host="localhost", 
#    database="devtest", 
#    user="postgres", 
#   password="********")
#cur = db_connection.cursor()
#cur.execute('SELECT version()')
#print("PostgreSQL version: {}".format(cur.fetchone()))
#create_table = False
#try:
#    cur.execute("SELECT * from {} LIMIT 1".format(database_table_name))
#    cur.fetchone()
#except:
#    create_table = True
#finally:
#    db_connection.reset()
#    cur = db_connection.cursor()

if create_table:
    # First we create a database table and add columns from the csv file header (all columns are treated as text)
    with open(csv_file_name, "r") as fin:
        csv_iterator = csv.reader(fin, delimiter=';')
        csv_header = next(csv_iterator)
        print("Creating {0} table from csv header: {1}".format(database_table_name, csv_header))
        cur.execute("CREATE TABLE {0} ({1})".format(database_table_name, ','.join(["{} text".format(item) for item in csv_header])))

    # Now we import all rows from the csv file to the database
    with open(csv_file_name, 'r') as fin:
        csv_iterator = csv.reader(fin, delimiter=';')
        next(csv_iterator) # skip the header
        to_db = [','.join(["'{}'".format(item) for item in line]) for line in csv_iterator]
        print("Inserting {} rows into database".format(len(to_db)))
        for row in to_db:
            cur.execute("INSERT INTO {0} VALUES ({1});".format(database_table_name, row))

    db_connection.commit()

# Now we have a database that we can query using reqular SQL, example:
query = "SELECT * from Posts"
cur.execute(query)
print("Query: {}".format(query))
print("Results:")
for x in cur.fetchall():
    print(x)

query = "SELECT AccountName from Posts WHERE Platform='instagram'"
cur.execute(query)
print("Query: {}".format(query))
print("Results:")
for x in cur.fetchall():
    print(x)

# ...now import the cool packages you want and connect with the database and do something fun with the data

