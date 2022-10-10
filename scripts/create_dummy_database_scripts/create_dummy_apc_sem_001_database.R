library(RMySQL)
library(RSQLite)

# Firstly, you need to have created a new MySQL db using command line
# Create a user and a password for the user

# Must enable writing in command line
# Login to mysql with 'mysql -u root -p'; then password Mjstedman6^^
# SET GLOBAL local_infile=1;

# Then run this

# Connect to the MySQL database
db_connection <- dbConnect(MySQL(),
                           user="db_user", password="Mjstedman6^^",
                           dbname="ABI", host="localhost")

# Load some dummy data
# must ensure date formatting yyyy-mm-dd in the .csv !!!!!
foo <- readr:: read_csv("/Users/nicholassunderland/Documents/2.Medical_work/5.Bristol/acsprojectns/acsprojectns/materials/dummy_databases/vw_APC_SEM_001_dummy_database.csv")

# Write out the table into the MySQL database
dbWriteTable(db_connection,
             value = foo,
             row.names = FALSE,
             name = "vw_APC_SEM_001",
             overwrite = TRUE)

# Check it is in there
dbListTables(db_connection)

# Disconnect
dbDisconnect(db_connection)




