# Prerequisites
# download and set up MySql for mac - 'brew install mysql'
# download odbc manager for mac - 'brew install --cask odbc-manager'
# download the MySql driver / Connector/ODBC 8.0.32 - https://dev.mysql.com/downloads/connector/odbc/
# download mysqlworkbench - brew install --cask mysqlworkbench
# create the schema to match the ICD database e.g. "ABI" etc.
# open ODBC manager and create a DSN named "XSW" to match the ICB; add your MySql 'user' and 'password'
# also create and set a variable called 'NO_SCHEMA' to 'false'
# https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-usagenotes-functionality-catalog-schema.html
# https://solutions.posit.co/connections/db/best-practices/schema/

# imports
library(DBI)
devtools::load_all()

# connect to mysql server
# conn <- DBI::dbConnect(odbc::odbc(), "XSW")
conf_path <- system.file("database_connections", "home", "mysql.yaml", package="acsprojectns")
conf = yaml::read_yaml(conf_path)
conf$drv <- RMariaDB::MariaDB()
conn <- do.call(DBI::dbConnect, conf)

# data tables; table_name: file_name
data_tables = list(
  list("catalog"="ABI",                "name"="vw_APC_SEM_001",          "data"="vw_APC_SEM_001_dummy_database.csv"),
  list("catalog"="ABI",                "name"="vw_APC_SEM_Spell_001",    "data"="vw_APC_SEM_Spell_001_dummy_database.csv"),
  list("catalog"="ABI",                "name"="vw_AE_SEM_001",           "data"="vw_AE_SEM_001_dummy_database.csv"),
  list("catalog"="ABI",                "name"="vw_NHAIS_Deaths_All",     "data"="vw_NHAIS_Deaths_All_dummy_database.csv"),
  list("catalog"="ABI",                "name"="Mortality",               "data"="CivilMortality_dummy_database.csv"),
  list("catalog"="MODELLING_SQL_AREA", "name"="primary_care_attributes", "data"="swd_attributes_history_dummy_database.csv"),
  list("catalog"="MODELLING_SQL_AREA", "name"="swd_attribute",           "data"="swd_attributes_dummy_database.csv"),
  list("catalog"="MODELLING_SQL_AREA", "name"="swd_activity",            "data"="swd_activity_dummy_database.csv"),
  list("catalog"="MODELLING_SQL_AREA", "name"="swd_LSOA_descriptions",   "data"="swd_lsoa_dummy_database.csv"),
  list("catalog"="MODELLING_SQL_AREA", "name"="swd_measurement",         "data"="swd_measurement_dummy_database.csv")
)

for(table in data_tables){
  fp <- system.file("materials", "dummy_databases", table$data, package="acsprojectns")
  d  <- readr:: read_csv(fp, show_col_types=F)
  n  <- paste(table$catalog, table$name, sep=".")
  DBI::dbWriteTable(conn, DBI::SQL(n), d, overwrite=T)
}


DBI::dbListTables(conn)


# con <- DBI::dbConnect(odbc::odbc(), "XSW", bigint = "character")
# databases <- con %>%
#   DBI::dbGetQuery("SELECT name FROM master.sys.databases")





DBI::dbDisconnect(conn)
