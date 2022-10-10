Database <- R6::R6Class(classname = "Database",

# PRIVATE ---------------------------------------------------------------------
                        private = list(
                          driver = NA_character_,
                          server = NA_character_,
                          host = NA_character_,
                          trusted_connection = NA,
                          user = NA_character_,
                          password = NA_character_,
                          name = NA_character_,
                          schema = NA_character_,
                          table = NA_character_,
                          port = NA_integer_,
                          connection = NA
                        ),

# PUBLIC ---------------------------------------------------------------------
                        public = list(

                          # The function to initialise the object
                          initialize = function(file_path){

                            if(!file.exists(file_path)) stop("Database connection failed, input file_path does not exist")

                            # Try to read in the database connection text file
                            tryCatch(
                              expr = {
                                conn_vals                  <- read.csv(file_path, row.names = 1)
                                private$driver             <- conn_vals["driver", "value"]
                                private$server             <- conn_vals["server", "value"]
                                private$trusted_connection <- as.logical(conn_vals["trusted_connection", "value"])
                                private$port               <- as.numeric(conn_vals["port", "value"])
                                private$host               <- conn_vals["host", "value"]
                                private$name               <- conn_vals["database", "value"]
                                private$schema             <- conn_vals["schema", "value"]
                                private$table              <- conn_vals["table", "value"]
                                private$user               <- conn_vals["user", "value"]
                                private$password           <- conn_vals["password", "value"]

                                tryCatch(
                                  expr = {
                                    if(private$driver == "SQL Server"){
                                      private$connection <- odbc::dbConnect(odbc::odbc(),
                                                                            Driver = private$driver,
                                                                            Server = private$server,
                                                                            Trusted_Connection = private$trusted_connection,
                                                                            Database = private$name,
                                                                            #UID = private$user,
                                                                            #PWD = private$password,
                                                                            Port = private$port
                                                                            )
                                    }else if(private$driver == "MariaDB"){
                                      private$connection <- DBI::dbConnect(RMariaDB::MariaDB(),
                                                                           user = private$user,
                                                                           password = private$password,
                                                                           dbname = private$name,
                                                                           host = "localhost")
                                    }
                                    message(paste0("Connection successful ","[",private$name," : ",private$table,"]"))
                                    # self$print()
                                  },
                                  error = function(e){
                                    warning(paste0("Database connection object not created for [",private$name," : ",private$table,"]"))
                                })
                              },
                            error = function(e){
                              stop("Error creating database connection object")
                            }
                           )
                          },

                          # The finalize / deconstructor, called when the object is deleted
                          finalize = function() {
                            if(inherits(private$connection, "DBIConnection")){
                              if(DBI::dbIsValid(private$connection))
                                DBI::dbDisconnect(private$connection)
                            }
                          },

                          # A custom print function
                          print = function(...){
                            cat("Database: \n")
                            cat("  Schema:   ", private$schema, "\n", sep = "")
                            cat("  Name:     ", private$name, "\n", sep = "")
                            cat("  Table:    ", private$table, "\n", sep = "")
                            cat("  Driver:   ", private$driver, "\n", sep = "")
                            cat("  Server:   ", private$server, "\n", sep = "")
                            cat("  Host:     ", private$host, "\n", sep = "")
                            cat("  Trusted:  ", private$trusted_connection, "\n", sep = "")
                            cat("  Port:     ", private$port, "\n", sep = "")
                            cat("  --------------------- \n", sep = "")
                            cat("  User:     ", private$user, "\n", sep = "")
                            if(!is.na(private$password)) cat("  Password: ", "*exists*", "\n", sep = "") else cat("  Password: ", private$password, "\n", sep = "")
                            cat("  --------------------- \n", sep = "")
                            invisible(self)
                          }
                        ),

# ACTIVE ---------------------------------------------------------------------
                        # Active can be called like fields/variables, but they can be functions on the inside of the object (i.e. you dont need to use brackets when calling)
                        active = list(
                          # A function to check if there is a valid connection
                          is_connected = function(){
                            if(inherits(private$connection, "DBIConnection")){
                              if(DBI::dbIsValid(private$connection))
                                return(TRUE)
                            }else{
                              return(FALSE)
                            }
                          },

                          # A function returning a reference to the data
                          data = function(){
                            if(inherits(private$connection, "DBIConnection")){
                              if(DBI::dbIsValid(private$connection))
                                if(!is.na(private$schema)){
                                  return(dplyr::tbl(private$connection, dbplyr::in_schema(private$schema, private$table)))
                                }else{
                                  return(dplyr::tbl(private$connection, private$table))
                                }
                            }else{
                              stop("Attempting to pull data without a valid connection, have you called disconnect? or did the connection not load?")
                            }
                          },

                          # A function returning the datatable name
                          table_name = function(){
                            return(private$table)
                          }
                        )
)

#
# db_connection_list <-
#   list(
#     hosp_sus      = tryCatch(expr  = connect_to_database(database_name = "ABI", table_name = "vw_APC_SEM_001"),
#                              error = function(e){return(NA)}),
#     hosp_bloods   = tryCatch(expr  = connect_to_database(database_name = "TEST_RESULTS", table_name = "blood_tests"),
#                              error = function(e){return(NA)}),
#     gp_activity   = tryCatch(expr  = connect_to_database(database_name = "MODELLING_SQL_AREA", table_name = "swd_activity"),
#                              error = function(e){return(NA)}),
#     gp_attributes = tryCatch(expr  = connect_to_database(database_name = "MODELLING_SQL_AREA", table_name = "swd_attribute"),
#                              error = function(e){return(NA)}),
#     uk_mortality  = tryCatch(expr  = connect_to_database(database_name = "ABI", table_name = "vw_NHAIS_Deaths_All"),
#                              error = function(e){return(NA)})
#   )
