DataFilter <- R6::R6Class(classname = "DataFilter",

        # PRIVATE ---------------------------------------------------------------------
        private = list(
          date_range_interval_  = lubridate::interval(NA, NA),
          medications_          = character()
        ),

        # PUBLIC ---------------------------------------------------------------------
        public = list(

          # The function to initialise the object
          initialize = function(date_range_interval = lubridate::interval(NA, NA),
                                medications         = character()){
            # Input checks
            stopifnot(lubridate::is.interval(date_range_interval),
                      is.character(medications))
            # Set the object fields
            private$date_range_interval <- date_range_interval
            private$medications         <- medications
          },

          # A custom print function
          print = function(...){
            cat("DataFilter object:")
            cat("   Date range:  ", as.character(lubridate::int_start(date_range_interval)), "-to-", as.character(lubridate::int_end(date_range_interval)))
            cat("   Medications: ", medications)
            cat("   ... ")
          }
        ),

        # ACTIVE ---------------------------------------------------------------------
        # Active can be called like fields/variables, but they can be functions on the inside of the object (i.e. you dont need to use brackets when calling)
        active = list(

          # A function returning the datatable name
          date_range_interval = function(value){
            if(missing(value)){
              if(is.na(private$date_range_interval)) warning("DataFilter field 'date_range_interval' is NA")
              return(private$date_range_interval)
            }else{
              stopifnot(lubridate::is.interval(date_range_interval))
              private$date_range_interval <- value
              invisible(self)
            }
          }
        )
)
