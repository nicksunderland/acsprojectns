#' R6 Class representing a prescription event
#' @description This class represents a single prescription event
#'
PrescriptionEvent <- R6::R6Class(classname = "PrescriptionEvent",

                       # PRIVATE ---------------------------------------------------------------------
                       private = list(
                         id_ = NA,
                         datetime_ = NA,
                         medication_df_ = NULL

                         # .data$medication_source,
                         # .data$medication_name,
                         # .data$medication_dose,
                         # .data$medication_units,
                         # .data$prescription_quantity,
                         # .data$prescription_type,
                         # .data$prescription_cost
                       ),

                       # PUBLIC ---------------------------------------------------------------------
                       public = list(

                         #' @description Create a new prescription object.
                         #' @param id patient ID
                         #' @param datetime the datetime
                         #' @param medication_df the medication dataframe
                         #' @return A new `PrescriptionEvent` object.
                         initialize = function(id, datetime, medication_df){
                           private$id_ <- id
                           private$datetime_ <- datetime
                           private$medication_df_ <- medication_df
                         },

                         #' @description Overriding custom print function
                         #' @param ... This object's fields
                         #' @return Print to console
                         print = function(...){
                           cat("Prescription: \n")
                           # cat("  Num medications: ", length(private$medication_list[!is.na(private$medication_list)]), "\n")
                         },

                         #' @description Checks if a medication is in the list
                         #' @param medication medication string
                         #' @param query_flag how to query the list c("all", "any")
                         #' @return a boolean
                         is_in = function(medication, query_flag){
                           stopifnot(is.character(medication),
                                     query_flag %in% c("all", "any"))
                           if(query_flag == "all"){
                             return(all(purrr::map_lgl(medication, function(med_in){any(grepl(tolower(med_in), private$medication_df_$medication_name), fixed = TRUE)})))
                           }else if(query_flag == "any"){
                             return(any(purrr::map_lgl(medication, function(med_in){any(grepl(tolower(med_in), private$medication_df_$medication_name), fixed = TRUE)})))
                           }
                         }
                       ),

                       # ACTIVE ---------------------------------------------------------------------
                       active = list(
                         #' @field id
                         #' Returns the id
                         id = function(value){
                           if(missing(value)){
                             return(private$id_)
                           }else{
                             stop("`$id` is read only", call. = FALSE)
                           }
                         },
                         #' @field medication_df
                         #' Returns the medication_df
                         medication_df = function(value){
                           if(missing(value)){
                             return(private$medication_df_)
                           }else{
                             stop("`$medication_df` is read only", call. = FALSE)
                           }
                         },
                         #' @field datetime
                         #' Returns the datetime
                         datetime = function(value){
                           if(missing(value)){
                             return(private$datetime_)
                           }else{
                             stop("`$datetime` is read only", call. = FALSE)
                           }
                         }


                       )
)
