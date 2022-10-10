#' R6 Class representing a medication
#' @description This class represents a single medication
#'
Medication <- R6::R6Class(classname = "Medication",

         # PUBLIC ---------------------------------------------------------------------
         public = list(

           #' @description Create a new medication object.
           #' @param id patient ID
           #' @param list_of_drugs list of Drug objects
           #' @return A new `PrescriptionEvent` object.
           initialize = function(pseudo_nhs_id,
                                 episode_start_datetime,
                                 medication_source,
                                 medication_name,
                                 medication_dose,
                                 medication_units,
                                 prescription_quantity,
                                 prescription_type,
                                 prescription_cost){

             private$id_               <- pseudo_nhs_id
             private$episode_datetime_ <- episode_start_datetime
             private$source_           <- medication_source
             tryCatch(expr  = {private$name_ <- trimws(tolower(as.character(medication_name)))},
                      error = function(e){
                        message(paste0("Problem setting Medication 'name' variable to: ", medication_name))
                        message(e)
                      })
             private$dose_             <- medication_dose
             private$units_            <- medication_units
             private$quantity_         <- prescription_quantity
             private$type_             <- prescription_type
             private$cost_             <- prescription_cost

           },

           #' @description Overriding custom print function
           #' @param ... This object's fields
           #' @return Print to console
           print = function(...){
             cat("Medication: \n")
             cat("  Drug:     ", private$name_, private$dose_, private$units_, "\n")
             cat("  Date:     ", as.character(private$episode_datetime_), "\n")
             cat("  Source:   ", private$source_, "\n")
             cat("  Type:     ", private$type_, "\n")
             cat("  Quantity: ", private$quantity_, "\n")
             cat("  Cost:     ", private$cost_, "\n", sep = "")
           }
         ),

         # ACTIVE ---------------------------------------------------------------------
         active = list(
           id = function(value){
             if(missing(value)){
               return(private$id_)
             }else{
               stop("`$id` is read only", call. = FALSE)
             }
           },
           date = function(value){
             if(missing(value)){
               return(as.POSIXct(private$episode_datetime_))
             }else{
               stop("`$date` is read only", call. = FALSE)
             }
           },
           date_dbl = function(value){
             if(missing(value)){
               return(as.double(private$episode_datetime_))
             }else{
               stop("`$date_dbl` is read only", call. = FALSE)
             }
           },
           source = function(value){
             if(missing(value)){
               return(as.character(private$source_))
             }else{
               stop("`$source` is read only", call. = FALSE)
             }
           },
           name = function(value){
             if(missing(value)){
               return(as.character(private$name_))
             }else{
               stop("`$name` is read only", call. = FALSE)
             }
           },
           dose = function(value){
             if(missing(value)){
               return(as.double(private$dose_))
             }else{
               stop("`$dose` is read only", call. = FALSE)
             }
           },
           units = function(value){
             if(missing(value)){
               return(as.character(private$units_))
             }else{
               stop("`$units` is read only", call. = FALSE)
             }
           },
           quantity = function(value){
             if(missing(value)){
               return(as.double(private$quantity_))
             }else{
               stop("`$quantity` is read only", call. = FALSE)
             }
           },
           type = function(value){
             if(missing(value)){
               return(as.character(private$type_))
             }else{
               stop("`$type` is read only", call. = FALSE)
             }
           },
           cost = function(value){
             if(missing(value)){
               return(as.double(private$cost_))
             }else{
               stop("`$cost` is read only", call. = FALSE)
             }
           }
         ),

         # PRIVATE ---------------------------------------------------------------------
         private = list(
           id_ = NA_real_,
           episode_datetime_ = NA_real_,
           source_ = NA_character_,
           name_ = NA_character_,
           dose_ = NA_real_,
           units_ = NA_character_,
           quantity_ = NA_real_,
           type_ = NA_character_,
           cost_ = NA_real_
         )
)
