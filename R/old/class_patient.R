#' R6 Class representing a patient
#' @description This class represents a single patient
#'
Patient <- R6::R6Class(classname = "Patient",

# PUBLIC ---------------------------------------------------------------------
                       public = list(

                         #' @description Create a new patient object.
                         #' @param id The ID that identifies this patient, must be castable as.double()
                         #' @param id_type The type of ID, must be %in% c("pseudo_nhs_id", "nhs_number_id", "mrn_id")  )
                         #' @return A new `Patient` object.
                         initialize = function(id, id_type){
                           switch(id_type,
                                  "pseudo_nhs_id" = {private$pseudo_nhs_id_ <- as.double(id)},
                                  "nhs_number_id" = {private$nhs_number_id_ <- as.double(id)},
                                  "mrn_id"        = {private$mrn_id_        <- as.double(id)})
                         },

                         #' @description Overriding custom print function
                         #' @param ... This object's fields
                         #' @return Print to console
                         print = function(...){
                           cat("Patient: \n")
                           cat("  pseudo_nhs_id: ", private$pseudo_nhs_id_, "\n", sep = "")
                           cat("  nhs_number_id: ", private$nhs_number_id_, "\n", sep = "")
                           cat("  mrn_id:        ", private$mrn_id_, "\n", sep = "")
                           cat("  date_of_birth: ", as.character(private$date_of_birth_), "\n", sep = "")
                           cat("  date_of_death: ", as.character(private$date_of_death_), "\n", sep = "")
                           cat("  num spells:    ", length(private$spell_list_[!is.na(private$spell_list_)]), "\n", sep = "")
                         },

                         #' @description Set the patient's date of birth
                         #' @param date_of_birth The date of birth as a POSIXct object
                         #' @return nothing
                         set_date_of_birth = function(date_of_birth){
                           try(private$date_of_birth_ <- as.POSIXct(date_of_birth))
                         },

                         #' @description Set the patient's death date
                         #' @param date_of_death The death date as a POSIXct object
                         #' @return nothing
                         set_date_of_death = function(date_of_death){
                           try(private$date_of_death_ <- as.POSIXct(date_of_death))
                         },

                         #' @description Set the patient's list of Spell objects
                         #' @param spell_list A list of Spell objects
                         #' @return nothing
                         set_spell_list = function(spell_list){
                           # Check spell list in the correct format (i.e. a list of Spell objects)
                           if(checkmate::test_class(spell_list, "Spell")){                                                        # Must be a single object of class Spell
                             private$spell_list_ <- list(spell_list)                                                              # Return a list of 1x Spell object
                           }else if(is.list(spell_list) & all(purrr::map_lgl(spell_list, ~ checkmate::test_class(.x, "Spell")))){ # Must be a list object, full of Spell objects
                             private$spell_list_ <- spell_list                                                                    # This is the correct type, return the list of Spell objects
                           }else{
                             private$spell_list_ <- "error setting spell list"
                           }
                         },

                         #' @description Set the patient's list of PrescriptionEvent objects
                         #' @param prescription_list A list of PrescriptionEvent objects
                         #' @return nothing
                         set_prescription_list = function(prescription_list){
                           # Check prescription list in the correct format (i.e. a list of PrescriptionEvent objects)
                           if(checkmate::test_class(prescription_list, "PrescriptionEvent")){                                                        # Must be a single object of class PrescriptionEvent
                             private$prescription_list_ <- list(prescription_list)                                                              # Return a list of 1x PrescriptionEvent object
                           }else if(is.list(prescription_list) & all(purrr::map_lgl(prescription_list, ~ checkmate::test_class(.x, "PrescriptionEvent")))){ # Must be a list object, full of PrescriptionEvent objects
                             private$prescription_list_ <- prescription_list                                                                    # This is the correct type, return the list of PrescriptionEvent objects
                           }else{
                             private$prescription_list <- "error setting prescription list"
                           }
                         },

                         #' @description Search the patient's list of Spell objects for diagnosis codes
                         #' @param codes A vector ICD-10 codes to search for e.g. c("I210", "I219")
                         #' @param search_strat_diag The diagnosis search strategy, one out of c("primary", "secondary", "all")
                         #' @param search_strat_date The date search strategy; return the most recent date with "max", or the most historical date with "min"
                         #' @param time_window The time window to search within - must be a lubridate::interval() object
                         #' @param return_val A switch on what is being returned. If nothing found then NA is returned.
                         #' "code" returns the ICD-10 code
                         #' "episode_datetime" returns the start date of the episode in which the ICD-10 code was found
                         #' "spell_interval" returns the spell interval in which the ICD-10 code was found
                         #' "exists" returns a boolean as to whether a code was found within the time window
                         #' @return Depends on the return_val parameter
                         query_codes = function(codes, search_strat_diag, search_strat_date, time_window, return_val){
                           # Input checks
                           stopifnot(
                             is.character(codes),
                             search_strat_diag %in% c("primary", "secondary", "all"),
                             search_strat_date %in% c("min", "max"),
                             lubridate::is.interval(time_window),
                             return_val %in% c("code", "episode_datetime", "spell_interval", "exists")
                           )

                           # Cycle through the Spells list and create a database containing all of the Episode data for each spell
                           l = purrr::map(private$spell_list_, ~ .x$df_diagnosis_code_datetimes %>%
                                                                     dplyr::filter(.data$code %in% codes &
                                                                                   lubridate::`%within%`(.data$episode_datetime, time_window) &
                                                                                   dplyr::case_when(search_strat_diag == "primary"   ~ .data$type == "primary",
                                                                                                    search_strat_diag == "secondary" ~ .data$type == "secondary",
                                                                                                    search_strat_diag == "all"       ~ TRUE,
                                                                                                    TRUE ~ TRUE))) %>%
                             dplyr::bind_rows() %>%
                             dplyr::arrange(match(.data$code, codes)) %>% # arrange the codes by the input code vector (need to put the most important first)
                             {if(search_strat_date == "min")                    dplyr::slice_min(., .data$episode_datetime, with_ties = FALSE) else
                               if(search_strat_date == "max")                   dplyr::slice_max(., .data$episode_datetime, with_ties = FALSE)} %>%
                             {if(return_val == "code" | return_val == "exists") dplyr::select(., .data$code) else
                               if(return_val == "episode_datetime")             dplyr::select(., .data$episode_datetime) else
                                 if(return_val == "spell_interval")             dplyr::select(., .data$spell_interval)} %>%
                             dplyr::pull()
                           # Return the requested "return_val" type, or an appropriately typed NA
                           if(length(l)==0){
                             v <- switch (return_val,
                                          "code"             = NA_character_,
                                          "episode_datetime" = as.POSIXct(NA_real_),
                                          "spell_interval"   = lubridate::interval(as.POSIXct(NA_real_), as.POSIXct(NA_real_), tzone = "GMT"),
                                          "exists"           = FALSE)
                             return(v)
                           }else{
                             v <- switch (return_val,
                                          "exists"           = TRUE,
                                          l)
                             return(v)
                           }
                         },

                         #' @description Get the patients age on a specific date
                         #' @param datetime The datetime at which to evaluate the patient's age
                         #' @return the patient's age in years
                         age_at = function(datetime){
                           if(is.na(private$date_of_birth_)){
                             warning(cat("Error: Patient$", as.character(self$id), "'date of birth' is not set"))
                             return(NA_integer_)
                           }else if(!lubridate::is.POSIXct(datetime)){
                             warning(cat("Error: datetime variable '", as.character(datetime), "' is not of POSIXct type"))
                             return(NA_integer_)
                           }else{
                             age <- lubridate::as.period(lubridate::interval(private$date_of_birth_, datetime))$year
                             return(as.integer(age))
                           }
                         },

                         #' @description Query the patient's prescription list
                         #' @param medications A vector of medications names to look for e.g. c("aspirin", "clopidogrel")
                         #' @param time_window The time window to search within - must be a lubridate::interval() object
                         #' @param return_val A switch on what is being returned. If nothing found then NA is returned.
                         #' "num" returns the number of 'medication +ve' prescription events in the time window; uses 'any' if multiple medications provided
                         #' "date_min" returns the minimum date of the 'medication +ve' prescription events; uses 'any' if multiple medications provided
                         #' "date_max" returns the maximum date of the 'medication +ve' prescription events; uses 'any' if multiple medications provided
                         #' "any_exist" returns a boolean as to whether any of the medications exist in the time window
                         #' "all_exist" returns a boolean as to whether all of the medications exist in the time window
                         #' @return Depends on the return_val parameter
                         query_medications = function(medications, time_window, return_val){
                           # Input checks
                           stopifnot(
                             is.character(medications),
                             lubridate::is.interval(time_window),
                             return_val %in% c("num", "date_min", "date_max", "any_exist", "all_exist")
                           )

                           date_filter = purrr::map_lgl(private$prescription_list_, ~ lubridate::`%within%`(.x$datetime, time_window))
                           switch(return_val,
                                  "any_exist" = return(any(purrr::map_lgl(private$prescription_list_[date_filter], ~ .x$is_in(medications, "any")))),
                                  "all_exist" = return(all( # are all of the medications found
                                                           purrr::map_lgl(medications, function(med){ # cycle each medication provided; 1 logical for each drug: TRUE(exists in one or more of the prescriptions), FALSE(doesn't)
                                                               any( # is that medication in any of the prescription events
                                                                   purrr::map_lgl(private$prescription_list_[date_filter], function(prescription_evnt){
                                                                       prescription_evnt$is_in(med, "any")}))}))),
                                  "num" = return(sum(purrr::map_lgl(private$prescription_list_[date_filter], ~ .x$is_in(medications, "any")), na.rm = TRUE)),
                                  return("error")
                                  #TODO: add date function, these don't work....
                           #        "date_min" = min(purrr::map(private$prescription_list_[date_filter][purrr::map_lgl(private$prescription_list_[date_filter], ~ .x$is_in(medications, "any"))], ~ .x$date) %>% purrr::reduce(c)),
                           #        "date_max" = max(purrr::map(private$prescription_list_[date_filter][purrr::map_lgl(private$prescription_list_[date_filter], ~ .x$is_in(medications, "any"))], ~ .x$date) %>% purrr::reduce(c))
                                )
                           }
                       ),

# ACTIVE ---------------------------------------------------------------------
                       active = list(

                         #' @field date_of_birth
                         #' Returns the date_of_birth private field in a read-only way
                         date_of_birth = function(value){
                           if(missing(value)){
                             return(private$date_of_birth_)
                           }else{
                             stop("`$date_of_birth` is read only", call. = FALSE)
                           }
                         },

                         #' @field date_of_death
                         #' Returns the date_of_death private field in a read-only way
                         date_of_death = function(value){
                           if(missing(value)){
                             return(private$date_of_death_)
                           }else{
                             stop("`$date_of_death` is read only", call. = FALSE)
                           }
                         },

                         #' @field id
                         #' Returns the ID private field in a read-only way
                         id = function(value){
                           if(missing(value)){
                             if(!is.na(private$pseudo_nhs_id_)) return(private$pseudo_nhs_id_)
                             if(!is.na(private$nhs_number_id_)) return(private$nhs_number_id_)
                             if(!is.na(private$mrn_id_))        return(private$mrn_id_)
                           }else{
                             stop("`$id` is read only", call. = FALSE)
                           }
                         },

                         #' @field spell_list
                         #' Returns the patient's spell_list in a read-only way
                         spell_list = function(value){
                           if(missing(value)){
                             return(private$spell_list_)
                           }else{
                             stop("`$spell_list` is read only", call. = FALSE)
                           }
                         },

                         #' @field prescription_list
                         #' Returns the patient's prescription_list in a read-only way
                         prescription_list = function(value){
                           if(missing(value)){
                             return(private$prescription_list_)
                           }else{
                             stop("`$prescription_list` is read only", call. = FALSE)
                           }
                         }
                       ),

# PRIVATE ---------------------------------------------------------------------
                      private = list(
                        pseudo_nhs_id_ = NA,
                        nhs_number_id_ = NA,
                        mrn_id_ = NA,
                        date_of_birth_ = NA,
                        date_of_death_ = NA,
                        spell_list_ = NA,
                        prescription_list_ = NA
                      )

)
