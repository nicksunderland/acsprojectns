Episode <- R6::R6Class(classname = "Episode",

                       # PRIVATE ---------------------------------------------------------------------
                       private = list(
                         episode_start_datetime_ = NA,
                         episode_end_datetime_ = NA,
                         primary_diagnosis_ = NA,
                         secondary_diagnoses_ = NA,
                         primary_procedure_ = NA,
                         primary_procedure_date_ = NA,
                         secondary_procedures_ = NA,
                         secondary_procedures_dates_ = NA
                       ),

                       # PUBLIC ---------------------------------------------------------------------
                       public = list(
                         initialize = function(episode_start_datetime,
                                               episode_end_datetime,
                                               primary_diagnosis_icd_code,
                                               secondary_diagnosis_icd_codes){

                           # private$id_                     <- pseudo_nhs_id
                           private$episode_start_datetime_ <- episode_start_datetime
                           private$episode_end_datetime_   <- episode_end_datetime
                           private$primary_diagnosis_      <- primary_diagnosis_icd_code
                           private$secondary_diagnoses_    <- secondary_diagnosis_icd_codes

                         },
                         print = function(...){
                           cat("Episode: \n")
                           cat("  start datetime:      ", as.character(private$episode_start_datetime_), "\n", sep = "")
                           cat("  end datetime:        ", as.character(private$spell_start_datetime_), "\n", sep = "")
                           cat("  primary diagnosis:   ", as.character(private$primary_diagnosis_), "\n", sep = "")
                           cat("  secondary diagnoses:", as.character(private$secondary_diagnoses_), "\n", sep = " ")
                         }
                       ),

                       # ACTIVE ---------------------------------------------------------------------
                       active = list(
                         episode_start_datetime = function(){
                           return(as.POSIXct(private$episode_start_datetime_))
                         },
                         episode_end_datetime = function(){
                           return(as.POSIXct(private$episode_end_datetime_))
                         },
                         primary_diagnosis = function(){
                           return(as.character(private$primary_diagnosis_))
                         },
                         secondary_diagnoses = function(){
                           return(as.character(private$secondary_diagnoses_))
                         }
                       )
)
