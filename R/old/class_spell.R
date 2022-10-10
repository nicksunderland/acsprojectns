Spell <- R6::R6Class(classname = "Spell",
                     #inherit = Patient,

                       # PRIVATE ---------------------------------------------------------------------
                       private = list(
                         id_ = NA,
                         spell_start_datetime_ = NA,
                         spell_end_datetime_ = NA,
                         spell_type_ = NA,
                         episode_df_ = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("episode_start_datetime",
                                                                                          "episode_end_datetime",
                                                                                          "primary_diagnosis_icd_code",
                                                                                          "secondary_diagnosis_icd_codes"))
                       ),

                       # PUBLIC ---------------------------------------------------------------------
                       public = list(
                         initialize = function(id,
                                               spell_start_datetime,
                                               spell_end_datetime,
                                               spell_type,
                                               episode_df = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("episode_start_datetime",
                                                                                                               "episode_end_datetime",
                                                                                                               "primary_diagnosis_icd_code",
                                                                                                               "secondary_diagnosis_icd_codes"))){
                           # Input type checks; either all type correct or all NA
                           stopifnot((is.double(id) &
                                      lubridate::is.POSIXct(spell_start_datetime) &
                                      lubridate::is.POSIXct(spell_end_datetime) &
                                      is.character(spell_type) &
                                      is.data.frame(episode_df) &
                                      nrow(episode_df)>0) | all(is.na(list(id,
                                                                           spell_start_datetime,
                                                                           spell_end_datetime,
                                                                           spell_type,
                                                                           episode_df))))
                           # Assign object fields
                           private$id_                   <- id
                           private$spell_start_datetime_ <- spell_start_datetime
                           private$spell_end_datetime_   <- spell_end_datetime
                           private$spell_type_           <- spell_type
                           private$episode_df_           <- episode_df
                         },

                         print = function(...){
                           cat("Spell: \n")
                           cat("  Spell start:  ", as.character(private$spell_start_datetime_), "\n")
                           cat("  Spell end:    ", as.character(private$spell_end_datetime_), "\n")
                           cat("  Episodes:\n")
                           print(head(private$episode_df_))
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
                         spell_type = function(value){
                           if(missing(value)){
                             return(private$spell_type_)
                           }else{
                             stop("`$spell_type` is read only", call. = FALSE)
                           }
                         },
                         spell_interval = function(value){
                           if(missing(value)){
                             return(lubridate::interval(private$spell_start_datetime_, private$spell_end_datetime_, tzone = "GMT"))
                           }else{
                             stop("`$spell_interval` is read only", call. = FALSE)
                           }
                         },
                         spell_start_datetime = function(value){
                           if(missing(value)){
                             return(private$spell_start_datetime_)
                           }else{
                             stop("`$spell_start_datetime` is read only", call. = FALSE)
                           }
                         },
                         spell_end_datetime = function(value){
                           if(missing(value)){
                             return(private$spell_end_datetime_)
                           }else{
                             stop("`$spell_end_datetime` is read only", call. = FALSE)
                           }
                         },
                         num_episodes = function(value){
                           if(missing(value)){
                             return(length(private$episode_list_))
                           }else{
                             stop("`$num_episodes` is read only", call. = FALSE)
                           }
                         },
                         df_diagnosis_code_datetimes = function(value){
                           if(missing(value)){
                             df <- private$episode_df_ %>%
                                   tidyr::unnest(c(.data$primary_diagnosis_icd_code,
                                                   .data$secondary_diagnosis_icd_codes)) %>%
                                   dplyr::mutate(spell_interval = lubridate::interval(private$spell_start_datetime_, private$spell_end_datetime_))
                             df_p <- df %>% dplyr::select(episode_datetime = .data$episode_start_datetime,
                                                          code             = .data$primary_diagnosis_icd_code,
                                                          .data$spell_interval) %>%
                                            dplyr::mutate(type = "primary")
                             df_s <- df %>% dplyr::select(episode_datetime = .data$episode_start_datetime,
                                                          code             = .data$secondary_diagnosis_icd_codes,
                                                          .data$spell_interval) %>%
                                            dplyr::mutate(type = "secondary")
                             return(rbind(df_p, df_s))
                           }else{
                             stop("`$df_diagnosis_code_datetimes` is read only", call. = FALSE)
                           }
                         },
                         episode_df = function(value){
                           if(missing(value)){
                             return(private$episode_df_)
                           }
                           else{
                             stop("`$episode_df` is read only", call. = FALSE)
                           }
                         }
                       )
)
