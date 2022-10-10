#' Title query_icd10_codes
#'
#' @param codes A vector ICD-10 codes to search for e.g. c("I210", "I219")
#' @param search_strat_diag The diagnosis search strategy, one out of c("primary", "secondary", "all")
#' @param search_strat_date The date search strategy; return the most recent date with "max", or the most historical date with "min"
#' @param time_window The time window to search within - must be a lubridate::interval() object
#' @param return_vals A switch on what is being returned. If nothing found then NA is returned. Can be a vector, in which case multiple colums returned
#' "code" returns the ICD-10 code; this will return the first code in the input codes - so arrange the input codes by most important first
#' "spell_interval" returns the spell interval in which the ICD-10 code was found
#' @param hospital_events_tbl table with hospital admissions data
#' @param search_strat_adm_method the admission method to filter on, on of c(...)
#' "exists" returns a boolean as to whether a code was found within the time window
#' @return Depends on the return_val parameter
#'
#' @export
#'
query_icd10_codes <- function(hospital_events_tbl, codes, search_strat_diag, search_strat_date, search_strat_adm_method = "all", time_window, return_vals){

  # Input checks
  stopifnot(
    "Codes must all be character values"
      = is.character(codes),
    "Diagnosis search strategy must be either: 'primary', 'secondary', or 'all'"
      = search_strat_diag %in% c("primary", "secondary", "all"),
    "Date search strategy must be either: 'min', 'max'"
      = search_strat_date %in% c("min", "max"),
    "Admission method search strategy must be one of: 'all', 'emergency', 'elective'"
      = search_strat_adm_method %in% c("all", "emergency", "elective"),
    "The return values must be a vec tor contain any number of: c('code', 'diagnosis_datetime', 'spell_interval', 'any_exist', 'all_exist')"
      = all(purrr::map_lgl(return_vals, ~ .x %in% c("code", "diagnosis_datetime", "admission_method", "spell_interval", "any_exist", "all_exist"))),
    "The time_window must be a valid lubridate time interval object"
      = lubridate::is.interval(time_window)
  )

  # Return the requested "return_val" type, or an appropriately typed NA
  na_return = list("code"               = NA_character_,
                   "diagnosis_datetime" = as.POSIXct(NA_real_),
                   "admission_method"   = NA_character_,
                   "spell_interval"     = lubridate::interval(as.POSIXct(NA_real_), as.POSIXct(NA_real_), tzone = "GMT"),
                   "any_exist"          = FALSE,
                   "all_exist"          = FALSE)
  if(is.null(hospital_events_tbl)){
    return(na_return[return_vals])
  }

  # Starting variables
  . = NULL

  # Combine the diagnosis codes
  df_diagnosis_code_datetimes <- rbind(
    # The primary ICD10 codes
    hospital_events_tbl %>% dplyr::select(code               = .data$primary_diagnosis_icd_code,
                                          diagnosis_datetime = .data$episode_start_datetime,
                                          spell_interval     = .data$spell_interval,
                                          admission_method   = .data$admission_method) %>%
                            dplyr::mutate(type = "primary"),
    # The secondary ICD10 codes
    hospital_events_tbl %>% tidyr::unchop(., .data$secondary_diagnosis_icd_codes) %>%
                            dplyr::select(code               = .data$secondary_diagnosis_icd_codes,
                                          diagnosis_datetime = .data$episode_start_datetime,
                                          spell_interval     = .data$spell_interval,
                                          admission_method   = .data$admission_method) %>%
                            dplyr::mutate(type = "secondary")) %>%
    # Filter out NA codes
    dplyr::filter(!is.na(.data$code)) %>%
    dplyr::mutate(code = as.character(.data$code))


  # See if our codes are present
  l <- df_diagnosis_code_datetimes %>%
       dplyr::filter(.data$code %in% codes &
                     lubridate::`%within%`(.data$diagnosis_datetime, time_window) &
                     dplyr::case_when(search_strat_diag == "primary"   ~ .data$type == "primary",
                                      search_strat_diag == "secondary" ~ .data$type == "secondary",
                                      search_strat_diag == "all"       ~ TRUE,
                                      TRUE ~ TRUE) &
                     dplyr::case_when(search_strat_adm_method == "elective"  ~ base::grepl("elective" , .data$admission_method, ignore.case=TRUE),
                                      search_strat_adm_method == "emergency" ~ base::grepl("emergency", .data$admission_method, ignore.case=TRUE),
                                      search_strat_adm_method == "all"       ~ TRUE,
                                      TRUE ~ TRUE)) %>%
       dplyr::arrange(match(.data$code, codes)) %>% # arrange the codes by the input code vector (need to put the most important first)
       dplyr::mutate(any_exist = TRUE, # must be true if we have passed the filter above
                     all_exist = all(purrr::map_lgl(codes, ~ .x %in% .data$code))) %>% # test if all codes exists in the data frame
      {if(search_strat_date == "min")                    dplyr::slice_min(., .data$diagnosis_datetime, with_ties = FALSE) else
        if(search_strat_date == "max")                   dplyr::slice_max(., .data$diagnosis_datetime, with_ties = FALSE)}


  # Return the requested "return_val" type, or an appropriately typed NA
  if(nrow(l)==0){
    return(na_return[return_vals])
  }else{
    return(l[return_vals])
  }
}
