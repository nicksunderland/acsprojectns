#' #' query_ed_activity
#' #'
#' #' @param time_window in what time wiondow
#' #' @param return_vals what to return; one or more of c(" ", " ", "...")
#' #' @param ed_activity_tbl the ed activity table
#' #' @param codes vector of ICD10 codes
#' #' @param search_strat_diag The diagnosis search strategy, one out of c("primary", "secondary", "all")
#' #' @param search_strat_date The date search strategy; return the most recent date with "max", or the most historical date with "min"
#' #'
#' #' @return depends on the return value asked for
#' #' @export
#' #'
#' query_ed_activity = function(ed_activity_tbl, codes, search_strat_diag, search_strat_date, time_window, return_vals){
#'
#'   # Input checks
#'   stopifnot(
#'     "Codes must all be character values"
#'     = is.character(codes),
#'     "Diagnosis search strategy must be either: 'primary', 'secondary', or 'all'"
#'     = search_strat_diag %in% c("primary", "secondary", "all"),
#'     "Date search strategy must be either: 'min', 'max'"
#'     = search_strat_date %in% c("min", "max"),
#'     "The return values must be a vec tor contain any number of: c('code', 'diagnosis_datetime', 'spell_interval', 'any_exist', 'all_exist')"
#'     = all(purrr::map_lgl(return_vals, ~ .x %in% c("code", "diagnosis_datetime", "any_exist", "all_exist", "count"))),
#'     "The time_window must be a valid lubridate time interval object"
#'     = lubridate::is.interval(time_window)
#'   )
#'
#'   # Starting variables
#'   . = NULL
#'
#'   # Return the requested "return_val" type, or an appropriately typed NA
#'   return_df = data.frame("code"               = NA_character_,
#'                          "diagnosis_datetime" = as.POSIXct(NA_real_),
#'                          "admission_method"   = NA_character_,
#'                          "spell_interval"     = lubridate::interval(as.POSIXct(NA_real_), as.POSIXct(NA_real_), tzone = "GMT"),
#'                          "any_exist"          = FALSE,
#'                    "all_exist"          = FALSE)
#'   return_df = data.frame("count"    = NA_real_,
#'                          "cost"     = NA_real_,
#'                          "date_min" = as.POSIXct(NA_real_),
#'                          "date_max" = as.POSIXct(NA_real_))
#'
#'   data <- swd_activity_tbl %>%
#'     dplyr::filter(lubridate::int_overlaps(time_window, .data$spell_interval)) %>%
#'     {if(!all(is.na(provider)))
#'       dplyr::filter(., .data$activity_provider %in% provider)
#'       else .} %>%
#'     {if(!all(is.na(setting)))
#'       dplyr::filter(., .data$activity_setting %in% setting)
#'       else .} %>%
#'     {if(!all(is.na(urgency)))
#'       dplyr::filter(., .data$activity_urgency %in% urgency)
#'       else .} %>%
#'     {if(!all(is.na(speciality)))
#'       dplyr::filter(., .data$activity_speciality %in% speciality)
#'       else .}
#'
#'   # Return value
#'   # Store the results
#'   return_df[1, "count"]    <- nrow(data)
#'   return_df[1, "cost"]     <- sum(data$activity_cost, na.rm = TRUE)
#'   if(nrow(data)>0){
#'     return_df[1, "date_min"] <- min(lubridate::int_start(data$spell_interval), na.rm = TRUE)
#'     return_df[1, "date_max"] <- max(lubridate::int_start(data$spell_interval), na.rm = TRUE)
#'   }
#'   return(return_df[return_vals])
#' }
