#' query_swd_activity
#'
#' @param swd_activity_tbl the table of swd activity info
#' @param provider the activity provider type to assess; one or more of c("primary_care_contact", "secondary", "community")
#' @param setting the activity setting to assess; one or more of c("gp", "anp", "...")
#' @param urgency the activity urgency flag; one or more of c(" ", " ", "...")
#' @param speciality the speciality to assess; one or more of c(" ", " ", "...")
#' @param time_window in what time wiondow
#' @param return_vals what to return; one or more of c(" ", " ", "...")
#'
#' @return depends on the return value asked for
#' @export
#'
query_swd_activity = function(swd_activity_tbl, provider, setting, urgency, speciality, time_window, return_vals){

  # Input checks
  stopifnot(
    #TODO: make a renaming dictionary part of the activity load, so that things like primary_care_contact are standardised
    all(purrr::map_lgl(provider,   ~ .x %in% c("primary_care_contact", "secondary", "community", "mental_health"))) | all(is.na(provider)),
    all(purrr::map_lgl(setting,    ~ .x %in% c("gp", "gp locum", "anp", "hca", "ae", "critical", "ip", "maternity", "op"))) | all(is.na(setting)),
    all(purrr::map_lgl(urgency,    ~ .x %in% c("urgent", "routine", "same day", "elective", "first", "follow_up", "non_elective", "procedure", "other", "missing_unknown"))) | all(is.na(urgency)),
    all(purrr::map_lgl(speciality, ~ .x %in% c("cardiology service", "cardiothoracic surgery service", "cardiac rehabilitation service"))) | all(is.na(speciality)),
    lubridate::is.interval(time_window),
    all(return_vals %in% c("count", "cost", "date_min", "date_max"))
  )

  # Starting variables
  . = NULL

  # Return the requested "return_val" type, or an appropriately typed NA
  return_df = data.frame("count"    = NA_real_,
                         "cost"     = NA_real_,
                         "date_min" = as.POSIXct(NA_real_),
                         "date_max" = as.POSIXct(NA_real_))

  data <- swd_activity_tbl %>%
    dplyr::filter(lubridate::int_overlaps(time_window, .data$spell_interval)) %>%
    {if(!all(is.na(provider)))
      dplyr::filter(., .data$activity_provider %in% provider)
     else .} %>%
    {if(!all(is.na(setting)))
      dplyr::filter(., .data$activity_setting %in% setting)
     else .} %>%
    {if(!all(is.na(urgency)))
      dplyr::filter(., .data$activity_urgency %in% urgency)
      else .} %>%
    {if(!all(is.na(speciality)))
      dplyr::filter(., .data$activity_speciality %in% speciality)
      else .}

  # Return value
  # Store the results
  return_df[1, "count"]    <- nrow(data)
  return_df[1, "cost"]     <- sum(data$activity_cost, na.rm = TRUE)
  if(nrow(data)>0){
    return_df[1, "date_min"] <- min(lubridate::int_start(data$spell_interval), na.rm = TRUE)
    return_df[1, "date_max"] <- max(lubridate::int_start(data$spell_interval), na.rm = TRUE)
  }
  return(return_df[return_vals])
}
