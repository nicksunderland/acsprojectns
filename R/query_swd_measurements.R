#' query_swd_measurements
#'
#' @param swd_measurements_tbl table of measurements
#' @param measurements which measurements to extract, a vector containing one or more of c('systolic_bp', 'diastolic_bp', 'cholesterol', 'hba1c', 'egfr')
#' @param time_window in what time window
#' @param return_vals what to return; one or more of c('count', 'value_date_min', 'value_date_max', 'date_min', 'date_max', 'mean', 'median', 'min', 'max')
#'
#' @return depends on the return value asked for
#'
#' @importFrom lubridate %within%
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @export
#'
query_swd_measurements = function(swd_measurements_tbl, measurements, time_window, return_vals){

  # Input checks
  stopifnot(
    "The measurements table must be a data.frame()" =
      is.data.frame(swd_measurements_tbl),
    "The measurements input value must be a valid character vector" =
      (is.character(measurements) & is.vector(measurements)),
    "The 'time_window' input must be a valid lubridate::interval object" =
      lubridate::is.interval(time_window),
    "The 'return_vals' must be a character vector containing any of: c('count', 'value_date_min', 'value_date_max',  'mean', 'median', 'min', 'max', 'date_min', 'date_max')" =
      all(purrr::map_lgl(return_vals, ~ .x %in% c('count', 'value_date_min', 'value_date_max', 'date_min', 'date_max', 'mean', 'median', 'min', 'max')))
  )

  # Starting variables
  . = NULL
  if(any(c('systolic_bp', 'diastolic_bp') %in% measurements)){
    measurements_tmp <- c(measurements, "blood_pressure")
  }else{
    measurements_tmp <- c(measurements)
  }

  data <- swd_measurements_tbl %>%
   dplyr::filter(lubridate::`%within%`(.data$measurement_datetime, time_window) &
                 .data$measurement_type %in% measurements_tmp) %>%
    stats::na.exclude(.data) %>%
    dplyr::mutate(systolic_bp = dplyr::case_when(.data$measurement_type == "blood_pressure"
                                                 ~ stringr::str_extract(.data$measurement_value, "\\d+(?=/)"),
                                                 TRUE ~ NA_character_),
                  diastolic_bp = dplyr::case_when(.data$measurement_type == "blood_pressure"
                                                  ~ stringr::str_extract(.data$measurement_value, "(?<=/)\\d+"),
                                                  TRUE ~ NA_character_)) %>%
    tidyr::pivot_longer(cols = c(.data$systolic_bp, .data$diastolic_bp),
                        names_to  = "blood_pressure_component",
                        values_to = "bp_value") %>%
    dplyr::mutate(measurement_value = dplyr::if_else(.data$measurement_type == "blood_pressure", .data$bp_value,                 .data$measurement_value),
                  measurement_value = as.numeric(.data$measurement_value),
                  measurement_type  = dplyr::if_else(.data$measurement_type == "blood_pressure", .data$blood_pressure_component, .data$measurement_type)) %>%
    fuzzyjoin::stringdist_right_join(data.frame(input_measurements = measurements),
                                     by       = c("measurement_type" = "input_measurements"),
                                     max_dist = 2) %>%
    dplyr::mutate(measurement_type = .data$input_measurements) %>%
    dplyr::select(.data$measurement_type,
                  .data$measurement_datetime,
                  .data$measurement_value) %>%
    dplyr::group_by(.data$measurement_type) %>%
    dplyr::mutate(count = dplyr::if_else(is.na(.data$measurement_value), as.integer(0), dplyr::n())) %>%
    dplyr::summarise(count  = ifelse(!all(is.na(.data$count)),             mean(.data$count,                      na.rm=T), NA_real_),
                     mean   = ifelse(!all(is.na(.data$measurement_value)), mean(.data$measurement_value,          na.rm=T), NA_real_),
                     median = ifelse(!all(is.na(.data$measurement_value)), stats::median(.data$measurement_value, na.rm=T), NA_real_),
                     min    = ifelse(!all(is.na(.data$measurement_value)), min(.data$measurement_value,           na.rm=T), NA_real_),
                     max    = ifelse(!all(is.na(.data$measurement_value)), max(.data$measurement_value,           na.rm=T), NA_real_),
             value_date_min = ifelse(!all(is.na(.data$measurement_value)), .data$measurement_value[which.min(.data$measurement_datetime)], NA_real_),
             value_date_max = ifelse(!all(is.na(.data$measurement_value)), .data$measurement_value[which.max(.data$measurement_datetime)], NA_real_),
                   date_min = dplyr::if_else(!all(is.na(.data$measurement_value)), min(.data$measurement_datetime,        na.rm=T), as.POSIXct(NA)),
                   date_max = dplyr::if_else(!all(is.na(.data$measurement_value)), max(.data$measurement_datetime,        na.rm=T), as.POSIXct(NA))) %>%
    tidyr::pivot_wider(names_from = .data$measurement_type,
                       names_glue = "{measurement_type}_{.value}",
                       values_from = c(.data$count,
                                       .data$mean,
                                       .data$median,
                                       .data$min,
                                       .data$max,
                                       .data$value_date_min,
                                       .data$value_date_max,
                                       .data$date_min,
                                       .data$date_max)) %>%

    # Finally, only return the data requested in the return_vals vector
    dplyr::select(dplyr::starts_with(measurements) &
                  dplyr::ends_with(return_vals))

  return(data)
}
