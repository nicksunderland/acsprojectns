#' query_gp_medications
#'
#' @param gp_medications_tbl the table of meds
#' @param medications the meds to search for; NA will search for all meds
#' @param time_window in what time wiondow
#' @param return_vals what to return
#' @param processing_strategy where to take each medication separate, look for any of the meds, or look for all of the meds
#'
#' @importFrom lubridate %within%
#'
#' @return depends on the return value asked for
#' @export
#'
query_gp_medications = function(gp_medications_tbl, medications, processing_strategy = "any", time_window, return_vals){

  #TODO: this actually doesn't work if you give it NA for medications; fix or remove NA as a possible input for medication list

  # Input checks
  stopifnot(
    "The medications tables must be a data.frame()" =
      is.data.frame(gp_medications_tbl),
    "The medications input value must be a valid character vector, or NA" =
      ((is.character(medications) & is.vector(medications)) | all(is.na(medications))),
    "The 'time_window' input must be a valid lubridate::interval object" =
      lubridate::is.interval(time_window),
    "The 'processing_strategy'[string] flag must be one of: 'any', 'all', 'for_each'" =
      processing_strategy %in% c("any", "all", "for_each"),
    "The 'return_vals' must be a character vector containing any of: c('num_scripts', 'av_quantity', 'av_dose', 'total_cost', 'date_min', 'date_max', 'exists')" =
      all(purrr::map_lgl(return_vals, ~ .x %in% c("num_scripts", "av_quantity", "av_dose", "total_cost", "date_min", "date_max", "exists")))
  )

  # Starting variables
  . = NULL

  # Process the medication data
  data <- gp_medications_tbl %>%
    dplyr::filter(lubridate::`%within%`(.data$episode_start_datetime, time_window)) %>%
    dplyr::filter(., !is.na(.data$medication_name)) %>%
    # fuzzyjoin::stringdist_right_join(data.frame(input_medications = medications),
    #                                  by       = c("medication_name" = "input_medications"),
    #                                  max_dist = 2) %>%

    # does this crash multithreding???
    {if(!all(is.na(medications)))
      fuzzyjoin::stringdist_right_join(., data.frame(input_medications = medications),
                                      by       = c("medication_name" = "input_medications"),
                                      max_dist = 2)
    else
      dplyr::mutate(., input_medications = .data$medication_name)} %>%

    # If doing things for each medication group_by the medications (using input_medications ensures consistency with the input search terms and avoids groups by slightly different database names e.g. warfarin and Warfarin)
    {if(processing_strategy == "for_each")
      dplyr::group_by(., .data$input_medications)
     else .} %>%

    # Gather some descriptives
    dplyr::mutate(num_scripts = ifelse(is.na(.data$medication_name), 0, dplyr::n()),
                  av_quantity = ifelse(is.na(.data$medication_name), NA_real_, mean(as.numeric(.data$prescription_quantity), na.rm = TRUE)),
                  av_dose     = ifelse(is.na(.data$medication_name), NA_real_, mean(as.numeric(.data$medication_dose), na.rm = TRUE)),
                  total_cost  = ifelse(is.na(.data$medication_name), NA_real_, sum(.data$prescription_cost, na.rm = TRUE)),
                  date_min    = ifelse(is.infinite(min(.data$episode_start_datetime, na.rm = TRUE)), as.POSIXct(NA), min(.data$episode_start_datetime, na.rm = TRUE)),
                  date_max    = ifelse(is.infinite(max(.data$episode_start_datetime, na.rm = TRUE)), as.POSIXct(NA), max(.data$episode_start_datetime, na.rm = TRUE)),
                  exists      = ifelse(is.na(.data$medication_name), FALSE, TRUE)) %>%

    # Ungroup the medication names, overwrite the exists column if needed
    {if(processing_strategy == "for_each" | all(is.na(medications)))
      dplyr::ungroup(.)
    else if(processing_strategy == "any")
      # Instead of filtering out the fuzzyjoin::stringdist_right_joins that don't exist; just boolean index out the ones that don't exist (so that we can continue to pipe the full dataframe on
      dplyr::mutate(., exists = any(medications %in% .data$input_medications[!is.na(.data$medication_name)]))
    else if(processing_strategy == "all")
      dplyr::mutate(., exists = all(medications %in% .data$input_medications[!is.na(.data$medication_name)]))
    else .} %>%

    # Select the descritives we want to return
    {if(processing_strategy == "for_each")
      dplyr::distinct(.,
                      .data$input_medications,
                      .data$num_scripts,
                      .data$av_quantity,
                      .data$av_dose,
                      .data$total_cost,
                      .data$date_min,
                      .data$date_max,
                      .data$exists) %>%
      tidyr::pivot_wider(names_from = .data$input_medications,
                         names_glue = "{input_medications}_{.value}",
                         values_from = c(.data$num_scripts,
                                         .data$av_quantity,
                                         .data$av_dose,
                                         .data$total_cost,
                                         .data$date_min,
                                         .data$date_max,
                                         .data$exists))
     else
       dplyr::distinct(.,
                       .data$num_scripts,
                       .data$av_quantity,
                       .data$av_dose,
                       .data$total_cost,
                       .data$date_min,
                       .data$date_max,
                       .data$exists)
      } %>%

    # Finally, only return the data requested in the return_vals vector
    dplyr::select(dplyr::ends_with(return_vals))

  # Return
  stopifnot(nrow(data) == 1)
  return(data)
}
