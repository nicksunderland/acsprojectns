#' create_medication_timeseries
#'
#' @param cohort_df d
#' @param meds_df d
#' @param t_start d
#' @param t_end d
#' @param step d
#' @param window_span d
#' @param t_units d
#' @param search_meds d
#'
#' @importFrom furrr future_map_dfr furrr_options
#' @importFrom future multisession plan
#' @importFrom parallel detectCores
#'
#'
#' @return medication timeseries
#' @export
#'
create_medication_timeseries <- function(cohort_df, meds_df, t_start, t_end, step, window_span, t_units, search_meds, merge_func = function(x){mean(as.numeric(x),na.rm=T)}){
  # Input checks
  stopifnot(
    "The cohort_df must be a data frame" = is.data.frame(cohort_df),
    "The cohort_df must have some rows"  = nrow(cohort_df)>0,
    "The cohort_df must have colnames c('id', 'idx_event_date', 'swd_date_range')" =  setequal(colnames(cohort_df), c('id', 'idx_event_date', 'swd_date_range')),
    "The cohort_df column 'idx_event_date' must be posixct type" = lubridate::is.POSIXct(cohort_df[["idx_event_date"]]),
    "The cohort_df column 'swd_date_range' must be lubridate::interval type" = lubridate::is.interval(cohort_df[["swd_date_range"]]),
    "The meds_df must be a data frame" = is.data.frame(meds_df),
    "The meds_df must have some rows"  = nrow(meds_df)>0,
    "The meds_df must have colnames c('id', 'prescription_date', 'medication_name', 'data_col')" =  setequal(colnames(meds_df), c('id', 'prescription_date', 'medication_name', 'data_col')),
    "The meds_df$data_col must be numeric, logical, or parsable as numeric - i.e. !all(is.na(as.numeric(meds_df$data_col)))" = !all(is.na(as.numeric(meds_df$data_col))),
    "The meds_df column 'prescription_date' must be posixct type" = lubridate::is.POSIXct(meds_df[["prescription_date"]]),
    "The meds_df column 'medication_name' must not contain NAs" = all(!is.na(meds_df[["medication_name"]])),
    "t_start, t_end, step, & window_span must all be numeric" = all(purrr::map_lgl(c(t_start, t_end, step, window_span), ~is.numeric(.x))),
    "search_meds must be a character vector" = all(purrr::map_lgl(search_meds, ~ is.character(.x))),
    "t_units must be a valid lubridate::duration unit" = t_units %in% c("seconds", "minutes", "hours", "days", "weeks", "months", "years"),
    "merge_func must be a function capable of merging numeric or logical data if there is duplicate data in time windows" = is.function(merge_func)
  )

  meds_df = meds_df %>%
    fuzzyjoin::regex_right_join(data.frame(search_meds = search_meds), by=c("medication_name" = "search_meds"), ignore_case = TRUE) %>%
    dplyr::mutate(medication_name = .data$search_meds) %>%
    dplyr::select(-.data$search_meds)

  # Set up the looping through the time steps - do this in parallel using furrr
  message("Creating medication timeseries...")
  future::plan(future::multisession, workers=parallel::detectCores()-1)
  medication_timeseries = furrr::future_map_dfr(
    .x = seq(t_start, t_end, step),
    .f = function(i){
        # Create the time step window (we will later filter to make sure the meds exist within this window)
        cohort_tmp = cohort_df %>%
          dplyr::mutate(time_step = i,
                        step_time_win =
                          lubridate::interval(.data$idx_event_date + lubridate::duration(i, units=t_units),
                                              .data$idx_event_date + lubridate::duration(i+window_span, units=t_units),
                                              tzone="GMT")) %>%
          dplyr::select(-.data$idx_event_date)

        # Join (really filtering) on the medications we are interested in, using a regex join to account for composit naming
        # Then join the cohort and filter for only meds that were prescribed within this time step
        time_win_meds = meds_df %>%
          dplyr::left_join(cohort_tmp %>% dplyr::select(.data$id, .data$time_step, .data$step_time_win), by="id") %>%
          dplyr::filter(lubridate::`%within%`(.data$prescription_date, .data$step_time_win)) %>%
          dplyr::distinct(.data$id, .data$medication_name, .data$data_col) %>% # as not currently interested if more than one prescription in this time step (may need to change later)
          dplyr::full_join(data.frame(search_meds = search_meds), by=c("medication_name" = "search_meds")) %>% # ensure that all of the search meds are represented in the df
          tidyr::pivot_wider(id_cols     = .data$id,
                             names_from  = .data$medication_name,
                             values_from = .data$data_col,
                             values_fn   = merge_func) %>% # so that when pivot wider all meds get a column
          dplyr::filter(!is.na(.data$id)) # due to the full join on the search meds there may be rows with no ID (if there are no prescriptions to match a search med) - filter these out now that we have pivoted to get the correct columns

        # Whole cohort with meds
        cohort_tmp = cohort_tmp %>%
          dplyr::left_join(time_win_meds, by="id") %>%
          dplyr::mutate(in_swd_date_range = dplyr::if_else(lubridate::int_overlaps(.data$swd_date_range,.data$step_time_win),T,F)) %>%
          dplyr::select(.data$id, .data$time_step, .data$in_swd_date_range, dplyr::all_of(search_meds))

        return(cohort_tmp)
     },
    .options=furrr::furrr_options(globals=c("meds_df", "cohort_df")),
    .progress=TRUE)

}

