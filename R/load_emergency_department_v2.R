#' load_emergency_department
#'
#' @param patient_ids double, patient ids
#' @param db_conn_struct list of Database objects
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return the ED attendance tibble
#'
#' @export
#'
load_emergency_department_v2 <- function(db_conn_struct,
                                         patient_ids){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Checks and adjustments
  stopifnot(all(purrr::map_lgl(db_conn_struct, ~ checkmate::test_class(.x, "Database"))),
            all(purrr::map_lgl(patient_ids,    ~ is.double(.x))))

  # Starting variables
  . = NULL

  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$sus_ae$is_connected){

    var_dict  <- create_data_table_variable_dictionary(db_conn_struct$sus_ae$table_name)    #create the named list to convert to standardised naming
    base_vars <- c(pseudo_nhs_id          = var_dict[["pseudo_nhs_id"]],                       #grab the basic variables we'll need
                   episode_start_date     = var_dict[["episode_start_date"]],
                   episode_end_date       = var_dict[["episode_end_date"]],
                   episode_start_time     = var_dict[["episode_start_time"]],
                   episode_end_time       = var_dict[["episode_end_time"]],
                   episode_start_date_alt = var_dict[["episode_start_date_alt"]],
                   episode_end_date_alt   = var_dict[["episode_end_date_alt"]],
                   episode_start_time_alt = var_dict[["episode_start_time_alt"]],
                   episode_end_time_alt   = var_dict[["episode_end_time_alt"]])
    primary_diagnosis_col           <- c(primary_diagnosis_icd_code = var_dict[["primary_diagnosis_icd_code"]])
    secondary_diagnosis_cols        <- purrr::map(2:23, ~ var_dict[[sprintf("secondary_diagnosis_icd_code_%d",.x)]])
    sec_std_var_names               <- purrr::map(2:23, ~           sprintf("secondary_diagnosis_icd_code_%d",.x)  )
    names(secondary_diagnosis_cols) <- sec_std_var_names
    secondary_diagnosis_cols        <- unlist(secondary_diagnosis_cols)
    all_vars  <- c(base_vars, primary_diagnosis_col, secondary_diagnosis_cols)  #combine all variable names to use for the initial select


    # Auto-create the SQL query and execute
    # Had to use filter_at() instead of newer filter(dplyr::across()) as I think it struggles to convert to SQL code
    #     https://stackoverflow.com/questions/26497751/pass-a-vector-of-variable-names-to-arrange-in-dplyr
    #     https://dplyr.tidyverse.org/articles/programming.html#fnref1
    data <- db_conn_struct$sus_ae$data %>%                                                  #a reference to the SQL table
      dplyr::select(dplyr::all_of(all_vars)) %>%                                  #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                      .data$pseudo_nhs_id %in% patient_ids) %>%
      #dplyr::show_query() %>%
      dplyr::collect() %>%                                                        #pull all the data to local
      # needed this conversion to work at home
      dplyr::mutate(episode_start_time     = lubridate::ymd_hms(.data$episode_start_time),
                    episode_end_time       = lubridate::ymd_hms(.data$episode_end_time)) %>%
      dplyr::mutate(pseudo_nhs_id          = as.numeric(.data$pseudo_nhs_id),
                    episode_start_time_alt = tidyr::replace_na(.data$episode_start_time, as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    episode_end_time_alt   = tidyr::replace_na(.data$episode_end_time,   as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    episode_start_date     = dplyr::coalesce(.data$episode_start_date, .data$episode_start_date_alt),
                    episode_end_date       = dplyr::coalesce(.data$episode_end_date,   .data$episode_end_date_alt),
                    episode_start_time     = dplyr::coalesce(.data$episode_start_time, .data$episode_start_time_alt),
                    episode_end_time       = dplyr::coalesce(.data$episode_end_time,   .data$episode_end_time_alt)) %>%
      tidyr::drop_na(.data$episode_start_date,
                     .data$episode_end_date,
                     .data$episode_start_time,
                     .data$episode_end_time) %>%
      dplyr::mutate(episode_start_time     = lubridate::as_datetime(.data$episode_start_time),
                    episode_end_time       = lubridate::as_datetime(.data$episode_end_time),
                    episode_start_date     = lubridate::as_datetime(.data$episode_start_date),
                    episode_end_date       = lubridate::as_datetime(.data$episode_end_date)) %>%
      dplyr::mutate(episode_start_datetime = lubridate::ymd_hms(paste0(lubridate::year  (.data$episode_start_date), "-",
                                                                       lubridate::month (.data$episode_start_date), "-",
                                                                       lubridate::day   (.data$episode_start_date), "-",
                                                                       " ",
                                                                       lubridate::hour  (.data$episode_start_time), ":",
                                                                       lubridate::minute(.data$episode_start_time), ":",
                                                                       lubridate::second(.data$episode_start_time)), tz = "GMT"),
                    episode_end_datetime   = lubridate::ymd_hms(paste0(lubridate::year  (.data$episode_end_date), "-",
                                                                       lubridate::month (.data$episode_end_date), "-",
                                                                       lubridate::day   (.data$episode_end_date), "-",
                                                                       " ",
                                                                       lubridate::hour  (.data$episode_end_time), ":",
                                                                       lubridate::minute(.data$episode_end_time), ":",
                                                                       lubridate::second(.data$episode_end_time)), tz = "GMT"),
                    spell_interval         = lubridate::interval(.data$episode_start_datetime, .data$episode_end_datetime, tz="GMT")) %>%
      tidyr::unite(.,                                     # concatenate the requested columns to a "," separated string
                   col = "secondary_diagnosis_icd_codes",
                   names(secondary_diagnosis_cols),
                   sep = ",",
                   remove = TRUE,
                   na.rm = TRUE) %>%
      dplyr::mutate(primary_diagnosis_icd_code    = purrr::map(.x = .data$primary_diagnosis_icd_code,
                                                               .f = function(x) gsub("[[:punct:] ]+","", x)),
                    secondary_diagnosis_icd_codes = purrr::map(.x = .data$secondary_diagnosis_icd_codes,
                                                               .f = function(x) purrr::map_chr(unique(unlist(strsplit(gsub(" ", "", x), ","))), ~ gsub("[[:punct:] ]+","", .x)) %>% unique(.) %>% purrr::discard(. == "")),

                    secondary_diagnosis_icd_codes = purrr::map(.data$secondary_diagnosis_icd_codes, function(x) if(identical(x, character(0))) NA_character_ else x)) %>%
      dplyr::select(.data$pseudo_nhs_id,
                    .data$episode_start_datetime,
                    .data$spell_interval,
                    .data$primary_diagnosis_icd_code,
                    .data$secondary_diagnosis_icd_codes)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    data <- dplyr::left_join(ids, data, by = "pseudo_nhs_id") %>% #creates NA entries for patients with missing data
      dplyr::group_nest(.data$pseudo_nhs_id, .key = "ed_attendances")
  }

  # Stop the clock
  message(paste("Time taken to execute load_admissions() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  return(data)






}
