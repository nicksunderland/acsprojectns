#' load_admissions
#'
#' @param db_conn_struct list of Database objects
#' @param patients list of Patient objects
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return modifies the patient objects in-place
#'
#' @export
#'
load_admissions <- function(db_conn_struct,
                            patients){

  # Timing checks
  ptm <- proc.time() # Start the clock
  # pb  <- progress::progress_bar$new(format = "  downloading [:bar] :percent eta: :eta",
  #                                   total = length(patients), clear = FALSE, width= 60)

  # Checks and adjustments
  stopifnot(all(purrr::map_lgl(db_conn_struct, ~ checkmate::test_class(.x, "Database"))),
            all(purrr::map_lgl(patients,       ~ checkmate::test_class(.x, "Patient" ))))

  # Starting variables
  . = NULL
  ids = purrr::map_dbl(patients, ~ .x$id)

  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$sus_apc$is_connected){

    var_dict  <- create_data_table_variable_dictionary(db_conn_struct$sus_apc$table_name)    #create the named list to convert to standardised naming
    base_vars <- c(pseudo_nhs_id        = var_dict[["pseudo_nhs_id"]],                       #grab the basic variables we'll need
                   episode_start_date   = var_dict[["episode_start_date"]],
                   episode_end_date     = var_dict[["episode_end_date"]],
                   episode_start_time   = var_dict[["episode_start_time"]],
                   episode_end_time     = var_dict[["episode_end_time"]],
                   spell_start_date     = var_dict[["spell_start_date"]],
                   spell_end_date       = var_dict[["spell_end_date"]],
                   spell_start_date_alt = var_dict[["spell_start_date_alt"]],
                   spell_end_date_alt   = var_dict[["spell_end_date_alt"]],
                   spell_start_time     = var_dict[["spell_start_time"]],
                   spell_end_time       = var_dict[["spell_end_time"]])
    primary_diagnosis_col           <- c(primary_diagnosis_icd_code = var_dict[["primary_diagnosis_icd_code"]])
    secondary_diagnosis_cols        <- purrr::map(1:23, ~ var_dict[[sprintf("secondary_diagnosis_icd_code_%d",.x)]])
    sec_std_var_names               <- purrr::map(1:23, ~           sprintf("secondary_diagnosis_icd_code_%d",.x)  )
    names(secondary_diagnosis_cols) <- sec_std_var_names
    secondary_diagnosis_cols        <- unlist(secondary_diagnosis_cols)
    all_vars  <- c(base_vars, primary_diagnosis_col, secondary_diagnosis_cols)  #combine all variable names to use for the initial select


    # Auto-create the SQL query and execute
    # Had to use filter_at() instead of newer filter(dplyr::across()) as I think it struggles to convert to SQL code
    #     https://stackoverflow.com/questions/26497751/pass-a-vector-of-variable-names-to-arrange-in-dplyr
    #     https://dplyr.tidyverse.org/articles/programming.html#fnref1
    data <- db_conn_struct$sus_apc$data %>%                                                  #a reference to the SQL table
      dplyr::select(dplyr::all_of(all_vars)) %>%                                  #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                    .data$pseudo_nhs_id %in% ids) %>%
      #dplyr::show_query() %>%
      dplyr::collect() %>%                                                        #pull all the data to local
      dplyr::mutate(pseudo_nhs_id          = as.numeric(.data$pseudo_nhs_id),
                    episode_start_time     = tidyr::replace_na(.data$episode_start_time, as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    episode_end_time       = tidyr::replace_na(.data$episode_end_time,   as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    spell_start_time       = tidyr::replace_na(.data$spell_start_time,   as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    spell_end_time         = tidyr::replace_na(.data$spell_end_time,     as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    spell_start_date       = dplyr::coalesce(.data$spell_start_date, .data$spell_start_date_alt),
                    spell_end_date         = dplyr::coalesce(.data$spell_end_date,   .data$spell_end_date_alt)) %>%
      tidyr::drop_na(.data$episode_start_date,
                     .data$episode_end_date,
                     .data$spell_start_date,
                     .data$spell_end_date) %>%
      dplyr::mutate(episode_start_datetime = lubridate::ymd_hms(paste0(.data$episode_start_date, " ",
                                                                       lubridate::hour  (.data$episode_start_time), ":",
                                                                       lubridate::minute(.data$episode_start_time), ":",
                                                                       lubridate::second(.data$episode_start_time)), tz = "GMT"),
                    episode_end_datetime   = lubridate::ymd_hms(paste0(.data$episode_end_date, " ",
                                                                       lubridate::hour  (.data$episode_end_time), ":",
                                                                       lubridate::minute(.data$episode_end_time), ":",
                                                                       lubridate::second(.data$episode_end_time)), tz = "GMT"),
                    spell_start_datetime   = lubridate::ymd_hms(paste0(.data$spell_start_date, " ",
                                                                       lubridate::hour  (.data$spell_start_time), ":",
                                                                       lubridate::minute(.data$spell_start_time), ":",
                                                                       lubridate::second(.data$spell_start_time)), tz = "GMT"),    # need to feed in a date with a time zone
                    spell_end_datetime     = lubridate::ymd_hms(paste0(.data$spell_end_date, " ",
                                                                       lubridate::hour  (.data$spell_end_time), ":",
                                                                       lubridate::minute(.data$spell_end_time), ":",
                                                                       lubridate::second(.data$spell_end_time)), tz = "GMT")) %>%
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
                    .data$episode_end_datetime,
                    .data$spell_start_datetime,
                    .data$spell_end_datetime,
                    .data$primary_diagnosis_icd_code,
                    .data$secondary_diagnosis_icd_codes)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = ids)
    data <- dplyr::left_join(ids, data, by = "pseudo_nhs_id")  #creates NA entries for patients with missing data

    # ***The main loading part*** #
    data <- data %>%
      tidyr::nest(episodes_df = c(-.data$pseudo_nhs_id, -.data$spell_start_datetime, -.data$spell_end_datetime)) %>%  # same as group_by id/spellstart/spellend, but in one line
      purrr::pmap(., function(pseudo_nhs_id,
                                     spell_start_datetime,
                                     spell_end_datetime,
                                     episodes_df){
                                              Spell$new(pseudo_nhs_id,
                                                        spell_start_datetime,
                                                        spell_end_datetime,
                                                        "Inpatient",
                                                        episodes_df)}) %>%
      rlist::list.group(., .$id)

    # Since length(patients) == length(data); we can use map2 to set the spell_list field of each Patient object (in-place)
    # purrr::map2(patients, data, function(x, y){ stopifnot(x$id == y[[1]]$id);
    #                                                    x$set_spell_list(y) } )
    # dont know why purrr doesnt update the R6 object
    purrr::map2(patients, data, function(x, y){ stopifnot(x$id == y[[1]]$id);
                                                x$set_spell_list(y) } )

  }

  # Stop the clock
  message(paste("Time taken to execute load_admissions() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  invisible(NULL)
}
