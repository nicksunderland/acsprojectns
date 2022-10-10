#' load_admissions
#'
#' @param patient_ids double, patient ids
#' @param db_conn_struct list of Database objects
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return modifies the patient objects in-place
#'
#' @export
#'
load_admissions_v2 <- function(db_conn_struct,
                               patient_ids){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Checks and adjustments
  stopifnot(all(purrr::map_lgl(db_conn_struct, ~ checkmate::test_class(.x, "Database"))),
            all(purrr::map_lgl(patient_ids,    ~ is.double(.x))))

  # Starting variables
  . = NULL
  admission_method_codes <- get_codes(file_path = system.file("admission_method_codes/admission_method_codes.csv", package = "acsprojectns"))


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
                   spell_end_time       = var_dict[["spell_end_time"]],
                   admission_method     = var_dict[["hospital_admission_method"]])
    #ICD diagnosis code columns
    primary_diagnosis_col           <- c(primary_diagnosis_icd_code = var_dict[["primary_diagnosis_icd_code"]])
    secondary_diagnosis_cols        <- purrr::map(1:23, ~ var_dict[[sprintf("secondary_diagnosis_icd_code_%d",.x)]])
    sec_std_var_names               <- purrr::map(1:23, ~           sprintf("secondary_diagnosis_icd_code_%d",.x)  )
    names(secondary_diagnosis_cols) <- sec_std_var_names
    secondary_diagnosis_cols        <- unlist(secondary_diagnosis_cols)
    #OPCS procedure code columns
    procedure_opcs_code_cols        <- purrr::map(1:24, ~ var_dict[[sprintf("procedure_opcs_code_%d",.x)]])
    procedure_col_names             <- purrr::map(1:24, ~           sprintf("procedure_opcs_code_%d",.x)  )
    names(procedure_opcs_code_cols) <- procedure_col_names
    procedure_opcs_code_cols        <- unlist(procedure_opcs_code_cols)
    all_vars  <- c(base_vars, primary_diagnosis_col, secondary_diagnosis_cols, procedure_opcs_code_cols)  #combine all variable names to use for the initial select


    # Auto-create the SQL query and execute
    # Had to use filter_at() instead of newer filter(dplyr::across()) as I think it struggles to convert to SQL code
    #     https://stackoverflow.com/questions/26497751/pass-a-vector-of-variable-names-to-arrange-in-dplyr
    #     https://dplyr.tidyverse.org/articles/programming.html#fnref1
    data <- db_conn_struct$sus_apc$data %>%                                                  #a reference to the SQL table
      dplyr::select(dplyr::all_of(all_vars)) %>%                                  #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                    .data$pseudo_nhs_id %in% patient_ids) %>%
      #dplyr::show_query() %>%
      dplyr::collect() %>%                                                        #pull all the data to local
      dplyr::mutate(episode_start_time     = lubridate::ymd_hms(.data$episode_start_time),
                    episode_end_time       = lubridate::ymd_hms(.data$episode_end_time),
                    spell_start_time       = lubridate::ymd_hms(.data$spell_start_time),
                    spell_end_time         = lubridate::ymd_hms(.data$spell_end_time)) %>%
      dplyr::mutate(pseudo_nhs_id          = as.numeric(.data$pseudo_nhs_id),
                    episode_start_time     = tidyr::replace_na(.data$episode_start_time, as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    episode_end_time       = tidyr::replace_na(.data$episode_end_time,   as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    spell_start_time       = tidyr::replace_na(.data$spell_start_time,   as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    spell_end_time         = tidyr::replace_na(.data$spell_end_time,     as.POSIXct(0, origin="1900-01-01", tz="UTC")),   # TODO: NOTE - there is the potential here to create negative length admissions, if the end time gets set to 00:00 on the same day as a start time that is know to be >00:00
                    spell_start_date       = dplyr::coalesce(.data$spell_start_date,     .data$spell_start_date_alt),
                    spell_end_date         = dplyr::coalesce(.data$spell_end_date,       .data$spell_end_date_alt),
                    spell_start_date       = dplyr::coalesce(.data$spell_start_date,     .data$episode_start_date),     # these two as last resort to deal with NAs
                    spell_end_date         = dplyr::coalesce(.data$spell_end_date,       .data$episode_end_date)) %>%   # these two as last resort to deal with NAs
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
                                                                       lubridate::second(.data$spell_end_time)), tz = "GMT"),
                    spell_interval         = lubridate::interval(.data$spell_start_datetime, .data$spell_end_datetime, tz="GMT")) %>%
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

      tidyr::unite(.,                                     # concatenate the requested columns to a "," separated string
                   col = "procedure_opcs_codes",
                   names(procedure_opcs_code_cols),
                   sep = ",",
                   remove = TRUE,
                   na.rm = FALSE) %>%

      dplyr::mutate(procedure_opcs_codes =  purrr::map(.x = .data$procedure_opcs_codes,
                                                       .f = function(x) purrr::map_chr(unique(unlist(strsplit(gsub(" ", "", x), ","))), ~ gsub("[[:punct:] ]+","", .x)) %>% unique(.) %>% purrr::discard(. == "" | .=="NA")),
                    procedure_opcs_codes = purrr::map(.data$procedure_opcs_codes, function(x) if(identical(x, character(0))) NA_character_ else x),
                    admission_method = as.character(.data$admission_method)) %>%
      dplyr::left_join(.,admission_method_codes, by = c("admission_method" = "admission_method_code")) %>%
      dplyr::mutate(admission_method = .data$admission_method_description) %>%
      dplyr::select(.data$pseudo_nhs_id,
                    .data$episode_start_datetime,
                    .data$spell_interval,
                    .data$admission_method,
                    .data$primary_diagnosis_icd_code,
                    .data$secondary_diagnosis_icd_codes,
                    .data$procedure_opcs_codes)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    data <- dplyr::left_join(ids, data, by = "pseudo_nhs_id") %>%  #creates NA entries for patients with missing data
      dplyr::group_nest(.data$pseudo_nhs_id, .key = "hospital_admissions")
  }

  # Stop the clock
  message(paste("Time taken to execute load_admissions() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  return(data)
}
