#' get_hospital_admissions
#'
#' @param sus_apc_obj an R6 Database object connected to the APC_SUS database
#' @param codes_list list of ICD-10 diagnosis codes or OPCS procedure codes to search for
#' @param datetime_window the beginning and end of the date window (inclusive), must be a lubridate::interval object e.g. lubridate::interval({start}, {end}, tzone = "GMT")
#' @param ccg_provider_code_list list of CCG provider codes, default = ALL
#' @param verbose TRUE = print SQL query to console
#' @param level whether to extract consultant episode or spell level data
#' @param search_strategy whether to look in primary diagnosis field, secondary diagnosis field, all diagnosis fields, procedure fields
#' @param id_list vector list of character pseudo NHS ids to filter on
#' @param return_all_codes whether to return all the ICD-10 codes associated with the episode/spell, or just those in the 'codes_list'
#' @param search_strat_adm_method the admission method
#'
#' @return a data frame
#'
#' @importFrom lubridate %within%
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#' @export
#'
#'
get_hospital_admissions <- function(sus_apc_obj,
                                    codes_list = NA,
                                    return_all_codes = FALSE,
                                    search_strategy = "primary_diagnosis",
                                    search_strat_adm_method = "all",
                                    datetime_window = lubridate::interval(NA, NA, tzone = "GMT"),
                                    id_list = NA,
                                    ccg_provider_code_list = NA,
                                    level = "spell",
                                    verbose = FALSE){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Input checks
  stopifnot(R6::is.R6(sus_apc_obj),
            (!all(is.na(codes_list)) | !all(is.na(id_list))),         # either need diagnosis codes OR ids to search for
            (is.atomic(codes_list) | is.na(codes_list)),    # diagnosis codes need to be a vector, or NA; if NA will need ids to search for
            search_strategy %in% c("primary_diagnosis", "secondary_diagnoses", "primary_and_secondary_diagnoses",
                                   "procedures", NA),# need a valid switch for where to search for diagnosis / procedure codes
            (lubridate::is.interval(datetime_window) & !is.na(datetime_window) & length(datetime_window) == 1),                      # must be a lubridate::interval for the date window
            (is.atomic(id_list) | is.na(id_list)),                              # ids list must be a vector, or NA
            (is.vector(ccg_provider_code_list) | is.na(ccg_provider_code_list)),# ccg provider list must be a vector, or NA
            "Admission method search strategy must be one of: 'all', 'emergency', 'elective'"
             = search_strat_adm_method %in% c("all", "emergency", "elective"),
            level %in% c("spell", "episode"),                                   # valid level
            is.logical(verbose))


  # Starting variables
  . = NULL
  admission_method_codes <- get_codes(file_path = system.file("admission_method_codes/admission_method_codes.csv", package = "acsprojectns"))
  var_dict  <- create_data_table_variable_dictionary(sus_apc_obj$table_name)    #create the named list to convert to standardised naming
  base_vars <- c(pseudo_nhs_id        = var_dict[["pseudo_nhs_id"]],            #grab the basic variables we'll need
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
                 ccg_provider_code    = var_dict[["ccg_provider_code"]],
                 admission_method     = var_dict[["hospital_admission_method"]])
  code_vars <- create_list_of_code_column_names(search_strategy, sus_apc_obj$table_name)
  all_vars  <- c(base_vars, code_vars)                                          #combine all variable names to use for the initial select
  win_start <- lubridate::int_start(datetime_window)
  win_end   <- lubridate::int_end(datetime_window)


  # Auto-create the SQL query and execute
  # Had to use filter_at() instead of newer filter(dplyr::across()) as I think it struggles to convert to SQL code
  #     https://stackoverflow.com/questions/26497751/pass-a-vector-of-variable-names-to-arrange-in-dplyr
  #     https://dplyr.tidyverse.org/articles/programming.html#fnref1
  data <- sus_apc_obj$data %>%                                                  #a reference to the SQL table
    dplyr::select(dplyr::all_of(all_vars)) %>%                                  #select the variables to work with
    dplyr::distinct() %>%
    dplyr::filter(!is.na(.data$pseudo_nhs_id) &                                 #leave if no id, as can't do anything with these
                  .data$episode_start_date >= win_start &                  #make sure inside the date window
                  .data$episode_start_date <= win_end) %>%
    {if(!all(is.na(codes_list)))             dplyr::filter_at(., dplyr::vars(names(code_vars)),#search either the primary, the secondary, or both, diagnosis code columns for the wanted codes
                                                                 dplyr::any_vars(. %in% codes_list)) else .} %>%
    {if(!all(is.na(id_list)))                dplyr::filter(., .data$pseudo_nhs_id %in% id_list) else .} %>% #if a list of ids are provided then filter for these
    {if(!all(is.na(ccg_provider_code_list))) dplyr::filter(., .data$ccg_provider_code %in% ccg_provider_code_list) else .} %>%  #if a list of CCG provder codes are provided then filter for these,
    {if(verbose)                             dplyr::show_query(.,) else .} %>%
    dplyr::collect() %>%                                                        #pull all the data to local
    dplyr::mutate(admission_method = as.character(.data$admission_method)) %>%  #ensure character type
    dplyr::left_join(., admission_method_codes, by = c("admission_method" = "admission_method_code")) %>%
    dplyr::mutate(admission_method = .data$admission_method_description) %>%
    dplyr::filter(dplyr::case_when(search_strat_adm_method == "elective"  ~ base::grepl("elective" , .data$admission_method, ignore.case=TRUE),
                                   search_strat_adm_method == "emergency" ~ base::grepl("emergency", .data$admission_method, ignore.case=TRUE),
                                   search_strat_adm_method == "all"       ~ TRUE,
                                   TRUE ~ TRUE)) %>%
    dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id))



  #Return as episode or spell level data
  if(level == "episode"){

    # If nothing found, just return an empty database with the same final col names (for compatibility with functions that use this function)
    if(nrow(data) == 0) return(stats::setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pseudo_nhs_id", "episode_interval", "ccg_provider_code", "episode_codes")))

    data <- data %>%
      dplyr::mutate(episode_start_time     = lubridate::ymd_hms(.data$episode_start_time),
                    episode_end_time       = lubridate::ymd_hms(.data$episode_end_time), ) %>%
      dplyr::mutate(episode_start_time     = tidyr::replace_na(.data$episode_start_time, as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    episode_end_time       = tidyr::replace_na(.data$episode_end_time,   as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                    episode_start_datetime = lubridate::ymd_hms(paste0(.data$episode_start_date, " ",
                                                                       lubridate::hour  (.data$episode_start_time), ":",
                                                                       lubridate::minute(.data$episode_start_time), ":",
                                                                       lubridate::second(.data$episode_start_time)), tz = "GMT"),
                    episode_end_datetime   = lubridate::ymd_hms(paste0(.data$episode_end_date, " ",
                                                                       lubridate::hour  (.data$episode_end_time), ":",
                                                                       lubridate::minute(.data$episode_end_time), ":",
                                                                       lubridate::second(.data$episode_end_time)), tz = "GMT"),
                    episode_interval       = lubridate::interval(.data$episode_start_datetime, .data$episode_end_datetime)) %>%
      dplyr::filter(.data$episode_interval %within% datetime_window) %>% #need to do this as we may have messed around with the datetime above; make sure it's still what we asked for
      tidyr::unite(.,                                     # concatenate the requested columns to a "," separated string
                   col = "episode_codes",
                   names(code_vars),
                   sep = ",",
                   remove = TRUE,
                   na.rm = TRUE) %>%
      dplyr::mutate(., episode_codes = purrr::map(.x = .data$episode_codes,
                                                  .f = function(x) unique(unlist(strsplit(trimws(x),","))))) %>%
      {if(!return_all_codes)
        dplyr::mutate(., episode_codes = purrr::map(.x = .data$episode_codes,
                                                    .f = function(x) base::intersect(x, codes_list)))
       else .} %>%
      dplyr::select(.data$pseudo_nhs_id,
                    .data$episode_interval,
                    .data$admission_method,
                    .data$ccg_provider_code,
                    .data$episode_codes)

  }else if(level == "spell"){

    # If nothing found, just return an empty database with the same final col names (for compatibility with functions that use this function)
    if(nrow(data) == 0) return(stats::setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pseudo_nhs_id", "episode_interval", "ccg_provider_code", "spell_codes")))

    # Need to process the spell data to remove conflicts
    data <- data %>%
       dplyr::mutate(spell_start_time     = lubridate::ymd_hms(.data$spell_start_time),
                     spell_end_time       = lubridate::ymd_hms(.data$spell_end_time)) %>%
       dplyr::mutate(spell_start_time     = tidyr::replace_na(.data$spell_start_time, as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                     spell_end_time       = tidyr::replace_na(.data$spell_end_time,   as.POSIXct(0, origin="1900-01-01", tz="UTC")),
                     spell_start_date     = dplyr::coalesce(.data$spell_start_date, .data$spell_start_date_alt),
                     spell_end_date       = dplyr::coalesce(.data$spell_end_date,   .data$spell_end_date_alt)) %>%  #try to get the date data from the available places, if no time data just set to zero.
       dplyr::filter(!is.na(.data$spell_start_date) & !is.na(.data$spell_end_date)) %>% #need to have spell date data
       dplyr::mutate(spell_start_datetime = lubridate::ymd_hms(paste0(.data$spell_start_date, " ",
                                                                      lubridate::hour  (.data$spell_start_time), ":",
                                                                      lubridate::minute(.data$spell_start_time), ":",
                                                                      lubridate::second(.data$spell_start_time)), tz = "GMT"),    # need to feed in a date with a time zone
                     spell_end_datetime   = lubridate::ymd_hms(paste0(.data$spell_end_date, " ",
                                                                      lubridate::hour  (.data$spell_end_time), ":",
                                                                      lubridate::minute(.data$spell_end_time), ":",
                                                                      lubridate::second(.data$spell_end_time)), tz = "GMT"),
                     spell_interval       = lubridate::interval(.data$spell_start_datetime, .data$spell_end_datetime)) %>%
      dplyr::filter(!is.na(.data$spell_interval) & # ensure all data is a valid interval, hopefully unnecessary given above
                    .data$spell_interval %within% datetime_window) %>% #need to do this as we may have messed around with the datetime above; make sure it's still what we asked for
      dplyr::select(.data$pseudo_nhs_id,
                    .data$spell_interval,
                    .data$admission_method,
                    .data$ccg_provider_code,
                    names(code_vars)) %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%

      # This takes a long time (see function below), as it compares each episodes with every other episode attached to the patient to see if they overlap. If they do then it merges them together into a single 'Spell'
      dplyr::group_modify(.f = process_overlaps_of_time_intervals,
                          time_interval_col = .data$spell_interval,
                          keep_spell = "longest",
                          cols_to_combine = names(code_vars),
                          combined_col_name = "spell_codes",
                          remove_input_cols_flag = TRUE) %>%
      dplyr::ungroup() %>%
      {if(!return_all_codes & !all(is.na(codes_list)))
        dplyr::mutate(., spell_codes = purrr::map(.x = .data$spell_codes,
                                                  .f = function(x) base::intersect(x, codes_list)))
       else .}

      #**************just to speed things up for testing, above modify() code takes ages
      # dplyr::mutate(combined_columns = names(code_vars))
      #**************

  }else{
    warning("Error processing episode/spell data in get_hospital_admissions()")
  }

  if(verbose){
    message(paste("Time taken to execute get_hospital_admissions() =",
                  round((proc.time() - ptm)[3], digits=2)),
                  " seconds") # Stop the clock
  }

  return(data)
}


#' create_list_of_code_column_names
#'
#' @param s one of c("primary_diagnosis", "secondary_diagnoses", "primary_and_secondary_diagnoses",
#' "primary_procedure", "secondary_procedures", "primary_and_secondary_procedures", NA)
#'
#' @return list of variable names

#' @param s the search strategy
#' @param data_table_name the name of the datatable
#'
#' @importFrom magrittr "%>%"
#' @export
#'
#' @examples create_list_of_code_column_names("primary", "vw_APC_SEM_001")
#'
create_list_of_code_column_names <- function(s, data_table_name){

  var_dict <- create_data_table_variable_dictionary(data_table_name)

          #create the named list to convert to standardised naming
  switch(s,
         "primary_diagnosis"   =             {primary_diagnosis_col           <- var_dict[["primary_diagnosis_icd_code"]]
                                              pri_std_var_name                <- "primary_diagnosis_icd_code"
                                              names(primary_diagnosis_col)    <- pri_std_var_name
                                              return(primary_diagnosis_col)},

         "secondary_diagnoses" =             {secondary_diagnosis_cols        <- purrr::map(1:23, function(x){ var_dict[[sprintf("secondary_diagnosis_icd_code_%d",x)]]})
                                              sec_std_var_names               <- purrr::map(1:23, function(x){           sprintf("secondary_diagnosis_icd_code_%d",x)  })
                                              names(secondary_diagnosis_cols) <- sec_std_var_names
                                              return(unlist(secondary_diagnosis_cols))},

         "primary_and_secondary_diagnoses" = {primary_diagnosis_col           <- var_dict[["primary_diagnosis_icd_code"]]
                                              pri_std_var_name                <- "primary_diagnosis_icd_code"
                                              names(primary_diagnosis_col)    <- pri_std_var_name
                                              secondary_diagnosis_cols        <- purrr::map(1:23, function(x){ var_dict[[sprintf("secondary_diagnosis_icd_code_%d",x)]]})
                                              sec_std_var_names               <- purrr::map(1:23, function(x){           sprintf("secondary_diagnosis_icd_code_%d",x)  })
                                              names(secondary_diagnosis_cols) <- sec_std_var_names
                                              return(c(primary_diagnosis_col, unlist(secondary_diagnosis_cols)))},

         "procedures"           =            {procedure_cols                  <- purrr::map(1:24, function(x){ var_dict[[sprintf("procedure_opcs_code_%d",x)]] })
                                              procedure_col_names             <- purrr::map(1:24, function(x){           sprintf("procedure_opcs_code_%d",x)   })
                                              names(procedure_cols)           <- procedure_col_names
                                              return(unlist(procedure_cols))},

         {return(c(""))}
  )
}


#' process_overlaps_of_time_intervals
#'
#' @param time_interval_df a dataframe with lubridate time interval data +/- other columns
#' @param .y the key, see notes for dplyr::group_modify
#' @param time_interval_col the column name of the column holding the time interval data
#' @param keep_spell c("longest", "shortest") how to process overlapping time intervals, keep longest or shortest
#' @param cols_to_combine when there are overlaps in the time intervals, which column should be combined (into a list)
#' @param remove_input_cols_flag TRUE/FALSE whether or not to remove the original columns that have been combined
#' @param combined_col_name the name of the column that will be returned containing the data in columns listed in col_to_combine
#'
#' @return a dataframe with overlapping time intervals removed +/- aggregation of cols_to_combine into a single column
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#' @importFrom rlang :=
#' @export
#'
process_overlaps_of_time_intervals <- function(time_interval_df,
                                               .y,
                                               time_interval_col,
                                               keep_spell = "longest",
                                               cols_to_combine = NA,
                                               combined_col_name = "combined_columns",
                                               remove_input_cols_flag = TRUE){

  # writing functions that take column names as variables:
  #     https://dplyr.tidyverse.org/articles/programming.html#fnref1
  # https://stackoverflow.com/questions/58283935/r-determine-if-each-date-interval-overlaps-with-all-other-date-intervals-in-a-d

  # Variable checks
  stopifnot(keep_spell %in% c("longest", "shortest"))

  # Starting variables
  . = NULL

  # Look for overlaps and apply settings
  time_interval_df <- time_interval_df %>%
    {if(nrow(time_interval_df)>1){
      dplyr::mutate(., overlaps = purrr::map(.x = seq_along({{time_interval_col}}),  # loop each spell interval and apply the below function
                                             .f = function(this_spell_idx){
                                               all_spell_idxs = seq_along({{time_interval_col}}) # int array 1:nrow(spell_interval)
                                               overlap_bool_vec = lubridate::int_overlaps({{time_interval_col}}[this_spell_idx], {{time_interval_col}}[all_spell_idxs]) # does this spell overlap any other spells in this group, including itself?
                                               overlap_idxs = sort(all_spell_idxs[overlap_bool_vec]) # ascending order of overlapping spell indices
                                               return(overlap_idxs) # this is now the 'grouping signature'  e.g. c(1, 2, 3, 5)
                                             }))
    } else {
      dplyr::mutate(., overlaps = c(1)) # did this to see if bypassing the comparing function sped things up if there was only one spell .... only improves by about 1 sec, over ~400 pts
    }
    }%>%
    dplyr::group_by(.data$overlaps) %>%         # group by spells that overlap; no need to group by pseudo_nhs_id as already grouped by this
    {if(!all(is.na({{cols_to_combine}}))){
      tidyr::unite(.,                                     # concatenate the requested columns to a "," separated string
                   col = !!rlang::sym(combined_col_name), # to unpack the string need to first call as a symbol, then use !! to tell tidyverse to use the value inside; rlang::sym() same as {base} as.name();   see: https://stackoverflow.com/questions/65025731/why-does-bang-bang-combined-with-as-name-give-a-different-output-compared
                   {{cols_to_combine}},
                   sep = ",",
                   remove = remove_input_cols_flag,
                   na.rm = TRUE) %>%
        dplyr::mutate(.,
                      !!rlang::sym(combined_col_name) := list(   # !!unquotes the variable;  need to use := instead of = for dynamically assigning variable names;     rlang::sym() not technically needed here as on LHS of '='; could just do !!
                        unique(
                          unlist(
                            strsplit(
                              paste0(
                                trimws(!!rlang::sym(combined_col_name)),
                                collapse = ","),
                              ",")
                          )
                        )
                      )
        )
    } else .} %>%

    {if      (keep_spell == "longest")  dplyr::slice_max(., lubridate::int_length({{time_interval_col}}), with_ties = FALSE)
      else if (keep_spell == "shortest") dplyr::slice_min(., lubridate::int_length({{time_interval_col}}), with_ties = FALSE)
      # else if (keep == "earliest_start")
      # else if (keep == "latest_end")
    } %>%
    dplyr::ungroup() %>%
    dplyr::select(-.data$overlaps)

  return(time_interval_df)
}
