#' get_prescriptions
#'
#' @param medication_list string vector or list of medication names to look for, or NA
#' @param datetime_window the beginning and end of the date window (inclusive), must be a lubridate::interval object e.g. lubridate::interval({start}, {end}, tzone = "GMT")
#' @param id_list vector or list of character pseudo NHS ids to filter on
#' @param verbose TRUE = print SQL query to console
#'
#' @return a data frame
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#' @export
#'
get_prescriptions <- function(medication_list = NA,
                              datetime_window = lubridate::interval(as.POSIXct("1900-01-01 00:00:00", tz = "GMT"), lubridate::now(tzone = "GMT")),
                              id_list = NA,
                              verbose = FALSE){


  # Input checks
  stopifnot((is.vector(id_list) | is.na(id_list)),
            (lubridate::is.interval(datetime_window) & !is.na(datetime_window) & length(datetime_window) == 1))

  # Starting variables
  . = NULL
  db_struct              <- create_db_struct()
  on.exit( disconnect_db_struct(db_struct) )  # Close all the connections in the db_struct
  data_ref               <- db_struct$gp_activity
  data_table_name        <- data_ref$data_table_ref$ops$x[1]
  var_dict <- create_data_table_variable_dictionary(data_table_name)            #create the named list to convert to standardised naming
  base_vars <- c(pseudo_nhs_id      = var_dict[["pseudo_nhs_id"]],              #grab the basic variables we'll need
                 episode_start_date = var_dict[["episode_start_date"]],         #datetime_window looking at prescriptions start_date==end_date
                 episode_end_date   = var_dict[["episode_end_date"]],
                 pod_level_1        = var_dict[["pod_level_1"]],                #the column containing the 'primary_care_prescription' flag
                 details_level_1a   = var_dict[["details_level_1a"]],           #the column containing ????the number of tablets given????
                 details_level_1b   = var_dict[["details_level_1b"]],           #the column containing the description of the drug prescribed
                 pod_level_2a       = var_dict[["pod_level_2a"]],               #the column containing whether 'repeat' or 'acute' prescription
                 episode_cost_1     = var_dict[["episode_cost_1"]])             #the column containing the cost of the medication

  #medication regex filters for sql and for processing
  sql_med_filter_string <- paste("(", paste(lapply(medication_list, function(x) sprintf("\"details_level_1b\" LIKE '%%%s%%'", x)), collapse=' OR '), ")", collapse='')
  med_filter_string <- paste0("(?i)(", paste(medication_list, collapse='|'),")[A-z ]* ([0-9.]+[A-z]+)")

  #generate the sql query and collect from the database
  data <- data_ref$data_table_ref %>%  #a reference to the SQL table
    dplyr::select(dplyr::all_of(base_vars)) %>%
    dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                  .data$episode_start_date >= lubridate::int_start(datetime_window) &
                  .data$episode_end_date <= lubridate::int_end(datetime_window) &
                  .data$pod_level_1 == 'primary_care_prescription' &
                  dplyr::sql(sql_med_filter_string)) %>%   # doesn't work at home........
    {if(!all(is.na(id_list))) dplyr::filter(., .data$pseudo_nhs_id %in% id_list) else .} %>% #if a list of ids are provided then filter for these
    {if(verbose) dplyr::show_query(.,) else .} %>%
    dplyr::collect() %>%                                                        #pull all the data to local
    dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id))


  #Do some processing
  data <- data %>%
    dplyr::mutate(episode_start_datetime = lubridate::ymd_hms(paste(.data$episode_start_date, "00:00:00"), tz="GMT"),  # there is no time data for prescriptions
                  episode_start_datetime = lubridate::ymd_hms(paste(.data$episode_end_date,   "00:00:00"), tz="GMT"),
                  spell_interval         = lubridate::interval(.data$episode_start_datetime, .data$episode_start_datetime),
                  medication_name        = stringr::str_match(.data$details_level_1b, pattern = med_filter_string)[,2], #capture group1
                  medication_dose        = stringr::str_match(.data$details_level_1b, pattern = med_filter_string)[,3], #capture group2
                  medication_amount      = .data$details_level_1a,
                  prescription_type      = .data$pod_level_2a) %>%
    dplyr::select(.data$pseudo_nhs_id,
                  .data$spell_interval,
                  .data$medication_name,
                  .data$medication_dose,
                  .data$medication_amount,
                  .data$prescription_type)


  return(data)
}
