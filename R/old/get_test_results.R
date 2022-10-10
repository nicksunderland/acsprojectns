#' get_test_results
#'
#' @param result_type_code_list list of test codes to filter on
#' @param datetime_window the beginning and end of the date window (inclusive), must be a lubridate::interval object e.g. lubridate::interval({start}, {end}, tzone = "GMT")
#' @param id_list list of patient hospital ids to search on
#' @param verbose where to print the SQL query
#'
#' @return test result dataframe
#' @export
#'
get_test_results <- function(result_type_code_list = c("all"),
                             datetime_window = lubridate::interval(as.POSIXct("1900-01-01 00:00:00", tz = "GMT"), lubridate::now(tzone = "GMT")),
                             id_list = NA,
                             verbose = FALSE){
  # Input checks
  stopifnot((lubridate::is.interval(datetime_window) & !is.na(datetime_window) & length(datetime_window) == 1),
            is.vector(result_type_code_list),
            all(result_type_code_list %in% c("all", "HB", "PLT")))

  #TODO: move the coding of the test results / codes into the mapping dictionary

  # Starting variables
  . = NULL
  db_struct              <- create_db_struct()
  on.exit( disconnect_db_struct(db_struct) )  # Close all the connections in the db_struct
  data_ref               <- db_struct$hosp_bloods
  data_table_name        <- data_ref$data_table_ref$ops$x[1]
  var_dict <- create_data_table_variable_dictionary(data_table_name)            #create the named list to convert to standardised naming
  base_vars <- c(pseudo_nhs_id     = var_dict[["pseudo_nhs_id"]],              #grab the basic variables we'll need
                 test_datetime     = var_dict[["test_datetime"]],                #the column containing the 'primary_care_prescription' flag
                 test_result_code  = var_dict[["test_result_code"]],           #the column containing ????the number of tablets given????
                 test_result_name  = var_dict[["test_result_name"]],           #the column containing the description of the drug prescribed
                 test_result_value = var_dict[["test_result_value"]],               #the column containing whether 'repeat' or 'acute' prescription
                 test_result_units = var_dict[["test_result_units"]])             #the column containing the cost of the medication

  # Generate the sql query and collect from the database
  data <- data_ref$data_table_ref %>%  #a reference to the SQL table
    dplyr::select(dplyr::all_of(base_vars)) %>%
    dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                  .data$test_datetime >= lubridate::int_start(datetime_window) &
                  .data$test_datetime <= lubridate::int_end(datetime_window)) %>%
    {if(!result_type_code_list[1] == "all") dplyr::filter(., .data$test_result_code %in% result_type_code_list) else .} %>%
    {if(!all(is.na(id_list)))               dplyr::filter(., .data$pseudo_nhs_id %in% id_list) else .} %>%
    {if(verbose) dplyr::show_query(.,) else .} %>%
    dplyr::collect() %>%                                                        #pull all the data to local
    dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id))

  # Do some exit checks on the data
  if(!all(is.numeric(data$test_result_value))) warning("Test result value column contains non-numeric data")

  return(data)
}
