#' get_mortality
#'
#' @param id_list  vector list of character pseudo NHS ids to filter on
#' @param datetime_window the beginning and end of the date window (inclusive), must be a lubridate::interval object e.g. lubridate::interval({start}, {end}, tzone = "GMT")
#' @param verbose TRUE = print SQL query to console
#'
#' @return the mortality dataframe
#'
#' @importFrom magrittr "%>%"
#' @export
#'
get_mortality <- function(id_list = NA,
                          datetime_window = lubridate::interval(NA, NA, tzone = "GMT"),
                          verbose = FALSE){

  # Input checks
  stopifnot(!all(is.na(id_list)),         # either need diagnosis codes OR ids to search for
            (lubridate::is.interval(datetime_window) & !is.na(datetime_window) & length(datetime_window) == 1),                                # must be a valid interval object
            is.logical(verbose))

  # Starting variables
  . = NULL
  db_struct       <- create_db_struct()
  on.exit( disconnect_db_struct(db_struct) )  # Close all the connections in the db_struct
  data_ref        <- db_struct$uk_mortality
  data_table_name <- data_ref$data_table_ref$ops$x[1]
  var_dict        <- create_data_table_variable_dictionary(data_table_name)            #create the named list to convert to standardised naming
  base_vars       <- c(pseudo_nhs_id = var_dict[["pseudo_nhs_id"]],            #grab the basic variables we'll need
                       date_of_death = var_dict[["date_of_death"]])

  # Generate the sql query and collect from the database
  data <- data_ref$data_table_ref %>%  #a reference to the SQL table
    dplyr::select(dplyr::all_of(base_vars)) %>%
    dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                  .data$date_of_death <= lubridate::int_end(datetime_window) &
                  .data$date_of_death >= lubridate::int_start(datetime_window)) %>%
    {if(!all(is.na(id_list))) dplyr::filter(., .data$pseudo_nhs_id %in% id_list) else .} %>%
    {if(verbose) dplyr::show_query(.,) else .} %>%
    dplyr::collect() %>%                                                        #pull all the data to local
    dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id))


  return(data)
}
