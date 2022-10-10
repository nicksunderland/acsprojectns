#' get_gp_attributes
#'
#' @param gp_att_obj the Database object for the GP attributes database and table
#' @param id_list list of IDS
#' @param cols_to_extract THE COLUMN NAMES TO EXTRACT
#'
#' @return a data frame
#' @export
#'
get_gp_attributes <- function(gp_att_obj,
                              id_list = NA,
                              cols_to_extract = NA){

  # Input checks
  stopifnot((is.vector(id_list) | is.na(id_list)))

  # Starting variables
  . = NULL
  var_dict               <- create_data_table_variable_dictionary(gp_att_obj$table_name)           #create the named list to convert to standardised naming
  if(!all(is.na(cols_to_extract))){
    base_vars              <- c(pseudo_nhs_id = var_dict[["pseudo_nhs_id"]])
    attributes_vars        <- purrr::map(cols_to_extract, function(x){ var_dict[[x]] })
    names(attributes_vars) <- cols_to_extract
    all_vars               <- unlist(c(base_vars, attributes_vars))
  }else{  # if not specified return all the columns in the variable mapping dictionary
    all_vars               <- var_dict
  }

  # Auto-create the SQL query and execute
  data <- gp_att_obj$data %>%  #a reference to the SQL table
    dplyr::select(dplyr::all_of(all_vars)) %>% # if columns specified get them, if not return all of the columns see above
    dplyr::distinct() %>%
    {if(!all(is.na(id_list))) dplyr::filter(., .data$pseudo_nhs_id %in% id_list) else .} %>% #if a list of ids are provided then filter for these
    dplyr::collect() %>%                                                        #pull all the data to local
    dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id))

  # Return the data frame
  return(data)
}
