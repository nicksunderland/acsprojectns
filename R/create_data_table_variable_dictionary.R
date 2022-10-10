#' create_data_table_variable_dictionary
#'
#' @param table_name string name of the table to work with
#'
#' @return a named list of standardised variable names mapped to db names
#'
#' @examples create_data_table_variable_dictionary("vw_APC_SEM_001")
#'
#' @importFrom magrittr "%>%"
#' @export
#'
create_data_table_variable_dictionary <- function(table_name){

  # Read in the variable name mapping matrix
  var_mapping_matrix <- readr::read_csv(system.file("data_table_variable_mapping.csv",
                                                    package = "acsprojectns"),
                                        show_col_types = FALSE)

  # Take the columns corresponding to the standardised var names and the underlying data table var names
  var_mapping_matrix <- stats::na.omit(var_mapping_matrix[c("standardised_var_name", table_name)])
  var_dict <- tibble::deframe(var_mapping_matrix)

  return(var_dict)
}
