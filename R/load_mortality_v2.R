#' load_mortality
#'
#' @param db_conn_struct list of Database objects
#' @param patient_ids list of Patient objects
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return modifies the patient objects in-place
#'
#' @export
#'
load_mortality_v2 <- function(db_conn_struct,
                              patient_ids){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Checks and adjustments
  stopifnot(all(purrr::map_lgl(db_conn_struct, ~ checkmate::test_class(.x, "Database"))),
            all(purrr::map_lgl(patient_ids,    ~ is.double(.x))))

  # Starting variables
  . = NULL

  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$mortality$is_connected){

    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$mortality$table_name)    #create the named list to convert to standardised naming
    base_vars       <- c(pseudo_nhs_id = var_dict[["pseudo_nhs_id"]],                                #grab the basic variables we'll need
                         date_of_death = var_dict[["date_of_death"]])

    # Generate the sql query and collect from the database
    mortality_1_data <- db_conn_struct$mortality$data %>%  #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                    .data$pseudo_nhs_id %in% patient_ids) %>%
      dplyr::collect() %>%                                                        #pull all the data to local
      dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id),
                    date_of_death = as.POSIXct(.data$date_of_death, tz = "GMT")) %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::slice_min(.data$date_of_death, with_ties = FALSE) %>%
      dplyr::ungroup() %>%
      dplyr::distinct(.data$pseudo_nhs_id,
                      .data$date_of_death)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    mortality_1_data <- dplyr::left_join(ids, mortality_1_data, by = "pseudo_nhs_id")

  }

  # Look in the civil registry database too
  if(db_conn_struct$mortality_2$is_connected){

    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$mortality_2$table_name)    #create the named list to convert to standardised naming
    base_vars       <- c(pseudo_nhs_id           = var_dict[["pseudo_nhs_id"]],                        #grab the basic variables we'll need
                         date_of_death           = var_dict[["date_of_death"]],
                         place_of_death          = var_dict[["place_of_death"]],
                         place_of_death_ccg_code = var_dict[["place_of_death_ccg_code"]])

    death_icd_code_cols           <- purrr::map(1:15, ~ var_dict[[sprintf("death_icd_code_%d",.x)]])
    death_icd_std_var_names       <- purrr::map(1:15, ~           sprintf("death_icd_code_%d",.x)  )
    names(death_icd_code_cols)    <- death_icd_std_var_names
    death_icd_code_cols           <- unlist(death_icd_code_cols)

    death_code_line_cols          <- purrr::map(1:15, ~ var_dict[[sprintf("death_icd_code_%d_line",.x)]])
    death_code_line_std_var_names <- purrr::map(1:15, ~           sprintf("death_icd_code_%d_line",.x)  )
    names(death_code_line_cols)   <- death_code_line_std_var_names
    death_code_line_cols          <- unlist(death_code_line_cols)

    all_vars  <- c(base_vars, death_icd_code_cols, death_code_line_cols)

    # Generate the sql query and collect from the database
    mortality_2_data <- db_conn_struct$mortality_2$data %>%  #a reference to the SQL table
      dplyr::select(dplyr::all_of(all_vars)) %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                    !is.na(.data$date_of_death) &
                    .data$pseudo_nhs_id %in% patient_ids) %>%
      dplyr::collect() %>%                                                        #pull all the data to local
      dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id),
                    date_of_death = as.POSIXct(.data$date_of_death, tz = "GMT")) %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::slice_min(.data$date_of_death, with_ties = FALSE) %>%
      dplyr::ungroup() %>%

      tidyr::unite(.,                                     # concatenate the requested columns to a "," separated string
                   col = "death_icd_codes",
                   names(death_icd_code_cols),
                   sep = ",",
                   remove = TRUE,
                   na.rm = FALSE) %>%
      tidyr::unite(.,                                     # concatenate the requested columns to a "," separated string
                   col = "death_icd_code_lines",
                   names(death_code_line_cols),
                   sep = ",",
                   remove = TRUE,
                   na.rm = FALSE) %>%

      dplyr::mutate(death_1a_icd_codes = purrr::map2(.x = .data$death_icd_codes,
                                                     .y = .data$death_icd_code_lines,
                                                     .f = function(x, y){
                                                       code_vec = purrr::map_chr(unlist(strsplit(x, ",")), function(z1) gsub("[[:punct:] ]+","", z1))
                                                       line_vec = purrr::map_chr(unlist(strsplit(y, ",")), function(z2) gsub("[[:punct:] ]+","", z2))
                                                       return(code_vec[line_vec == "a"])
                                                    }),
                    death_1a_icd_codes = purrr::map(.data$death_1a_icd_codes, function(x) if(identical(x, character(0))) NA_character_ else x),
                    death_1b_icd_codes = purrr::map2(.x = .data$death_icd_codes,
                                                     .y = .data$death_icd_code_lines,
                                                     .f = function(x, y){
                                                       code_vec = purrr::map_chr(unlist(strsplit(x, ",")), function(z1) gsub("[[:punct:] ]+","", z1))
                                                       line_vec = purrr::map_chr(unlist(strsplit(y, ",")), function(z2) gsub("[[:punct:] ]+","", z2))
                                                       return(code_vec[line_vec == "b"])
                                                     }),
                    death_1b_icd_codes = purrr::map(.data$death_1b_icd_codes, function(x) if(identical(x, character(0))) NA_character_ else x),
                    death_1c_icd_codes = purrr::map2(.x = .data$death_icd_codes,
                                                     .y = .data$death_icd_code_lines,
                                                     .f = function(x, y){
                                                       code_vec = purrr::map_chr(unlist(strsplit(x, ",")), function(z1) gsub("[[:punct:] ]+","", z1))
                                                       line_vec = purrr::map_chr(unlist(strsplit(y, ",")), function(z2) gsub("[[:punct:] ]+","", z2))
                                                       return(code_vec[line_vec == "c"])
                                                     }),
                    death_1c_icd_codes = purrr::map(.data$death_1c_icd_codes, function(x) if(identical(x, character(0))) NA_character_ else x),
                    death_2_icd_codes  = purrr::map2(.x = .data$death_icd_codes,
                                                     .y = .data$death_icd_code_lines,
                                                     .f = function(x, y){
                                                       code_vec = purrr::map_chr(unlist(strsplit(x, ",")), function(z1) gsub("[[:punct:] ]+","", z1))
                                                       line_vec = purrr::map_chr(unlist(strsplit(y, ",")), function(z2) gsub("[[:punct:] ]+","", z2))
                                                       return(code_vec[line_vec == "d"])
                                                     }),
                    death_2_icd_codes  = purrr::map(.data$death_2_icd_codes, function(x) if(identical(x, character(0))) NA_character_ else x)) %>%

      dplyr::distinct(.data$pseudo_nhs_id,
                      .data$date_of_death,
                      .data$place_of_death,
                      .data$place_of_death_ccg_code,
                      .data$death_1a_icd_codes,
                      .data$death_1b_icd_codes,
                      .data$death_1c_icd_codes,
                      .data$death_2_icd_codes)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    mortality_2_data <- dplyr::left_join(ids, mortality_2_data, by = "pseudo_nhs_id")

    # only way I could make replacing these work.....
    mortality_2_data$death_1a_icd_codes[purrr::map_lgl(mortality_2_data$death_1a_icd_codes, ~ is.null(.x))] <- NA_character_
    mortality_2_data$death_1b_icd_codes[purrr::map_lgl(mortality_2_data$death_1b_icd_codes, ~ is.null(.x))] <- NA_character_
    mortality_2_data$death_1c_icd_codes[purrr::map_lgl(mortality_2_data$death_1c_icd_codes, ~ is.null(.x))] <- NA_character_
    mortality_2_data$death_2_icd_codes [purrr::map_lgl(mortality_2_data$death_2_icd_codes,  ~ is.null(.x))] <- NA_character_

  }

  # Join the data frames on the ID
  data <- purrr::reduce(list(ids, mortality_1_data, mortality_2_data), dplyr::left_join, by = "pseudo_nhs_id") %>%
    dplyr::mutate(date_of_death = dplyr::coalesce(.data$date_of_death.x, .data$date_of_death.y)) %>%
    dplyr::select(-c(.data$date_of_death.x, .data$date_of_death.y)) %>%
    dplyr::relocate(.data$date_of_death, .after = .data$pseudo_nhs_id) %>%
    dplyr::group_nest(.data$pseudo_nhs_id, .key = "mortality")


  # Stop the clock
  message(paste("Time taken to execute load_mortality() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  return(data)
}
