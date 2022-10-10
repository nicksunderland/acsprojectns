#' Title query_icd10_codes
#'
#' @param codes A vector ICD-10 codes to search for e.g. c("I210", "I219")
#' @param search_strat_diag The diagnosis search strategy, one out of c("primary", "secondary", "all")
#' @param time_window The time window to search within - must be a lubridate::interval() object
#' @param return_vals A switch on what is being returned. If nothing found then NA is returned. Can be a vector
#' @param mortality_tbl The mortality table
#' @return Depends on the return_val parameter
#'
#' @importFrom lubridate %within%
#' @export
#'
query_mortality <- function(mortality_tbl, time_window, return_vals, codes = NA, search_strat_diag = "1abc"){

  # Input checks
  stopifnot(
    "The input mortality table column names do not match the expected input" =
      all(purrr::map_lgl(colnames(mortality_tbl), ~ .x %in% c("date_of_death", "death_1a_icd_codes", "death_1b_icd_codes", "death_1c_icd_codes", "death_2_icd_codes", "place_of_death", "place_of_death_ccg_code"))),
    "The ICD codes vector is not a character vector OR NA" =
      (is.character(codes) | all(is.na(codes))),
    "The search_strat_diag input must be one of: c('1a', '1b', '1c', '2', '1abc', 'all')" =
      search_strat_diag %in% c("1a", "1b", "1c", "1abc", "2", "all"),
    "The time_window input must be a lubridate::interval() object" =
      lubridate::is.interval(time_window),
    "The return values must be one or more of: c('date_of_death', 'death_1a_icd_codes', 'death_1b_icd_codes', 'death_1c_icd_codes', 'death_2_icd_codes', 'place_of_death', 'place_of_death_ccg_code')" =
      all(purrr::map_lgl(return_vals, ~ .x %in% c("date_of_death", "death_1a_icd_codes", "death_1b_icd_codes", "death_1c_icd_codes", "death_2_icd_codes", "place_of_death", "place_of_death_ccg_code")))
  )

  # Starting variables
  . = NULL
  na_return_lst = data.frame("date_of_death"           = as.POSIXct(NA),
                             "death_1a_icd_codes"      = I(list(NA_character_)),
                             "death_1b_icd_codes"      = I(list(NA_character_)),
                             "death_1c_icd_codes"      = I(list(NA_character_)),
                             "death_2_icd_codes"       = I(list(NA_character_)),
                             "place_of_death"          = NA_character_,
                             "place_of_death_ccg_code" = NA_character_)


  data <- mortality_tbl %>%
    dplyr::filter(lubridate::`%within%`(.data$date_of_death, time_window)) %>%
    {if(!all(is.na(codes)) & search_strat_diag == "all")
        dplyr::filter(., purrr::pmap_lgl(list(.data$death_1a_icd_codes,
                                              .data$death_1b_icd_codes,
                                              .data$death_1c_icd_codes,
                                              .data$death_2_icd_codes),
                                         function(a, b, c, d){
                                           any(c(a,b,c,d) %in% codes)
                                         }))
      else if(!all(is.na(codes)) & search_strat_diag == "1abc")
        dplyr::filter(., purrr::pmap_lgl(list(.data$death_1a_icd_codes,
                                              .data$death_1b_icd_codes,
                                              .data$death_1c_icd_codes),
                                         function(a, b, c){
                                           any(c(a,b,c) %in% codes)
                                         }))
      else if(!all(is.na(codes)) & search_strat_diag == "1a")
        dplyr::filter(., purrr::map_lgl(.data$death_1a_icd_codes, ~ any(.x %in% codes)))

      else if(!all(is.na(codes)) & search_strat_diag == "1b")
        dplyr::filter(., purrr::map_lgl(.data$death_1b_icd_codes, ~ any(.x %in% codes)))

      else if(!all(is.na(codes)) & search_strat_diag == "1c")
        dplyr::filter(., purrr::map_lgl(.data$death_1c_icd_codes, ~ any(.x %in% codes)))

      else if(!all(is.na(codes)) & search_strat_diag == "2")
        dplyr::filter(., purrr::map_lgl(.data$death_2_icd_codes, ~ any(.x %in% codes)))

      else .} %>%
    dplyr::slice_min(.data$date_of_death)


  if(nrow(data) == 1){
    return(data[return_vals])
  }else if(nrow(data) == 0){
    return(na_return_lst[return_vals])
  }else{
     stop("Something went wrong in query mortality")
  }
}
