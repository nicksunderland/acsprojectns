#' get_codes
#'
#' @param file_path the file path to the csv file containing the icd-10 codes.
#' @param append_character a character to append to the ICD-10 codes; e.g. append_character = "-" with give "I200" and "I200-"
#' This is useful becuase a small subset of SUS codes have other characters appended
#'
#' @return returns a data frame with the icd-10 codes, or the ICD-10 codes plus the same codes with an appended character
#' @export
#'
get_codes <- function(file_path, append_character = NA_character_){

  codes <- readr::read_csv(file_path, show_col_types = FALSE)

  if(is.na(append_character)){
    return(codes)
  }else{
    adjusted_codes <- codes %>%
      dplyr::mutate(row_number = dplyr::row_number(),
                    icd_codes = paste0(.data$icd_codes, append_character)) %>%
      dplyr::bind_rows(codes %>% dplyr::mutate(row_number = dplyr::row_number())) %>%
      dplyr::arrange(.data$row_number) %>%
      dplyr::select(-.data$row_number)
    return(adjusted_codes)
  }

}
