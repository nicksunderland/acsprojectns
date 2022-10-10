#' query_swd_attributes
#' @param swd_attributes_tbl the table of swd attributes info
#' @param search_strat_date either min or max
#' @param time_window in what time wiondow
#' @param return_vals what to return; one or more of c(" ", " ", "...")
#'
#' @importFrom lubridate %within%
#'
#' @return depends on the return value asked for
#' @export
#'
query_swd_attributes = function(swd_attributes_tbl, time_window, search_strat_date, return_vals){


  # Return the requested "return_val" type, or an appropriately typed NA
  return_lst <- list()
  for(i in 1:length(return_vals)){
    return_lst[[return_vals[i]]] <- NA
  }
  if(is.null(swd_attributes_tbl)){
    return(return_lst[return_vals])
  }

  # Input checks
  stopifnot(
    search_strat_date %in% c("min", "max"), # add and "average" function too
    lubridate::is.interval(time_window),
    all(return_vals %in% colnames(swd_attributes_tbl))  # need to check if table NULL first as we access the table here
  )

  # Starting variables
  . = NULL

  if(!is.null(swd_attributes_tbl)){

    data <- swd_attributes_tbl %>%
      dplyr::filter(.data$attribute_period_date %within% time_window) %>%
      {if(search_strat_date == "min")  dplyr::slice_min(., .data$attribute_period_date, with_ties = FALSE) else
        if(search_strat_date == "max") dplyr::slice_max(., .data$attribute_period_date, with_ties = FALSE)}

  }

  # Return value
  if(nrow(data) == 0){
    return(return_lst[return_vals])
  }else{
    return(data[return_vals])
  }
}
