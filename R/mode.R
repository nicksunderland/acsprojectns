#' mode
#'
#' @param x vector to get mode of
#'
#' @return the mode
#' @export
#'
mode <- function(x) {
  ux   <- unique(x)[!is.na(unique(x))]
  r    <- ux[which.max(tabulate(match(x, ux)))]
  return(r)
}
