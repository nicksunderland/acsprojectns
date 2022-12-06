#' gen_msoa_geo_data
#'
#' @param msoa msoa codes
#' @param values values to plot (numerical)
#' @param dat an optional data.frame with at least the col names 'msoa' and 'values'
#'
#' @return a geo dataframe ready for ploting
#' @export
#'
gen_msoa_geo_data <- function(msoa, values, dat=NULL){

  # Checks
  if(!is.null(dat))
  {
    msoa   = dat[,"msoa"]
    values = dat[,"values"]
  }
  stopifnot(length(msoa)==length(values))

  # Get the paths of the map geometries
  geo_filepaths <- list.files(path = system.file("geojson_files/bnssg", package="acsprojectns"), full.names = T)

  # Combine the geometries
  geo_plot_data <- data.frame()
  for (i in 1:length(geo_filepaths)){
    if(stringr::str_detect(geo_filepaths[i], "msoa")){
      geojson_data <- geojsonio::geojson_read(geo_filepaths[i], what = 'sp')
      tidy_df = broom::tidy(geojson_data, region="MSOA11NM")
      geo_plot_data     <- rbind(geo_plot_data, tidy_df)
    }
  }

  # Merge the values with the geo data
  geo_plot_data <- geo_plot_data %>%
    dplyr::rename(msoa = id) %>%
    dplyr::left_join(data.frame(msoa=msoa, values=values), by="msoa")

  return(geo_plot_data)
}
