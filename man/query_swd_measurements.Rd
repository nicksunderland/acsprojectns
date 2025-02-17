% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/query_swd_measurements.R
\name{query_swd_measurements}
\alias{query_swd_measurements}
\title{query_swd_measurements}
\usage{
query_swd_measurements(
  swd_measurements_tbl,
  measurements,
  time_window,
  return_vals
)
}
\arguments{
\item{swd_measurements_tbl}{table of measurements}

\item{measurements}{which measurements to extract, a vector containing one or more of c('systolic_bp', 'diastolic_bp', 'cholesterol', 'hba1c', 'egfr')}

\item{time_window}{in what time window}

\item{return_vals}{what to return; one or more of c('count', 'value_date_min', 'value_date_max', 'date_min', 'date_max', 'mean', 'median', 'min', 'max')}
}
\value{
depends on the return value asked for
}
\description{
query_swd_measurements
}
