% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/get_hospital_admissions.R
\name{process_overlaps_of_time_intervals}
\alias{process_overlaps_of_time_intervals}
\title{process_overlaps_of_time_intervals}
\usage{
process_overlaps_of_time_intervals(
  time_interval_df,
  .y,
  time_interval_col,
  keep_spell = "longest",
  cols_to_combine = NA,
  combined_col_name = "combined_columns",
  remove_input_cols_flag = TRUE
)
}
\arguments{
\item{time_interval_df}{a dataframe with lubridate time interval data +/- other columns}

\item{.y}{the key, see notes for dplyr::group_modify}

\item{time_interval_col}{the column name of the column holding the time interval data}

\item{keep_spell}{c("longest", "shortest") how to process overlapping time intervals, keep longest or shortest}

\item{cols_to_combine}{when there are overlaps in the time intervals, which column should be combined (into a list)}

\item{combined_col_name}{the name of the column that will be returned containing the data in columns listed in col_to_combine}

\item{remove_input_cols_flag}{TRUE/FALSE whether or not to remove the original columns that have been combined}
}
\value{
a dataframe with overlapping time intervals removed +/- aggregation of cols_to_combine into a single column
}
\description{
process_overlaps_of_time_intervals
}
