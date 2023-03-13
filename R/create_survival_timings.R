#' create_survival_timings
#'
#' @param data the events data frame with columns c("nhs_number", "event_desc", "index_start", "event_start", "event_end", "last_fu")
#' @param outcome_groupings this must be NA, character vector, or a named list of character vectors"
#'
#' @return the timings data frame for Cox PH modelling
#' @export
#'
create_survival_timings <- function(data, outcome_groupings=NA) {

  # ensure correct column names
  req_cols <- c("nhs_number", "event_desc", "index_start", "event_start", "event_end", "last_fu")
  stopifnot(all(req_cols %in% colnames(data)))
  data <- data |> dplyr::select(dplyr::all_of(req_cols))

  # generate lookup table - `values` are the event_desc and `ind` are the new event str/factor
  if(all(is.na(outcome_groupings))) {
    outcome_groupings = data.frame("values" = unique(data$event_desc),
                                   "ind" = as.factor(unique(data$event_desc)))
  }else if(is.character(outcome_groupings)) {
    outcome_groupings = data.frame("values" = outcome_groupings,
                                   "ind" = as.factor(outcome_groupings))
  }else if(is.list(outcome_groupings) & !is.null(names(outcome_groupings))) {
    outcome_groupings = utils::stack(outcome_groupings)
  }else {
    stop("error parsing `outcome_groupings`; this must be NA, character vector, or a named list of character vectors")
  }
  # rename to make more explicit
  outcome_groupings <- outcome_groupings |> dplyr::rename("event_desc_map" = "values", "outcome" = "ind")

  # set up the start and stop times and event status for survival
  foo=data %>%
    # recode the event column by the groupings list - fuzzy allows use of regex
    fuzzyjoin::regex_left_join(outcome_groupings, by=c("event_desc"="event_desc_map")) |>
    # no longer need event_desc
    dplyr::select(-event_desc) |>
    # also the new groups may have created same group happening twice in the same admission (because
    # they used to be distinct event_desc e.g. NSTEMI & STEMI --> now just ACS), ensure just one
    # of these is kept
    dplyr::group_by(nhs_number, event_start, event_end, outcome) |>
    dplyr::slice_head(n=1L) |>
    dplyr::ungroup() |>
    # complete the data.frame so each nhs_number has at least one of each event outcome
    tidyr::complete(outcome, tidyr::nesting(nhs_number, index_start)) |>
    # if the outcome is not specified in the fuzzy join above it will be NA, assume we don't want
    # these; need to do this filter after running complete, so that each nhs number gets an outcome
    dplyr::filter(!is.na(outcome)) |>
    # event status
    dplyr::mutate(status = !is.na(event_start)) |>
    # each outcome needs to have a line representing no event happening at the final interval - i.e.
    # a status=='FALSE'. If there is no event at all for the patient, then this will just be a
    # single row. If there are event(s), then the final row will be a 'FALSE' unless the previous
    # event ends at the same time as follow up ends (e.g. a death)
    dplyr::group_by(nhs_number, outcome) |>
    dplyr::filter(!(dplyr::n()>1 & !status)) %>%
    # add back in an extra row for the end interval, up until max follow up time
    {
      #TODO - currently adding a FALSE when there is already a FALSE
      { . -> tmp } |>
        # take the events
        dplyr::filter(status==TRUE) |>
        dplyr::group_by(nhs_number, outcome) |>
        # take the last event
        dplyr::slice_max(event_start, with_ties=FALSE) |>
        # if the last event ends on the last follow up, don't create a non-event period after this
        dplyr::filter(event_end < last_fu) |>
        # overwrite with a non-event end period, status is FALSE as this is a non-event period
        dplyr::mutate(event_start=as.Date(NA), event_end=as.Date(NA), status=FALSE) |>
        # add these period back into the main data
        dplyr::bind_rows(tmp)
    } |>
    # do the recurrent events within event types
    dplyr::group_by(nhs_number, outcome) |>
    # earliest event first - needs to be event_end.
    dplyr::arrange(event_end, .by_group=TRUE) |>
    # the start time is 0 if at the beginning; or the time from index event to the end of the previous
    # event. i.e. leaving a gap of time that the person is not at risk whilst they are an inpatient.
    dplyr::mutate(tstart = lubridate::time_length(lubridate::interval(index_start, dplyr::lag(event_end, default=index_start[[1]])), unit="days"),
                  tstop  = dplyr::if_else(status,
                                          lubridate::time_length(lubridate::interval(index_start, event_start), unit="days"),
                                          lubridate::time_length(lubridate::interval(index_start, Sys.Date()), unit="days")),
                  event  = 1:dplyr::n(),
                  len = lubridate::time_length(lubridate::interval(event_start, event_end), unit="days")) |>
    # always ungroup
    dplyr::ungroup() |>
    # tidy up
    dplyr::select(nhs_number, tstart, tstop, outcome, status, event)
}
