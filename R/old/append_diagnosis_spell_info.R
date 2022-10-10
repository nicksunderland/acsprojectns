#' #' append_diagnosis_spell_info
#' #'
#' #' @param A the data.frame to append to
#' #' @param A_id_col_name the column in 'A' containing the IDs to join on
#' #' @param A_spell_interval_col_name the column in 'A' containing the index spell intervals (i.e. lubridate::interval objects)
#' #' @param B the spell information to process and append - must be a data.frame(col1=c(IDs), col2=c(lubridate::interval spell_intervals))
#' #' @param appended_col_name a string... what to call the returned column
#' #' @param processing one of c("first_subsequent", "last_subsequent", "number_subsequent", "most_recent_historical", "most_distant_historical", "number_historical")
#' #' @param follow_up_time_window a lubridate::duration object;  must be NEGATIVE if looking backwards
#' #'
#' #' @return A, when one appended column as per the processing choices
#' #'
#' #' @importFrom magrittr "%>%"
#' #' @importFrom rlang .data
#' #' @importFrom rlang :=
#' #' @export
#' #'
#' append_diagnosis_spell_info <- function(A,
#'                               A_id_col_name,
#'                               A_spell_interval_col_name,
#'                               B,
#'                               appended_col_name,
#'                               processing = "first_subsequent",
#'                               follow_up_time_window = lubridate::duration(Inf, units = "months")
#'                               ){
#'
#'   stopifnot(ncol(B) == 2,
#'             lubridate::is.duration(follow_up_time_window))
#'   #TODO add in more checks on the inputs
#'
#'   . = NULL
#'
#'   # Rename the spell_interval column of B (user chosen depending on processing option and input data)
#'   colnames(B)[2] <- appended_col_name
#'
#'   # Join the index event spells (A) to the subsequent event spells (B/appended)
#'   A <- dplyr::left_join(A, B, by = {{A_id_col_name}}) %>%
#'     {if(processing %in% c("first_subsequent", "last_subsequent", "number_subsequent")){
#'        # Ensure that the subsequent event (appended_col_name) spell is after the end of the index spell (A/A_spell_interval_col_name) and before the end of the follow_up_time_window
#'        # Over-write with NA (actually lubridate::interval(NA,NA)...) if outside of this window; else just return the spell interval unchanged
#'        dplyr::mutate(., !!rlang::sym(appended_col_name) := dplyr::case_when((lubridate::int_start(!!rlang::sym(appended_col_name)) < lubridate::int_end(!!rlang::sym(A_spell_interval_col_name))) |
#'                                                                             (lubridate::int_start(!!rlang::sym(appended_col_name)) > (lubridate::int_end(!!rlang::sym(A_spell_interval_col_name)) + follow_up_time_window)) |
#'                                                                             (is.na(!!rlang::sym(appended_col_name)))      ~ lubridate::interval(NA,NA),
#'                                                                             TRUE                                          ~ !!rlang::sym(appended_col_name))) %>%
#'        # Group by everything except the appended spell interval, then slice off the first/last subsequent spell, or count the number of subsequent spells - depending on the processing flag
#'        dplyr::group_by(dplyr::across(-!!rlang::sym(appended_col_name))) %>%
#'         {if(processing == "first_subsequent") {
#'            dplyr::filter(., rank(!!rlang::sym(appended_col_name), na.last = TRUE, ties.method = "first") == 1)
#'          }else if(processing == "last_subsequent"){
#'            dplyr::filter(., rank(!!rlang::sym(appended_col_name), na.last = FALSE, ties.method = "last") == max(rank(!!rlang::sym(appended_col_name))))
#'          }else if(processing == "number_subsequent"){
#'            dplyr::mutate(., !!rlang::sym(appended_col_name) := sum(!is.na(!!rlang::sym(appended_col_name)))) %>%
#'            dplyr::distinct(.,)
#'          }else{
#'            dplyr::mutate(., !!rlang::sym(appended_col_name) := "error" ) %>%
#'            dplyr::distinct(.,)
#'          }
#'         } %>%
#'        dplyr::ungroup()
#'
#'     } else if(processing %in% c("most_recent_historical", "most_distant_historical", "number_historical")) {
#'
#'       # see comments above, essentially the same thing but for spells that occurred BEFORE the index event, i.e. historical spells
#'       dplyr::mutate(., !!rlang::sym(appended_col_name) := dplyr::case_when((lubridate::int_end(!!rlang::sym(appended_col_name)) > lubridate::int_start(!!rlang::sym(A_spell_interval_col_name))) |
#'                                                                            (lubridate::int_end(!!rlang::sym(appended_col_name)) < (lubridate::int_start(!!rlang::sym(A_spell_interval_col_name)) + follow_up_time_window)) |
#'                                                                            (is.na(!!rlang::sym(appended_col_name)))      ~ lubridate::interval(NA,NA),
#'                                                                            TRUE                                          ~ !!rlang::sym(appended_col_name))) %>%
#'         dplyr::group_by(dplyr::across(-!!rlang::sym(appended_col_name))) %>%
#'         {if(processing == "most_distant_historical") {
#'           dplyr::filter(., rank(!!rlang::sym(appended_col_name), na.last = TRUE, ties.method = "first") == 1)
#'         }else if(processing == "most_recent_historical"){
#'           dplyr::filter(., rank(!!rlang::sym(appended_col_name), na.last = FALSE, ties.method = "last") == max(rank(!!rlang::sym(appended_col_name))))
#'         }else if(processing == "number_historical"){
#'           dplyr::mutate(., !!rlang::sym(appended_col_name) := sum(!is.na(!!rlang::sym(appended_col_name)))) %>%
#'             dplyr::distinct(.,)
#'         }else{
#'           dplyr::mutate(., !!rlang::sym(appended_col_name) := "error" ) %>%
#'             dplyr::distinct(.,)
#'         }
#'         } %>%
#'         dplyr::ungroup()
#'
#'
#'     } else .}
#'
#'
#'
#'   return(A)
#' }
