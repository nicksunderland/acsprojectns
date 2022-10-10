#' load_swd_attributes_v2
#'
#' @param db_conn_struct list of Database objects
#' @param patient_ids list of patient ids
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return a df to append to the ids
#' @export
#'
load_swd_attributes_v2 <- function(db_conn_struct,
                                   patient_ids){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Checks and adjustments
  stopifnot(all(purrr::map_lgl(db_conn_struct, ~ checkmate::test_class(.x, "Database"))),
            all(purrr::map_lgl(patient_ids,    ~ is.double(.x))))

  # Starting variables
  . = NULL

  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$swd_att_hist$is_connected){

    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$swd_att_hist$table_name)       #create the named list to convert to standardised naming
    base_vars       <- c(pseudo_nhs_id                                 = var_dict[["pseudo_nhs_id"]],                               #grab the basic variables we'll need
                         attribute_period_date                         = var_dict[["attribute_period_date"]],
                         smoking_status                                = var_dict[["smoking_status"]],
                         bmi                                           = var_dict[["bmi"]],
                         obesity                                       = var_dict[["obesity"]],
                         obesity_qof                                   = var_dict[["obesity_qof"]],
                         electronic_frailty_index                      = var_dict[["electronic_frailty_index"]],
                         dna_cpr                                       = var_dict[["dna_cpr"]],
                         hypertension                                  = var_dict[["hypertension"]],
                         hypertension_qof                              = var_dict[["hypertension_qof"]],
                         chronic_kidney_disease                        = var_dict[["chronic_kidney_disease"]],
                         chronic_kidney_disease_qof                    = var_dict[["chronic_kidney_disease_qof"]],
                         anaemia_other                                 = var_dict[["anaemia_other"]],
                         anaemia_iron_deficiency                       = var_dict[["anaemia_iron_deficiency"]],
                         coagulopathy                                  = var_dict[["coagulopathy"]],
                         stroke                                        = var_dict[["stroke"]],
                         stroke_qof                                    = var_dict[["stroke_qof"]],
                         alcoholic_liver_disease                       = var_dict[["alcoholic_liver_disease"]],
                         non_alcoholic_fatty_liver_disease             = var_dict[["non_alcoholic_fatty_liver_disease"]],
                         other_liver_disease                           = var_dict[["other_liver_disease"]],
                         atrial_fibrillation                           = var_dict[["atrial_fibrillation"]],
                         atrial_fibrillation_qof                       = var_dict[["atrial_fibrillation_qof"]],
                         arrhythmia_other                              = var_dict[["arrhythmia_other"]],
                         ischaemic_heart_disease_mi                    = var_dict[["ischaemic_heart_disease_mi"]],
                         ischaemic_heart_disease_non_mi                = var_dict[["ischaemic_heart_disease_non_mi"]],
                         heart_failure                                 = var_dict[["heart_failure"]],
                         heart_failure_qof                             = var_dict[["heart_failure_qof"]],
                         vascular_disease                              = var_dict[["vascular_disease"]],
                         vascular_disease_qof                          = var_dict[["vascular_disease_qof"]],
                         cardiac_disease_other                         = var_dict[["cardiac_disease_other"]],
                         pre_diabetes                                  = var_dict[["pre_diabetes"]],
                         type_1_diabetes                               = var_dict[["type_1_diabetes"]],
                         type_2_diabetes                               = var_dict[["type_2_diabetes"]],
                         cancer_qof                                    = var_dict[["cancer_qof"]],
                         lung_cancer                                   = var_dict[["lung_cancer"]],
                         lung_cancer_within_5_years_flag               = var_dict[["lung_cancer_within_5_years_flag"]],
                         breast_cancer                                 = var_dict[["breast_cancer"]],
                         breast_cancer_within_5_years_flag             = var_dict[["breast_cancer_within_5_years_flag"]],
                         bowel_cancer                                  = var_dict[["bowel_cancer"]],
                         bowel_cancer_within_5_years_flag              = var_dict[["bowel_cancer_within_5_years_flag"]],
                         prostate_cancer                               = var_dict[["prostate_cancer"]],
                         prostate_cancer_within_5_years_flag           = var_dict[["prostate_cancer_within_5_years_flag"]],
                         leukaemia_lymphoma_cancer                     = var_dict[["leukaemia_lymphoma_cancer"]],
                         leukaemia_lymphoma_cancer_within_5_years_flag = var_dict[["leukaemia_lymphoma_cancer_within_5_years_flag"]],
                         cervical_cancer                               = var_dict[["cervical_cancer"]],
                         cervical_cancer_within_5_years_flag           = var_dict[["cervical_cancer_within_5_years_flag"]],
                         ovarian_cancer                                = var_dict[["ovarian_cancer"]],
                         ovarian_cancer_within_5_years_flag            = var_dict[["ovarian_cancer_within_5_years_flag"]],
                         melanoma_cancer                               = var_dict[["melanoma_cancer"]],
                         melanoma_cancer_within_5_years_flag           = var_dict[["melanoma_cancer_within_5_years_flag"]],
                         nonmalignant_skin_cancer                      = var_dict[["nonmalignant_skin_cancer"]],
                         nonmalignant_skin_cancer_within_5_years_flag  = var_dict[["nonmalignant_skin_cancer_within_5_years_flag"]],
                         headneck_cancer                               = var_dict[["headneck_cancer"]],
                         headneck_cancer_within_5_years_flag           = var_dict[["headneck_cancer_within_5_years_flag"]],
                         giliver_cancer                                = var_dict[["giliver_cancer"]],
                         giliver_cancer_within_5_years_flag            = var_dict[["giliver_cancer_within_5_years_flag"]],
                         other_cancer                                  = var_dict[["other_cancer"]],
                         other_cancer_within_5_years_flag              = var_dict[["other_cancer_within_5_years_flag"]],
                         metastatic_cancer                             = var_dict[["metastatic_cancer"]],
                         metastatic_cancer_within_5_years_flag         = var_dict[["metastatic_cancer_within_5_years_flag"]],
                         bladder_cancer                                = var_dict[["bladder_cancer"]],
                         bladder_cancer_within_5_years_flag            = var_dict[["bladder_cancer_within_5_years_flag"]],
                         kidney_cancer                                 = var_dict[["kidney_cancer"]],
                         kidney_cancer_within_5_years_flag             = var_dict[["kidney_cancer_within_5_years_flag"]])


    swd_att_hist_data <- db_conn_struct$swd_att_hist$data %>%                                       #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%                                 #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &                                 #leave if no id, as can't do anything with these
                      !is.na(.data$attribute_period_date) &
                      .data$pseudo_nhs_id %in% patient_ids) %>%
      dplyr::collect() %>%
      dplyr::mutate(pseudo_nhs_id         = as.numeric(.data$pseudo_nhs_id),
                    attribute_period_date = lubridate::as_date(.data$attribute_period_date))

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    swd_att_hist_data <- dplyr::left_join(swd_att_hist_data, ids, by = "pseudo_nhs_id")
  }

  if(db_conn_struct$swd_att$is_connected){

    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$swd_att$table_name)       #create the named list to convert to standardised naming
    base_vars       <- c(pseudo_nhs_id                        = var_dict[["pseudo_nhs_id"]],
                         index_of_multiple_deprivation_decile = var_dict[["index_of_multiple_deprivation_decile"]],
                         income_decile                        = var_dict[["income_decile"]],
                         employment_decile                    = var_dict[["employment_decile"]],
                         air_quality_indicator                = var_dict[["air_quality_indicator"]],
                         urban_rural_classification           = var_dict[["urban_rural_classification"]])

    swd_att_data <- db_conn_struct$swd_att$data %>%                                       #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%                                 #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &                                 #leave if no id, as can't do anything with these
                    .data$pseudo_nhs_id %in% patient_ids) %>%
      dplyr::collect() %>%
      dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id))

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    swd_att_data <- dplyr::left_join(ids, swd_att_data, by = "pseudo_nhs_id")

  }

  # Join the data frames on the ID
  ids <- data.frame(pseudo_nhs_id = patient_ids)
  data <- purrr::reduce(list(ids, swd_att_hist_data, swd_att_data), dplyr::full_join, by = "pseudo_nhs_id") %>%
    dplyr::group_nest(.data$pseudo_nhs_id, .key = "swd_attributes")

  # Stop the clock
  message(paste("Time taken to execute load_swd_attributes() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  return(data)
}
