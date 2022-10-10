##################################################################
##                     Libraries and stuff                      ##
##################################################################
library(usethis)
library(devtools)
library(lubridate)
library(survival)
library(survminer)
rm(list=ls())
load_all()
future::plan(future::multisession, workers=parallel::detectCores()-1)  # you must re-install the whole package after each edit to use future_maps, otherwise it doesn't use the correct functions


##----------------------------------------------------------------
##                    Load the feature matrix                    -
##----------------------------------------------------------------
load("data/feature_matrix.rda")

##----------------------------------------------------------------
##                            Resources                          -
##----------------------------------------------------------------
# multiple curves - https://github.com/kassambara/survminer/issues/195

##----------------------------------------------------------------
##                       Create the fits                         -
##----------------------------------------------------------------
nace_fit         <- survfit(Surv(feature_matrix$surv_nace_days, feature_matrix$status_nace) ~ 1)
macce_fit        <- survfit(Surv(feature_matrix$surv_macce_days, feature_matrix$status_macce) ~ 1)
stroke_fit       <- survfit(Surv(feature_matrix$surv_stroke_days, feature_matrix$status_stroke) ~ 1)
bleed_fit        <- survfit(Surv(feature_matrix$surv_bleed_days, feature_matrix$status_bleed) ~ 1)
acs_fit          <- survfit(Surv(feature_matrix$surv_acs_days, feature_matrix$status_acs) ~ 1)
pci_fit          <- survfit(Surv(feature_matrix$surv_pci_days, feature_matrix$status_pci) ~ 1)
cabg_fit         <- survfit(Surv(feature_matrix$surv_cabg_days, feature_matrix$status_cabg) ~ 1)
hf_fit           <- survfit(Surv(feature_matrix$surv_hf_days, feature_matrix$status_hf) ~ 1)
death_fit        <- survfit(Surv(feature_matrix$surv_death_days, feature_matrix$status_death) ~ 1)
cv_death_fit     <- survfit(Surv(feature_matrix$surv_cv_death_days, feature_matrix$status_cv_death) ~ 1)
non_cv_death_fit <- survfit(Surv(feature_matrix$surv_non_cv_death_days, feature_matrix$status_non_cv_death) ~ 1)
bleed_death_fit  <- survfit(Surv(feature_matrix$surv_bleed_death_days, feature_matrix$status_bleed_death) ~ 1)


##----------------------------------------------------------------
##                       Plot survival curves                    -
##----------------------------------------------------------------


# -- Survival - NACE by HbA1c measured by 4 months
{
  hba1c_group = feature_matrix %>%
    dplyr::mutate(hba1c_group = as.character(.data$hba1c_group),
                  hba1c_group = replace(.data$hba1c_group, lubridate::duration(.data$surv_nace_days, units = "days") < lubridate::duration(4, units = "months"), "event_before_4m"),
                  hba1c_group = factor(.data$hba1c_group, levels = c("event_before_4m", "Normal", "Prediabetes", "Diabetes", "Not measured"))) %>%
    dplyr::select(.data$hba1c_group) %>%
    dplyr::pull()

  nace_fit_by_4m_hba1c <- survfit(Surv(feature_matrix$surv_nace_days, feature_matrix$status_nace) ~ hba1c_group)
  survminer::ggsurvplot(nace_fit_by_4m_hba1c,
                        data = feature_matrix,
                        risk.table = TRUE,
                        tables.theme = theme_cleantable(),
                        ylim = c(0.0, 1.0),
                        title = "ACS patients - survival from NACE by HbA1c result within 4months (+blanking)",
                        xlab = "Days",
                        ylab = "Survival (%)",
                        legend.labs = levels(hba1c_group),
                        palette = c("grey", "green", "orange", "red", "purple"))

  hba1c_group = feature_matrix %>%
    dplyr::select(.data$hba1c_group) %>%
    dplyr::mutate(hba1c_group = factor(.data$hba1c_group, levels = c("Normal", "Prediabetes", "Diabetes", "Not measured"))) %>%
    dplyr::pull()
  nace_fit_by_4m_hba1c <- survfit(Surv(feature_matrix$surv_nace_days, feature_matrix$status_nace) ~ hba1c_group)
  survminer::ggsurvplot(nace_fit_by_4m_hba1c,
                        data = feature_matrix,
                        risk.table = TRUE,
                        tables.theme = theme_cleantable(),
                        ylim = c(0.0, 1.0),
                        title = "ACS patients - survival from NACE by HbA1c result within 4months (-blanking)",
                        xlab = "Days",
                        ylab = "Survival (%)",
                        legend.labs = levels(hba1c_group),
                        palette = c("green", "orange", "red", "purple"))
}

# -- Survival - NACE by BP measured by 4 months
{
  bp_group = feature_matrix %>%
    dplyr::mutate(bp_group = as.character(.data$bp_group),
                  bp_group = replace(.data$bp_group, lubridate::duration(.data$surv_nace_days, units = "days") < lubridate::duration(4, units = "months"), "event_before_4m"),
                  bp_group = factor(.data$bp_group, levels = c("event_before_4m", "Controlled BP", "High BP", "Not measured"))) %>%
    dplyr::select(.data$bp_group) %>%
    dplyr::pull()

  nace_fit_by_4m_bp <- survfit(Surv(feature_matrix$surv_nace_days, feature_matrix$status_nace) ~ bp_group)
  survminer::ggsurvplot(nace_fit_by_4m_bp,
                        data = feature_matrix,
                        risk.table = TRUE,
                        tables.theme = theme_cleantable(),
                        ylim = c(0.0, 1.0),
                        title = "ACS patients - survival from NACE",
                        xlab = "Days",
                        ylab = "Survival (%)",
                        legend.labs = levels(bp_group),
                        palette = c("grey", "green", "red", "purple"))
}

#
# # -- Survival - NACE
# {
#   fits = list(nace = nace_fit, stroke = stroke_fit, bleed = bleed_fit, acs = acs_fit, pci = pci_fit, cabg = cabg_fit, hf= hf_fit, death=death_fit, cv_death=cv_death_fit, non_cv_death=non_cv_death_fit, bleed_death=bleed_death_fit)
#   survminer::ggsurvplot(fits,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         ylim = c(0.0, 1.0),
#                         title = "ACS patients - survival from NACE",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
# }
#
# # -- Survival - MACCE
# {
# fits = list(macce = macce_fit)
# survminer::ggsurvplot(fits,
#                       data = feature_matrix,
#                       combine = TRUE,
#                       risk.table = TRUE,
#                       tables.theme = theme_cleantable(),
#                       ylim = c(0.0, 1.0),
#                       title = "ACS patients - survival from MACCE",
#                       xlab = "Days",
#                       ylab = "Survival (%)")
# }
#
# # -- Survival - death breakdown
# {
# fits = list(death=death_fit, cv_death=cv_death_fit, non_cv_death=non_cv_death_fit, bleed_death=bleed_death_fit)
# survminer::ggsurvplot(fits,
#                       data = feature_matrix,
#                       combine = TRUE,
#                       risk.table = TRUE,
#                       tables.theme = theme_cleantable(),
#                       ylim = c(0.5, 1.0),
#                       title = "ACS patients - survival, death breakdown",
#                       xlab = "Days",
#                       ylab = "Survival (%)")
# }
#
# # -- Survival - cardiac breakdown
# {
# fits = list(acs = acs_fit, pci = pci_fit, cabg = cabg_fit, hf= hf_fit, cv_death=cv_death_fit)
# survminer::ggsurvplot(fits,
#                       data = feature_matrix,
#                       combine = TRUE,
#                       risk.table = TRUE,
#                       tables.theme = theme_cleantable(),
#                       legend.labs = c("ACS admission", "Emergency PCI", "Emergency CABG", "HF admission", "CV death"),
#                       palette = c("black", "red", "rosybrown1", "blue", "cyan"),
#                       ylim = c(0.8, 1.0),
#                       title = "ACS patients - survival from cardiac end points",
#                       xlab = "Days",
#                       ylab = "Survival (%)")
# }
#
# # -- Survival - bleeding breakdown
# {
# bleed_fit          <- survfit(Surv(feature_matrix$surv_bleed_days,       feature_matrix$status_bleed) ~ 1)
# gi_bleed_fit       <- survfit(Surv(feature_matrix$surv_gi_bleed_days,    feature_matrix$status_gi_bleed) ~ 1)
# ich_bleed_fit      <- survfit(Surv(feature_matrix$surv_ich_bleed_days,   feature_matrix$status_ich_bleed) ~ 1)
# other_gi_bleed_fit <- survfit(Surv(feature_matrix$surv_other_bleed_days, feature_matrix$status_other_bleed) ~ 1)
# fits = list(all_bleed = bleed_fit, gi_bleed = gi_bleed_fit, ich_bleed = ich_bleed_fit, other_bleed = other_gi_bleed_fit)
# survminer::ggsurvplot(fits,
#                       data = feature_matrix,
#                       combine = TRUE,
#                       risk.table = TRUE,
#                       tables.theme = theme_cleantable(),
#                       legend.labs = c("All bleeds", "GI bleeds", "ICH bleeds", "Other bleeds"),
#                       palette = c("black", "red", "blue", "cyan"),
#                       ylim = c(0.9, 1.0),
#                       title = "ACS patients - survival from bleeding hospital admissions",
#                       xlab = "Days",
#                       ylab = "Survival (%)")
# }
#
# # -- Survival split by cardiac rehab attendance
# {
#   # Plot death components by rehab attendance
#   death_fit_by_rehab        <- survfit(Surv(feature_matrix$surv_death_days,        feature_matrix$status_death)        ~ feature_matrix$any_cardiac_rehab)
#   cv_death_fit_by_rehab     <- survfit(Surv(feature_matrix$surv_cv_death_days,     feature_matrix$status_cv_death)     ~ feature_matrix$any_cardiac_rehab)
#   non_cv_death_fit_by_rehab <- survfit(Surv(feature_matrix$surv_non_cv_death_days, feature_matrix$status_non_cv_death) ~ feature_matrix$any_cardiac_rehab)
#   fits = list(death=death_fit_by_rehab, cv_death=cv_death_fit_by_rehab, non_cv_death=non_cv_death_fit_by_rehab)
#   survminer::ggsurvplot(fits,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         legend.labs = c("All death - no rehab", "All death - rehab", "CV death - no rehab", "CV death - rehab", "Non-CV death - no rehab", "Non-CV death - rehab"),
#                         palette = c("black", "grey", "red", "rosybrown1", "blue", "cornflowerblue"),
#                         ylim = c(0.5, 1.0),
#                         title = "ACS patients - survival, death breakdown, by any cardiac rehab attendance",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
#
# }
#
# # -- Survival by IMD quartile
# {
#   # Plot NACE by IMD quartile
#   nace_imd_fit         <- survfit(Surv(feature_matrix$surv_nace_days, feature_matrix$status_nace) ~ feature_matrix$imd_quartile)
#   survminer::ggsurvplot(nace_imd_fit,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         ylim = c(0.7, 1.0),
#                         title = "ACS patients - survival from NACE by IMD quartile",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
# }
#
# # -- Survival split by lipid treatment
# {
#   # Plot NACE by lipid category
#   nace_lipid_fit       <- survfit(Surv(feature_matrix$surv_nace_days, feature_matrix$status_nace) ~ feature_matrix$lipid_strategy_0m_desc)
#   survminer::ggsurvplot(nace_lipid_fit,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         legend.labs = c("Censored", "None", "High dose", "Low/moderate dose"),
#                         palette = c("grey", "red", "green", "orange"),
#                         ylim = c(0.0, 1.0),
#                         title = "ACS patients - survival from NACE by initial strategy at 0m",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
#
#   # Plot MACCE by lipid category
#   test = feature_matrix %>% dplyr::filter(.data$lipid_strategy_4m_desc != "censored_by_4months")
#   test$lipid_strategy_4m_desc = factor(test$lipid_strategy_4m_desc, levels = c("optimum_dose", "sub_optimum_dose", "none"))
#   macce_lipid_fit       <- survfit(Surv(test$surv_macce_days, test$status_macce) ~ test$lipid_strategy_4m_desc)
#   survminer::ggsurvplot(macce_lipid_fit,
#                         data = test,
#                         combine = TRUE,
#                         risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         legend.labs = c("None", "High dose", "Low/moderate dose"),
#                         palette = c("red", "green", "orange"),
#                         ylim = c(0.5, 1.0),
#                         title = "ACS patients - survival from MACCE by initial strategy",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
#
#   # Plot recurrent ACS by lipid strategy at 0 months
#   acs_fit          <- survfit(Surv(feature_matrix$surv_acs_days, feature_matrix$status_acs) ~ feature_matrix$lipid_strategy_0m_desc)
#   fits = list(acs = acs_fit)
#   survminer::ggsurvplot(fits,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         legend.labs = c("Censored", "None", "High dose", "Low/moderate dose"),
#                         palette = c("grey", "red", "green", "orange"),
#                         ylim = c(0.5, 1.0),
#                         title = "ACS patients - survival from recurrent ACS by initial lipid strategy",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
#
#   # Plot emergency PCI by lipid strategy at 4 months
#   pci_fit          <- survfit(Surv(feature_matrix$surv_pci_days, feature_matrix$status_pci) ~ feature_matrix$lipid_strategy_0m_desc)
#   fits = list(pci = pci_fit)
#   survminer::ggsurvplot(fits,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         legend.labs = c("Censored", "None", "High dose", "Low/moderate dose"),
#                         palette = c("grey", "red", "green", "orange"),
#                         ylim = c(0.8, 1.0),
#                         title = "ACS patients - survival from emergency PCI by initial lipid strategy",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
#
#   # Plot CV death by lipid strategy at 4 months
#   cv_death_fit     <- survfit(Surv(feature_matrix$surv_cv_death_days, feature_matrix$status_cv_death) ~ feature_matrix$lipid_strategy_0m_desc)
#   fits = list(cv_death=cv_death_fit)
#   survminer::ggsurvplot(fits,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         legend.labs = c("Censored", "None", "High dose", "Low/moderate dose"),
#                         palette = c("grey", "red", "green", "orange"),
#                         ylim = c(0.4, 1.0),
#                         title = "ACS patients - survival from CV death by initial lipid strategy",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
#
#   # Plot heart failure admissions by lipid strategy at 4 months
#   hf_fit           <- survfit(Surv(feature_matrix$surv_hf_days, feature_matrix$status_hf) ~ feature_matrix$lipid_strategy_0m_desc)
#   fits = list(hf= hf_fit)
#   survminer::ggsurvplot(fits,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         legend.labs = c("Censored", "None", "High dose", "Low/moderate dose"),
#                         palette = c("grey", "red", "green", "orange"),
#                         ylim = c(0.5, 1.0),
#                         title = "ACS patients - survival from heart failure admission by 0month lipid strategy",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
# }
#
# # -- Survival split by blood pressure targets
# {
#   # Plot NACE by blood pressure targets
#   nace_htn_fit       <- survfit(Surv(feature_matrix$surv_nace_days, feature_matrix$status_nace) ~ feature_matrix$bp_group)
#   survminer::ggsurvplot(nace_htn_fit,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         #risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         legend.labs = c("High BP", "Controlled BP", "Not measured"),
#                         palette = c("red","green",  "gray"),
#                         ylim = c(0.0, 1.0),
#                         title = "ACS patients - survival from NACE by NICE BP targets at <=4m post ACS",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
# }
#
# # -- Survival split by HbA1c targets
# {
#   # Plot NACE by HbA1c targets
#   nace_hba1c_fit       <- survfit(Surv(feature_matrix$surv_nace_days, feature_matrix$status_nace) ~ feature_matrix$hba1c_group)
#   survminer::ggsurvplot(nace_hba1c_fit,
#                         data = feature_matrix,
#                         combine = TRUE,
#                         #risk.table = TRUE,
#                         tables.theme = theme_cleantable(),
#                         legend.labs = c("Diabetes", "Normal", "Not measured", "Prediabetes"),
#                         palette = c("red", "green", "gray", "orange"),
#                         ylim = c(0.0, 1.0),
#                         title = "ACS patients - survival from NACE by HbA1c level at <=4m post ACS",
#                         xlab = "Days",
#                         ylab = "Survival (%)")
# }
#
# # -- Cox Proportional Hazards Model for lipid therapy on MACCE
# {
#   test = feature_matrix %>% dplyr::filter(.data$lipid_strategy_4m_desc != "censored_by_4months")
#   test$lipid_strategy_4m_desc = factor(test$lipid_strategy_4m_desc, levels = c("optimum_dose", "sub_optimum_dose", "none"))
#   res.cox <- coxph(Surv(test$surv_macce_days, test$status_macce) ~ test$lipid_strategy_4m_desc, data = test)
#   summary(res.cox)
# }
#
#
