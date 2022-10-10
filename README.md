
<!-- README.md is generated from README.Rmd. Please edit that file -->

# acsprojectns

<!-- badges: start -->

[![R-CMD-check](https://github.com/nicksunderland/acsprojectns/workflows/R-CMD-check/badge.svg)](https://github.com/nicksunderland/acsprojectns/actions)
<!-- badges: end -->

The goal of acsprojectns is to simplify querying of multiple healthcare
databases for important information relating to the estimation of
bleeding and ischaemic risk in patients.

## Installation

You can install the development version of acsprojectns from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("nicksunderland/acsprojectns")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(acsprojectns)
library(magrittr)


# Define a list of ICD-10 codes to search for
codes = c("I219", "I220", "some_other_code") # 2 acute coronary syndrome codes

# Define a list of CCG provider codes to narrow the population
ccg_provider_codes = c("11H", "11T", "12A", "15c") # Bristol/BNSSG codes; alternatively 'NA' to search all

# Define a time window to search within
time_window = lubridate::interval(as.POSIXct("2012-01-01"), as.POSIXct("2022-01-01"), tzone = "GMT")

# Get hospital admissions containing the ICD-10 codes as the primary diagnosis
cohort <- get_hospital_admissions(ccg_provider_code_list = ccg_provider_codes,
                                  level                  = "spell",
                                  codes_list             = codes,
                                  search_strategy        = "primary_diagnosis",
                                  return_all_codes       = FALSE,
                                  datetime_window        = time_window,
                                  verbose                = TRUE) %>%    # All hospital spells (non-overlapping)
  dplyr::group_by(pseudo_nhs_id) %>%                                    
  dplyr::slice_min(lubridate::int_start(spell_interval)) %>%            # Take only the first spell
  dplyr::ungroup() %>%
  dplyr::distinct(pseudo_nhs_id,                                        # Rename columns as appropriate
                  acs_spell_interval_index = spell_interval, 
                  acs_codes = spell_codes)
#> <SQL>
#> SELECT *
#> FROM (SELECT *
#> FROM (SELECT *
#> FROM (SELECT DISTINCT `AIMTC_Pseudo_NHS` AS `pseudo_nhs_id`, `StartDate_ConsultantEpisode` AS `episode_start_date`, `EndDate_ConsultantEpisode` AS `episode_end_date`, `StartTime_Episode` AS `episode_start_time`, `EndTime_Episode` AS `episode_end_time`, `AIMTC_ProviderSpell_Start_Date` AS `spell_start_date`, `AIMTC_ProviderSpell_End_Date` AS `spell_end_date`, `StartDate_HospitalProviderSpell` AS `spell_start_date_alt`, `DischargeDate_FromHospitalProviderSpell` AS `spell_end_date_alt`, `StartTime_HospitalProviderSpell` AS `spell_start_time`, `DischargeTime_HospitalProviderSpell` AS `spell_end_time`, `AIMTC_PCTRegGP` AS `ccg_provider_code`, `DiagnosisPrimary_ICD` AS `primary_diagnosis_icd_code`
#> FROM `vw_APC_SEM_001`) `q01`
#> WHERE (NOT(((`pseudo_nhs_id`) IS NULL)) AND `episode_start_date` >= '2012-01-01T00:00:00Z' AND `episode_start_date` <= '2022-01-01T00:00:00Z')) `q02`
#> WHERE (`primary_diagnosis_icd_code` IN ('I219', 'I220', 'some_other_code'))) `q03`
#> WHERE (`ccg_provider_code` IN ('11H', '11T', '12A', '15c'))
#> Time taken to execute get_hospital_admissions() = 2.01 seconds

print(head(cohort, 10))
#> # A tibble: 10 × 3
#>    pseudo_nhs_id acs_spell_interval_index                         acs_codes
#>            <dbl> <Interval>                                       <list>   
#>  1             5 2021-11-21 12:26:00 GMT--2021-11-21 20:11:00 GMT <chr [1]>
#>  2            37 2019-08-19 02:49:00 GMT--2019-09-19 10:15:00 GMT <chr [1]>
#>  3            60 2018-06-18 07:06:00 GMT--2018-06-18 08:47:00 GMT <chr [1]>
#>  4            61 2018-06-18 21:57:00 GMT--2018-06-18 10:56:00 GMT <chr [1]>
#>  5            74 2018-03-18 00:50:00 GMT--2018-04-18 18:27:00 GMT <chr [1]>
#>  6            80 2021-05-21 08:30:00 GMT--2021-06-21 22:04:00 GMT <chr [1]>
#>  7           105 2021-06-21 11:23:00 GMT--2021-06-21 12:22:00 GMT <chr [1]>
#>  8           132 2019-08-19 00:35:00 GMT--2019-09-19 06:03:00 GMT <chr [1]>
#>  9           157 2018-09-18 00:25:00 GMT--2018-10-18 18:16:00 GMT <chr [1]>
#> 10           168 2019-02-19 21:43:00 GMT--2019-02-19 05:06:00 GMT <chr [1]>
```

What is special about using `README.Rmd` instead of just `README.md`?
You can include R chunks like so:

``` r
summary(cars)
#>      speed           dist       
#>  Min.   : 4.0   Min.   :  2.00  
#>  1st Qu.:12.0   1st Qu.: 26.00  
#>  Median :15.0   Median : 36.00  
#>  Mean   :15.4   Mean   : 42.98  
#>  3rd Qu.:19.0   3rd Qu.: 56.00  
#>  Max.   :25.0   Max.   :120.00
```
