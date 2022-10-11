if(.Platform$OS.type == "unix") {
  DEV_FLAG = "home"
} else {
  DEV_FLAG = "work"
}

db_conn_struct = list(
  sus_apc      = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_apc_sem.csv",               package="acsprojectns")),
  sus_ae       = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_ae_sem.csv",                package="acsprojectns")),
  swd_act      = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_swd_activity.csv",          package="acsprojectns")),
  swd_att      = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_swd_attribute.csv",         package="acsprojectns")),
  swd_att_hist = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_swd_attribute_history.csv", package="acsprojectns")),
  mortality    = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_nhais_deaths.csv",          package="acsprojectns")),
  mortality_2  = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_civil_mortality.csv",       package="acsprojectns")),
  results      = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_swd_measurement.csv",       package="acsprojectns")),
  swd_lsoa     = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_swd_lsoa_descriptions.csv", package="acsprojectns")),
  camb_scr     = Database$new(system.file(paste0("database_connections/",DEV_FLAG), "database_new_cambridge_score.csv",   package="acsprojectns"))
)

acs_date_window_min <- as.POSIXct("2018-07-01", tz = "GMT")   #GP activity, including prescriptions, in the SWD exist from 2018-07-01; GP attributes actually starts 2019-10-01
acs_date_window_max <- as.POSIXct("2021-12-31", tz = "GMT")   #as.POSIXct("2021-12-31", tz = "GMT")   #
