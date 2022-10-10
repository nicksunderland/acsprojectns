##################################################################
##                     Things I need to fix                     ##
##################################################################
# TODO:
# need to check attribute data as to whether they still exist in the dataset - then censor / update the max follow up date for survival stuff
# to ICD10 query - add something to deal with hospital admission method - both filtering and returning; and to count number within the time window
# 3. reorder codes in the codes .csv file, most important first
# 4. formally define the figures and needed data - create feature matrix
# remove the is.null() test from the queries as I fixed the loads NULL problem
# add dummy DFs to the beginning of the load functions to the end joins work if no database found
# add checks on the load outputs, e.g. make sure that demographics is only one row
# fix the query medications function to deal with differences in units when averaging

# add a gp_practice_code to the feature matrix
