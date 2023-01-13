library(fabricatr)
library(GGally)
library(ggplot2)
library(dplyr)
library(purrr)

# Based on ARC-HBR validation paper (Cao et al.)

# Define the proportions of HBR patients to be in each HBR score category - central illustration figure
config = list(
  arc_hbr_pct = c("0"=0, "1"=0.616, "2"=0.291, "3"=0.079, "4"=0.014, "5"=0, "6"=0, "7"=0, "8"=0, "9"=0),
  pct_male = 0.751,
  pct_oac = 0.185,
  pct_sev_ckd = 0.137,
  pct_mod_ckd = 0.396,
  pct_sev_anaemia = 0.332,
  pct_mild_anaemia = 0.369,
  pct_bleed = 0.037,
  pct_thrmcyt = 0.043,
  pct_surgery = 0.082,
  pct_cva = 0.207,
  pct_malig = 0.168,
  age_mean = 71.7,
  age_sd = 11.0)

# Size of output / final dataset
Nout = 1e6

# Size of dataframe to sample from
N = Nout*10

# Create the data
hbr_data <- data.frame(
  # DEMOGRAPHICS from table 1
  sex           = sample(factor(c("male","female")), N, replace=T, prob=c(config[["pct_male"]],1-config[["pct_male"]])),
  # MAJOR criteria - from figure 1
  major_anaemia = sample(c(T,F), N, replace=T, prob=c(config[["pct_sev_anaemia"]],1-config[["pct_sev_anaemia"]])),
  major_oac     = sample(c(T,F), N, replace=T, prob=c(config[["pct_oac"]],1-config[["pct_oac"]])),
  major_malig   = sample(c(T,F), N, replace=T, prob=c(config[["pct_malig"]],1-config[["pct_malig"]])),
  major_sev_ckd = sample(c(T,F), N, replace=T, prob=c(config[["pct_sev_ckd"]],1-config[["pct_sev_ckd"]])),
  major_surgery = sample(c(T,F), N, replace=T, prob=c(config[["pct_surgery"]],1-config[["pct_surgery"]])),
  major_thrmcyt = sample(c(T,F), N, replace=T, prob=c(config[["pct_thrmcyt"]],1-config[["pct_thrmcyt"]])),
  # MINOR criteria - from figure 1
  minor_age_gt75= rnorm(N, mean=config[["age_mean"]], sd=config[["age_sd"]])>=75,
  minor_mod_ckd = sample(c(T,F), N, replace=T, prob=c(config[["pct_mod_ckd"]],1-config[["pct_mod_ckd"]])),
  minor_mild_an = sample(c(T,F), N, replace=T, prob=c(config[["pct_mild_anaemia"]],1-config[["pct_mild_anaemia"]])),
  major_cva     = sample(c(T,F), N, replace=T, prob=c(config[["pct_cva"]],1-config[["pct_cva"]])),
  minor_bleed   = sample(c(T,F), N, replace=T, prob=c(config[["pct_bleed"]],1-config[["pct_bleed"]]))
) %>%
  # Calculate the number of times the HBR category has been satisfied (x1 for a major and x1 for 2 minor)
  mutate(maj_score = pmap_dbl(select(., starts_with("major")), sum),
         min_score = pmap_dbl(select(., starts_with("minor")), sum) %/% 2,
         arc_hbr_score = map2_dbl(maj_score, min_score, sum)) %>%
  # Ditch the scores we don't care about, i.e. the ones where we want 0% proportion, as defined int he config
  filter(between(arc_hbr_score,
                 left  = as.numeric(names(config$arc_hbr_pct)[first(which(config$arc_hbr_pct!=0))]),
                 right = as.numeric(names(config$arc_hbr_pct)[last(which(config$arc_hbr_pct!=0))]))) %>%
  # Apply the wanted proportions of each HBR score group
  group_by(arc_hbr_score) %>%
  group_modify(~slice_head(.x, n=Nout*config$arc_hbr_pct[[as.character(.y)]])) %>%
  ungroup()

# Check how well we did - by group
hbr_data %>%
  mutate(n=n()) %>%
  group_by(arc_hbr_score) %>%
  summarise(prop = n()/first(n)) %>%
  mutate(arc_hbr_score = as.character(arc_hbr_score)) %>%
  left_join(data.frame(arc_hbr_score = names(config$arc_hbr_pct),
                       target_pct    = unname(config$arc_hbr_pct)),
            by = "arc_hbr_score")

# Check how well we did - by group
hbr_data %>%
  mutate(n=n()) %>%
  summarise(across(matches("major|minor"), ~ sum(.x)/first(n)))

# Pairs plot
# ggpairs(data = hbr_data %>% select(arc_hbr_score, starts_with("major")),
#         mapping = ggplot2::aes(colour=arc_hbr_score))
