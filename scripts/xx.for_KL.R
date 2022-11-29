# Deal with the questions from the ICB meeting on this ACS work
library(usethis)
library(devtools)
library(icdb)
library(dplyr)
library(ggplot2)
library(viridis)
rm(list=ls())
load_all()
source(system.file("scripts/0.init.R", package = "acsprojectns"))

df <- db_conn_struct$swd_att$data %>%
  select(nhs_number, sex, bmi, ihd_mi, ihd_nonmi, hf, ht) %>%
  collect() %>%
  mutate(bmi = replace(bmi, (bmi<10|bmi>100), NA))

summarise(df, across(.cols = -nhs_number, ~sum(is.na(.x))/n()))

df %>%
  summarise(pct_missing = sum(is.na(bmi))/n(),
            n      = n(),
            mean   = mean(bmi, na.rm = T),
            sd     = sd(bmi, na.rm = T),
            median = median(bmi, na.rm = T),
            iq25   = quantile(bmi, probs=0.25, na.rm=T),
            iq75   = quantile(bmi, probs=0.75, na.rm=T),
            max    = max(bmi, na.rm = T),
            min    = min(bmi, na.rm = T))

df %>%
  group_by(ihd_mi) %>%
  summarise(pct_missing = sum(is.na(bmi))/n(),
            n      = n(),
            mean   = mean(bmi, na.rm = T),
            sd     = sd(bmi, na.rm = T),
            median = median(bmi, na.rm = T),
            iq25   = quantile(bmi, probs=0.25, na.rm=T),
            iq75   = quantile(bmi, probs=0.75, na.rm=T),
            max    = max(bmi, na.rm = T),
            min    = min(bmi, na.rm = T))

ggplot(df,aes(x=bmi, group=ihd_mi, fill=ihd_mi)) +
      stat_density(position="identity", bw=1.25) +
      scale_x_continuous(limits =c(0,70)) +
      facet_grid(~ihd_mi, scales = "free")


ggplot(df,aes(x=bmi)) +
  geom_histogram(position="identity", binwidth=1) +
  scale_x_continuous(limits =c(0,70))










