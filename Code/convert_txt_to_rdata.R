# Description: Converts the raw text files imported by WRDS into faster to access .RData files

library(data.table)
library(tidyr)
library(dplyr)
library(lubridate)
library(assertthat)
library(zoo)

DIR = '~/Data/CRSP_MF/'

summary_raw = fread(DIR %>% paste0('summary.txt')) %>% as_tibble()
returns_raw = fread(DIR %>% paste0('returns.txt')) %>% as_tibble()
map_raw = fread(DIR %>% paste0('ret_map.txt')) %>% as_tibble()

clean_data = function (df, type_list){
  print('Cleaning date variables')
  for(x in type_list[['date_vars']]){
    print(x)
    df[[x]] = as.Date(df[[x]])
  }
  
  print('Cleaning numeric variables')
  for(x in type_list[['float_vars']]){
    print(x)
    df[[x]] = as.numeric(df[[x]])
  }
  
  print('Cleaning integer variables')
  for(x in type_list[['int_vars']]){
    print(x)
    df[[x]] = as.integer(df[[x]])
  }
  
  return(df)
}

# Missing value codings
code_as_missing = function(x, code = -99){
  return(ifelse(x == code, NA, x))
}

# Help bound numbers
cap = function(x, bounds = c(0, 100)){
  y = ifelse(x < bounds[1], bounds[1], ifelse(x > bounds[2], bounds[2], x))
  return(y)
}

#### Cleaning up the summary data ####

summary_defs = list()
summary_defs[['date_vars']] = c('caldt', 'nav_latest_dt', 'tna_latest_dt', 'first_offer_dt', 'mgr_dt')
summary_defs[['float_vars']] = c('nav_latest', 'tna_latest', 'per_com', 'per_oth', 'per_cash', 'per_bond')
summary_defs[['int_vars']] = c('crsp_fundno', 'crsp_portno', 'crsp_cl_grp', 'merge_fundno')
summary = clean_data(summary_raw, summary_defs)

# Clean the dates so that the ends line up
summary = summary %>% mutate(caldt = ceiling_date(caldt, unit = 'month'))

# Check that fundno + date uniquely identifies
duplicate_summary = summary[duplicated(summary %>% select(crsp_fundno, caldt)), ]
assert_that(nrow(duplicate_summary) == 0)

# Take care of missing data
summary = summary %>% 
  mutate(nav_latest = code_as_missing(nav_latest),
         tna_latest = code_as_missing(tna_latest),
         per_com = cap(per_com),
         per_oth = cap(per_oth),
         per_cash = cap(per_cash),
         per_bond = cap(per_bond),
         exp_ratio = cap(exp_ratio),
         mgmt_fee = cap(mgmt_fee))

# Restrict yourself to dedicated equity MF
equity_funds = summary %>% 
  filter(substring(crsp_obj_cd, 0, 2) == "ED") %>% 
  select(crsp_fundno) %>% 
  unique()

#### Clean the returns ####
return_list = list('date_vars'=c('caldt'), 'float_vars' = c('mtna', 'mret', 'mnav'), 'int_vars' = c('crsp_fundno'))
returns = clean_data(returns_raw, return_list)

# Clean up the dates in both summary and returns to line up with the end of the month
returns = returns %>% mutate(caldt = ceiling_date(caldt, unit = 'month'))

# Missing data + uniqueness
returns = returns %>% 
  mutate(mtna = code_as_missing(mtna))
return_summary = returns[duplicated(returns %>% select(crsp_fundno, caldt)), ]
assert_that(nrow(return_summary) == 0)

# Remove some of the more extreme return numbers
RET_TRESHOLD = 0.2
returns = returns %>% 
  arrange(crsp_fundno, caldt) %>% 
  group_by(crsp_fundno) %>% 
  mutate(px_ret = mnav / lag(mnav, 1) - 1,
         ret_diff = abs(mret - px_ret),
         cleaned_ret = ifelse(abs(mret) <= RET_TRESHOLD, mret,
                              ifelse(abs(px_ret) <= RET_TRESHOLD, px_ret, mret))) %>% 
  filter(!is.na(cleaned_ret))

new_returns = returns %>% filter(abs(mret) > RET_TRESHOLD)
print(summary(new_returns))

## Consolidate the returns
returns = returns %>% 
  rename(aum = mtna) %>% 
  select(crsp_fundno, caldt, aum, cleaned_ret)

# Building flows. Backfill AUMs
returns = returns %>%
  inner_join(equity_funds, by = 'crsp_fundno') %>% 
  arrange(crsp_fundno, caldt) %>% 
  group_by(crsp_fundno) %>% 
  mutate(aum = na.locf(aum, na.rm = FALSE),
         inflow = aum - lag(aum) * (1 + cleaned_ret)) 

# # Clean the dates on the map
# map = map_raw %>% 
#   mutate(begdt = as.Date(as.character(begdt), format = '%Y%m%d'),
#          enddt = as.Date(as.character(begdt), format = '%Y%m%d'))
# 
# # Merge in the returns with the portfolio map
# joined_returns = returns %>% 
#   inner_join(equity_funds, by = 'crsp_fundno') %>% 
#   inner_join(map, by = 'crsp_fundno') 
# 
# FIRST_MAP_DATE = '2003-03-31'
# compiled_returns = joined_returns %>% 
#   filter(caldt >= begdt || caldt <= FIRST_MAP_DATE) %>% 
#   filter((caldt < enddt) || is.na(enddt))
# 
# # Aggregate up to the portfolio level
# compiled_returns = compiled_returns %>% 
#   arrange(crsp_fundno, caldt) %>% 
#   group_by(crsp_fundno) %>% 
#   mutate(lag_aum = lag(aum))
# 
# portfolio_returns = compiled_returns %>% 
#   arrange(crsp_portno, crsp_fundno, caldt) %>% 
#   group_by(crsp_portno, crsp_fundno) %>% 
#   mutate(aum_denominator = ifelse(is.na(lag_aum), 0, lag_aum))
# 
# print('Collapsing down to portfolios')
# portfolio_returns = portfolio_returns %>% 
#   group_by(crsp_portno, caldt) %>% 
#   summarize(fund_name = min(fund_name, na.rm = T),
#             aum = sum(aum),
#             ret = sum(aum_denominator * cleaned_ret) / sum(aum_denominator))
#             