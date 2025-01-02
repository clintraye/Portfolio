import numpy as np
import pandas as pd
import os

# =====================================================================================
# PREPARATION OF MIGRANT DATA CSV FOR MERGING

# Loaded migrant data and set some dataframe dtypes
dtype_dict = {"Child's Country of Origin": 'string',
               "Child's Gender": 'string',
               "Sponsor Zipcode": 'string',
               "Relationship of Sponsor": 'string'
               }
migrant_df = pd.read_csv('data.csv',dtype=dtype_dict)
migrant_backup = migrant_df.copy()
original_len_migrant_df = len(migrant_df)

# Set dtypes for Date of Entry and Release
#Created columns for year and month in anticipation of employment data merge
migrant_df["Child's Date of Entry"] = migrant_df["Child's Date of Entry"].astype('datetime64[ns]')
migrant_df['entry_year'] = migrant_df["Child's Date of Entry"].dt.strftime('%Y')
migrant_df['entry_month'] = migrant_df["Child's Date of Entry"].dt.strftime("%m")
migrant_df["Child's Date of Release"] = migrant_df["Child's Date of Release"].astype('datetime64[ns]')
migrant_df['release_year'] = migrant_df["Child's Date of Release"].dt.strftime('%Y')
migrant_df['release_month'] = migrant_df["Child's Date of Release"].dt.strftime("%m")

# Inspected two instances per Github README, IDs: 1905 and 3787; Removed duplicates
migrant_df = migrant_df.drop([1904,3787])

# Created new Sponsor Category value for NaN values in Sponsor Category
# Note: When Sponsor Category was NaN all sponsor relationships appear to be outside of immediate family
migrant_df["Sponsor Category"] = migrant_df["Sponsor Category"].fillna(4)
nan_rows = migrant_df.isna().any(axis=1)
migrant_nan_df = migrant_df[nan_rows]
data_preserved = round(100*(original_len_migrant_df-len(migrant_nan_df))/original_len_migrant_df,4)
# print("Percent of data preserved after removing rows with NaN values for zipcodes: " + str(data_preserved) + "%" + "\n")

# Removed rows with NaN value for Zipcode
migrant_df = migrant_df.dropna()

# Changed Sponsor Category dtype to integer
migrant_df["Sponsor Category"] = migrant_df["Sponsor Category"].astype('int64')

# Renamed Sponsor Zipcode in anticipation of merge
migrant_df.rename(columns={"Sponsor Zipcode": "zip"}, inplace=True)

# WORK SUMMARY
print(migrant_df.dtypes)
print(migrant_df.head())
print("\n\n")


# =====================================================================================
# PREPARATION OF ZIPCODE/STATE/COUNTY CSV FOR MERGING

# Loaded zip code data and set some dataframe dtypes
dtype_dict2 = {"zip": 'string',
               "state": 'string',
               "county": 'string'
               }
zip_df = pd.read_csv('zip_code_database.csv', dtype=dtype_dict2)
zip_backup = zip_df.copy()

# Dropped columns that lacked relevance
zip_df = zip_df.drop(columns=['type',
                              'decommissioned',
                              'primary_city',
                              'acceptable_cities',
                              'unacceptable_cities',
                              'timezone',
                              'area_codes',
                              'world_region',
                              'country',
                              'latitude',
                              'longitude',
                              'irs_estimated_population'])


# Explored NaN values for zip_df
# All were related to missing counties and none of the zips appeared in our migrant_df
num_na_zip = zip_df.isnull().sum().sum()
# print("Number of fields from zip_df with NaN data:", num_na_zip, "\n")

# WORK SUMMARY
print(zip_df.dtypes)
print(zip_df.head())
print("\n\n")


# =====================================================================================
# MERGE OF MIGRANT DATA WITH ZIP/COUNTY DATA

#Merged migrant_df and zip_df with a left join
mig_county_merge = pd.merge(migrant_df,zip_df,on='zip',how='left')

#Filtered out observations that involved releases outside of the 50 states plus DC
before_state_filter = len(mig_county_merge)
state_codes_removed = ['PR', 'VI', 'AE', 'AA', 'AP', 'AS', 'GU', 'PW', 'AS', 'FM', 'MH', 'MP']
mig_county_merge = mig_county_merge[mig_county_merge.state.isin(state_codes_removed) == False]
after_state_filter = len(mig_county_merge)

# Explored Nan values in mig_county_merge; All related to zips in the migrant_df that didn't have county and state values
# Determined the number of rows affected by the migrant release zips without state/county data
nan_rows_mig_county = mig_county_merge.isna().any(axis=1)
mig_county_nan_df = mig_county_merge[nan_rows_mig_county]
num_merge_obs_nan = len(mig_county_nan_df)

#Removed rows without state/county data
mig_county_merge = mig_county_merge.dropna()

#WORK SUMMARY - Assembled final observations
final_merge_length = len(mig_county_merge)
num_observations_removed = original_len_migrant_df-final_merge_length
print("Total number of observations removed:", num_observations_removed)
data_preserved_merge = round(100*(original_len_migrant_df-num_observations_removed)/original_len_migrant_df,4)
print("Percent of migrant data preserved: " + str(data_preserved_merge) + "% \n")
num_na_merge = mig_county_merge.isnull().sum().sum()
print("Number of fields from final_merge with NaN values:", num_na_merge, "\n")

print(mig_county_merge.dtypes)
print(mig_county_merge.head())
