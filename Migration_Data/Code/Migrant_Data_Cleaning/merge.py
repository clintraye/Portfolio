import pandas as pd
import os

# =====================================================================================
# PREPARATION OF MIGRANT DATA CSV FOR MERGING

# Loaded migrant data and checked dataframe dtypes
dtype_dict = {"Child's Country of Origin": 'string',
               "Child's Gender": 'string',
               "Sponsor Zipcode": 'string',
               "Relationship of Sponsor": 'string'
               }
migrant_df = pd.read_csv('data.csv',dtype=dtype_dict)
migrant_backup = migrant_df.copy()
original_len_migrant_df = len(migrant_df)
# print(migrant_df.dtypes)

# Set dtypes for Date of Entry and Release
#Created columns for year and month in anticipation of employment data merge
migrant_df["Child's Date of Entry"] = migrant_df["Child's Date of Entry"].astype('datetime64[ns]')
migrant_df['entry_year_month'] = migrant_df["Child's Date of Entry"].dt.strftime('%Y-%m')
migrant_df["Child's Date of Release"] = migrant_df["Child's Date of Release"].astype('datetime64[ns]')
migrant_df['release_year_month'] = migrant_df["Child's Date of Release"].dt.strftime('%Y-%m')


# Selected single ID from each duplicate
# Inspected two instances per Github README, IDs: 1905 and 3787
# Opted to preserve observation for later release date
# print(migrant_df.loc[migrant_df['ID'] == 1905])
# print(migrant_df.loc[migrant_df['ID'] == 3787])
print("migrant_df length before duplicate removal:", len(migrant_df))
migrant_df = migrant_df.drop([1904,3787])
print("migrant_df length after duplicate removal:", len(migrant_df))

# Confirmed duplicate removal
# print(migrant_df.loc[migrant_df['ID'] == 1905])
# print(migrant_df.loc[migrant_df['ID'] == 3787])

# Retrieved count of rows with NaN values; isolated those rows in a new dataframe
# Looked at all 668 NaN rows and each involved NaN values for Zipcode or Sponsor Category
# Created new Sponsor Category value for NaN values in Sponsor Category
# When Sponsor Category was NaN all sponsor relationships appear to be outside of immediate family
# Removed rows with NaN value for Zipcode
num_nan_migrant = migrant_df.isnull().sum().sum()
print("Number of fields from migrant_df with NaN data:", num_nan_migrant)
migrant_df["Sponsor Category"] = migrant_df["Sponsor Category"].fillna(4)
num_na_migrant_after_fill = migrant_df.isnull().sum().sum()
print("Number of rows with NaN data after filling Sponsor Category:", num_na_migrant_after_fill)
nan_rows = migrant_df.isna().any(axis=1)
migrant_nan_df = migrant_df[nan_rows]
# print(migrant_nan_df.to_string())
data_preserved = round(100*(original_len_migrant_df-len(migrant_nan_df))/original_len_migrant_df,4)
print("Percent of data preserved after removing rows with Zipcode NaN values: " + str(data_preserved) + "%")
migrant_df = migrant_df.dropna()
print("migrant_df length after NaN removal:", len(migrant_df))

# Changed Sponsor Category dtype to integer
migrant_df["Sponsor Category"] = migrant_df["Sponsor Category"].astype('int64')
# print(migrant_df["Sponsor Category"].unique())
migrant_df.rename(columns={"Sponsor Zipcode": "zip"}, inplace=True)

print(migrant_df.dtypes)

print("\n\n")


# =====================================================================================
# PREPARATION OF ZIPCODE/STATE/COUNTY CSV FOR MERGING

dtype_dict2 = {"zip": 'string',
               "state": 'string',
               "county": 'string'
               }
zip_df = pd.read_csv('zip_code_database.csv', dtype=dtype_dict2)
zip_backup = zip_df.copy()
# print(zip_df.dtypes)
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
print(zip_df.dtypes)

# Explored NaN values for zip_df
# All were related to missing counties and none of the zips appeared in our migrant_df
num_na_zip = zip_df.isnull().sum().sum()
print("Number of fields from zip_df with NaN data:", num_na_zip)
# nan_rows_zip = zip_df.isna().any(axis=1)
# zip_nan_df = zip_df[nan_rows_zip]

print("\n\n")


# =====================================================================================
# MERGE OF MIGRANT DATA WITH ZIP/COUNTY DATA

#Merged migrant_df and zip_df with a left join
mig_county_merge = pd.merge(migrant_df,zip_df,on='zip',how='left')
# print(mig_county_merge)

#Filtered out observations that involved releases outside of the 50 states plus DC
before_state_filter = len(mig_county_merge)
print("Length of mig_county_merge before filtering out territories and military abbreviations:", len(mig_county_merge))
state_codes_removed = ['PR', 'VI', 'AE', 'AA', 'AP', 'AS', 'GU', 'PW', 'AS', 'FM', 'MH', 'MP']
mig_county_merge = mig_county_merge[mig_county_merge.state.isin(state_codes_removed) == False]
after_state_filter = len(mig_county_merge)
print("Length of mig_county_merge after filtering out territories and military abbreviations:", len(mig_county_merge))
print("Number of observations removed due to release location outside of 50 US states plus DC:", (before_state_filter - after_state_filter))


# Explored Nan values in this merged dataset
# All were related to zip codes in the migrant_df that didn't have a county and state values
# num_na_mig_county = mig_county_merge.isnull().values.ravel().sum()
# print("Number of rows from mig_county_merge with NaN data:", num_na_mig_county)
# mig_county_merge = mig_county_merge[mig_county_merge["zip"] != "-"]
# num_na_mig_county = mig_county_merge.isnull().sum().sum()
# print("Number of rows from mig_county_merge with NaN after removing zip=='-':",
#       num_na_mig_county)
# mig_county_merge = mig_county_merge[mig_county_merge["zip"] != "00000"]
# num_na_mig_county = mig_county_merge.isnull().sum().sum()
# print("Number of rows from mig_county_merge with NaN after removing zip=='00000':", 
#       num_na_mig_county)
# mig_county_merge = mig_county_merge[mig_county_merge["zip"] != "0000"]
# num_na_mig_county = mig_county_merge.isnull().sum().sum()
# print("Number of rows from mig_county_merge with NaN after removing zip=='0000':", 
#       num_na_mig_county)

# Determined the number of rows affected by the migrant release zipcodes without state/county data
nan_rows_mig_county = mig_county_merge.isna().any(axis=1)
mig_county_nan_df = mig_county_merge[nan_rows_mig_county]
num_merge_obs_nan = len(mig_county_nan_df)
print("Number of mig_county_merge observations with NaN values:", num_merge_obs_nan)
# weird_zip = mig_county_nan_df["zip"].unique()
# print(*weird_zip)
# print(len(weird_zip))
# print(mig_county_nan_df.iloc[0:50])
print("Number of observations after removing Zipcode NaN values: " + str(len(mig_county_merge)-len(mig_county_nan_df)))

#Removed rows without state/county data
mig_county_merge = mig_county_merge.dropna()

#Assembled final observations
final_merge_length = len(mig_county_merge)
num_observations_removed = original_len_migrant_df-final_merge_length
print("Total number of observations removed:", num_observations_removed)
data_preserved_merge = round(100*(original_len_migrant_df-num_observations_removed)/original_len_migrant_df,4)
print("Percent of migrant data preserved: " + str(data_preserved_merge) + "%")

num_na_merge = mig_county_merge.isnull().sum().sum()
print("Number of fields from final_merge with NaN values:", num_na_merge)

print(mig_county_merge)
