import numpy as np
import pandas as pd
import re
import json
from data_clean_tools import createJson, createCsv

# Steps to create cleaned data for improt into D3
# 1. Download state unemployment data from https://data.bls.gov/PDQWeb/la
#       - On website, select (1. <State>, 2. "F Counties and equivalents", 3. <highlight all areas>)
#       - Rename downloaded file as "<state acronym>_county_data" into "Data" project folder
# 2. Add two-letter acronyms of all the states you want to create data for to the "stateList" in code below
# 3. Run this script.
#    Resulting .json file saves as "<state acronym>_county_data.json", .csv files as "<state acronym>_county_data.json"
# 4. Validate correct number of migrants were added to json file by looking at console output.

stateList = ["IA"]
for state in stateList:
    createJson(state)
    createCsv(state)










