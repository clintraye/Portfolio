import requests
import json
import pandas
import re
import os

# Create all necessary series IDs to request from BLS server
# Series IDs will be of the format:
# LA: Local area unemployment
# U: Seasonally unadjusted
# F: Counties and equivalents
# 03: Unemployment rate
# As an example: LAUCN010010000000003 requests the unemployment rates for Autauga County, AL
# A listing of all available area codes (as downloaded from BLS website) is stored at ./Area_Codes.txt

# Loop through all 'counties and equivalents' and pull the code, name, and state
areas = []
with open(os.path.join(os.getcwd(), 'Code//Unemployment_Data_Gathering//Area_Codes.txt'), 'r') as f:
    for line in f:
        if line[0] == 'F':
            split = re.split(',', line)
            # For some cool reason DC is listed as a county or equivalent area and does not have an associated state code
            try:
                state = re.split('\W', split[1])[1]
            except:
                state = "District of Columbia"
            split2 = re.split('\W', split[0])
            area_code = split2[1]
            area_name = ' '.join(split2[2:])
            areas.append(('LAU' + area_code + '03', area_name, state))


rows = []
for i in range(len(areas)//50 + 1):
    print([x[1:] for x in areas[i*50: (i+1)*50]])
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": [x[0] for x in areas[i*50: (i+1)*50]], "startyear": "2015",
                      "endyear": "2023", 'registrationkey': '3b267a9cd74948a099910204de458756'})
    p = requests.post(
        'https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    json_data = json.loads(p.text)
    for j, series in enumerate(json_data['Results']['series']):
        for item in series['data']:
            year = item['year']
            period = item['periodName']
            value = item['value']
            state = areas[j+(i*50)][2]
            county = areas[j+(i*50)][1]
            rows.append([state, county, year, period, value])
x = pandas.DataFrame(
    columns=["state", "county", "year", "month", "unemployment_rate",], data=rows)
x.to_csv(os.path.join(os.getcwd(),
         'Data/Unemployment_Data/Unemployment_Data_By_County.csv'), index=False)
