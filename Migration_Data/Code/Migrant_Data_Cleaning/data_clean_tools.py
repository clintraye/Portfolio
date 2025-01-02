# Given a string of data representing a single county's data, returns a structured dictionary
import numpy as np
import pandas as pd
import re
import json

def createCsv(stateAcronym):
    nytimes_path = "..//Data//migrant_release_data.csv"
    nytimes = pd.read_csv(nytimes_path)  # release year, month are int type
    nytimes = nytimes.loc[nytimes["state"] == stateAcronym]
    nytimes.drop("Unnamed: 0", inplace=True, axis=1)
    nytimes.set_index(['state','county', 'release_year', 'release_month'], inplace=True)
    nytimes.index.names = ['state','county', 'year', 'month']

    stateAcronym = stateAcronym.upper()
    statePath = "..//Data//" + stateAcronym.upper() + "_county_data"

    # read BLS data
    with open(statePath, "r") as f:
        stateStr = f.read()

    # perform regex match for each county
    pattern = "(Series Id[\w\W\s\S]*?)Preliminary."
    patternObj = re.compile(pattern)
    matches = patternObj.findall(stateStr)

    tabularDf = pd.DataFrame(columns=['state', 'county', 'year', 'month', 'labor_force', 'employment', 'unemployment', 'unemployment_rate'])
    monthMap = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, \
                "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
    for countyStr in matches:
        county = re.search("Area:[\s]*(.*)", countyStr).group(1)
        county = county.split(",")[0]

        # get monthly data
        dataPattern = "(Year,Period[\w\W\s\S]*)\\n2023,Jun"
        patternObj = re.compile(dataPattern)
        matches = patternObj.findall(countyStr)
        matchesList = matches[0].split("\n")

        header = matchesList.pop(0).split(",")
        for row in matchesList:
            values = row.split(",")
            values[0] = int(values[0])
            values[1] = monthMap[values[1]]
            tabularDf.loc[len(tabularDf)] = [stateAcronym, county] + values

    tabularDf.set_index(['state', 'county', 'year', 'month'], inplace=True)
    mergedData = tabularDf.join(nytimes, on=['state', 'county', 'year', 'month'], how='left')
    print("Number of migrants added to CSV file for " + stateAcronym + ":", str(mergedData.loc[mergedData['ID'].notnull()].shape[0]))

    mergedData.to_csv("..//Data//" + stateAcronym + "_county_table.csv")

def createJson(stateAcronym):
    '''
    :param stateAcronym: State acronym (ex. "IA" for Iowa)
    :return:
    '''
    stateAcronym = stateAcronym.upper()
    statePath = "..//Data//" + stateAcronym.upper() + "_county_data"
    nytimesPath = "..//Data//migrant_release_data.csv"

    # read BLS data
    with open(statePath, "r") as f:
        stateStr = f.read()

    # clean nytime data, create release_year and release_month columns, filter for state
    monthMap = {"1":"Jan", "2":"Feb", "3":"Mar", "4":"Apr", "5":"May", "6":"Jun",\
                "7":"Jul", "8":"Aug", "9":"Sep", "10":"Oct", "11":"Nov", "12":"Dec"}
    nytimes = pd.read_csv(nytimesPath)
    nytimes.drop("Unnamed: 0", inplace=True, axis=1)
    nytimes = nytimes.loc[ nytimes["state"] == stateAcronym ]
    #nytimes[["release_year", "release_month"]] = nytimes.release_year_month.str.split("-",expand=True)
    nytimes["release_year"] = nytimes["release_year"].apply(pd.to_numeric)
    nytimes["release_month"] = nytimes["release_month"].apply(lambda x: monthMap[str(x)])

    #perform regex match for each county
    pattern = "(Series Id[\w\W\s\S]*?)Preliminary."
    patternObj = re.compile(pattern)
    matches = patternObj.findall(stateStr)

    # each element in list is a dictionary representing a single county
    countyData = []
    for county in matches:
        countyData.append(parseCounty(county, nytimes))

    migrantCt = 0
    for county in countyData:
        for year in county["annual"]:
            for month in year["monthly"]:
                migrantCt += len(month["migrants"])

    # Output Data Validation
    print("Actual number of migrants in NYTimes data for " + stateAcronym + ":", nytimes.loc[(nytimes["state"] == stateAcronym)].shape[0])
    print("Number of migrants added to json file for " + stateAcronym + ":", migrantCt)

    # write to json file
    outputPath = "..//Data//" + stateAcronym + "_county_data.json"
    with open(outputPath, "w") as fw:
        json.dump(countyData, fw)

def parseCounty(countyStr, nytimesState):
    # get county name
    county = re.search("Area:[\s]*(.*)", countyStr).group(1)
    county = county.split(",")[0]

    # get monthly data
    dataPattern = "(Year,Period[\w\W\s\S]*)\\n2023,Jun"
    patternObj = re.compile(dataPattern)
    matches = patternObj.findall(countyStr)
    matchesList = matches[0].split("\n")

    # populate dictionary to return
    result = dict()
    header = matchesList.pop(0).split(",")
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    result["county"] = county.split(",")[0]
    result["annual"] = [{"year": y, "monthly": [{"month": m} for m in months if not ((y == 2023) & (m in months[5:]))]} for y in range(2015, 2024)]
    for value in matchesList:
        data = value.split(",")
        year = int(data[0])
        month = data[1]
        for annualDict in result["annual"]:
            if annualDict["year"] == year:
                for monthlyDict in annualDict["monthly"]:
                    if monthlyDict["month"] == month:
                        monthlyDict.update({h: d for h, d in zip(header[-4:], data[-4:])})
                        monthlyDict["migrants"] = getMigrants(nytimesState, county, year, month)
    return result



def getMigrants(df, county, year, month):
    '''
    :param df: panda dataframe with nytimes data already filtered for state
    :param county: county name in string (ex. "Polk County")
    :param year: year in string (ex. "2015")
    :param month: first three letters of a month in string (ex. "Jan")
    :return: array of dictionaries, with each dictionary containing data for a single released migrant
    '''

    dfFiltered = df[ (df["county"] == county) & (df["release_year"] == year) & (df["release_month"] == month)]
    result = dfFiltered.to_dict("records")
    return result