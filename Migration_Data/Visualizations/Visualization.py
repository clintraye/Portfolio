from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import os
import json
import numpy as np
from pathlib import Path

root_dir = Path(__file__).parents[1]

states_df = pd.read_csv(os.path.join(
    root_dir, 'Data\\Map_Data\\States.csv'))
counties_df = pd.read_csv(os.path.join(
    root_dir, 'Data\\Map_Data\\Counties.csv'), dtype={"fips": str})

migrants_df = pd.read_csv(os.path.join(
    root_dir, 'Data\\Migrant_Data\\Migrant_Data.csv'))
migrants_df["date"] = pd.to_datetime(
    migrants_df["Child's Date of Release"], format="%Y-%m-%d").apply(lambda dt: dt.replace(day=1))

unemployment_df = pd.read_csv(os.path.join(
    root_dir, 'Data\\Unemployment_Data\\Unemployment_Data_By_County.csv'))
unemployment_df['date'] = pd.to_datetime(
    unemployment_df['month']+unemployment_df['year'].astype(str), format='%B%Y').apply(lambda dt: dt.replace(day=1))

county_corr_df = pd.DataFrame()

with open(os.path.join(root_dir, 'Visualizations\\counties.geojson')) as f:
    counties_geojson = json.load(f)
sponsor_types = {"All": 0, "Parent": 1, "Sibling": 2,
                 "Other Family/Unrelated Sponsor": 3, "Other": 4}
genders = ["All", "Male", "Female"]
times_dict = {i: time for i, time in enumerate(pd.date_range(
    '2015-01-01', '2023-05-31', freq='MS').strftime("%b-%y").tolist())}

app = Dash(__name__)
app.title = 'Unemployment Rates vs. Unaccompanied Migrant Placement'

app.layout = html.Div([
    html.H1('Unemployment Rates vs. Unaccompanied Migrant Placement',
            style={"text-align": "center"}),
    html.H2('Georgia Tech CSE 6242: Team 080',
            style={"text-align": "center"}),
    html.H3('Narges Bassirzadeh, Benjamin V Falby, MinYoung Kim, Guysnove M Lutumba, Vakula Mallapally, and Clinton R Raye ',
            style={"text-align": "center"}),
    html.P("This visualization highlights child migrant labor in the U.S., as detailed by recent investigations (Murray et al., 2023). \
            Utilizing Health & Human Services (HHS) data secured by the New York Times and data from the US Bureau of Labor Services, \
           the correlation between child migrant release destinations/dates and local unemployment rates is presented.",
           style={"text-align": "center"}),
    html.P("Filter By State:"),
    dcc.Dropdown(id='state_dropdown', options=list(
        states_df['name'].values), value=states_df['name'].iloc[0]),
    html.P("County:"),
    dcc.Dropdown(id='county_dropdown', options=[]),
    html.P("Sponsor Type:"),
    dcc.Dropdown(id='sponsor_dropdown', options=list(
        sponsor_types.keys()), value="All"),
    html.P("Gender:"),
    dcc.Dropdown(id='gender_dropdown', options=genders, value="All"),
    html.P("Release Date:"),
    dcc.RangeSlider(id='timeslider', min=0, max=max(list(times_dict.keys())),  marks={key: times_dict[key] for key in times_dict.keys(
    ) if key % 4 == 0}, allowCross=False, step=1, value=[0, max(list(times_dict.keys()))]),
    html.P(children="Processing Status: Incomplete", id="processing_status"),
    dcc.Graph(id="map"),
    dcc.Graph(id="graph")
])


@app.callback(
    Output("processing_status", "children"),
    Input("state_dropdown", "value"),
    Input('county_dropdown', 'value'),
    Input('sponsor_dropdown', 'value'),
    Input('gender_dropdown', 'value'),
    Input('timeslider', 'value')
)
def run_correlation(state, county, sponsor, gender, times):
    global county_corr_df
    try:
        times = [pd.to_datetime(times_dict[x], format="%b-%y") for x in times]
        filtered_migrant_df = migrants_df.copy()
        # Filter the migrant data based on inputs
        if state != "All":
            state_code = states_df[states_df['name'] == state].iloc[0].state
            filtered_migrant_df = filtered_migrant_df[filtered_migrant_df['state'] == state_code]
        if county != "All":
            filtered_migrant_df = filtered_migrant_df[filtered_migrant_df['county'] == county]
        if sponsor != "All":
            filtered_migrant_df = filtered_migrant_df[filtered_migrant_df['Sponsor Category']
                                                      == sponsor_types[sponsor]]
        if gender != "All":
            filtered_migrant_df = filtered_migrant_df[filtered_migrant_df["Child's Gender"] == gender[0]]
        filtered_migrant_df = filtered_migrant_df[filtered_migrant_df["date"] >= times[0]]
        filtered_migrant_df = filtered_migrant_df[filtered_migrant_df["date"] <= times[1]]

        migrant_count_df = filtered_migrant_df.groupby(
            ["date", "state", "county"]).size().reset_index(name='count')

        filtered_unemployment_df = unemployment_df.copy()
        filtered_unemployment_df = filtered_unemployment_df[
            filtered_unemployment_df["date"] >= times[0]]
        filtered_unemployment_df = filtered_unemployment_df[
            filtered_unemployment_df["date"] <= times[1]]

        merged_df = filtered_unemployment_df.merge(migrant_count_df, how='inner', on=[
            'date', 'state', 'county'])

        def p_corr(x, y):
            return (((x-x.mean()) * (y-y.mean())).sum()
                    / np.sqrt((((x-x.mean())**2).sum() * ((y-y.mean())**2).sum())))

        merged_df['count'] = merged_df['count'].astype(float)
        merged_df['unemployment_rate'] = merged_df['unemployment_rate'].astype(
            float)

        corr_df = merged_df.groupby(['state', 'county'])[['count', 'unemployment_rate']].apply(lambda s: pd.Series({
            "Pearson Coefficient": p_corr(s["count"], s["unemployment_rate"]),
        }))

        corr_df.fillna(0, inplace=True)
        corr_df = corr_df.reset_index()

        county_corr_df = corr_df.merge(
            counties_df, how='left', on=['state', 'county'])
        county_corr_df['Location'] = county_corr_df['state'] + ' - ' + \
            county_corr_df['county']
        county_corr_df.sort_values(
            'Pearson Coefficient', inplace=True, ascending=False)

        return "Processing Status: Complete"
    except Exception as e:
        print(e)
        return "Processing Status: Error"


@ app.callback(
    Output('county_dropdown', 'options'),
    Output('county_dropdown', 'value'),
    Input("state_dropdown", "value"),
)
def update_county_dropdown(state):
    state_code = states_df[states_df['name'] == state].iloc[0].state
    counties = list(counties_df[counties_df['state'].isin(
        [state_code, "USA"])]['county'].values)
    return counties, counties[0]


@ app.callback(
    Output("map", "figure"),
    Input("processing_status", "children"))
def display_choropleth(status):
    if status.split(':')[-1].strip() != "Complete":
        return None
    colorscale = ["rgb(255, 0, 0)", "rgb(255, 255, 255)", "rgb(0, 0, 255)"]
    fig = px.choropleth(county_corr_df, geojson=counties_geojson,
                        locations='fips', color='Pearson Coefficient', scope="usa", hover_data=['Location', 'Pearson Coefficient'],
                        color_continuous_scale=colorscale,
                        color_continuous_midpoint=0)
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0,
                      "b": 0}, width=1700, height=900)
    return fig


@app.callback(
    Output('graph', 'figure'),
    Input("processing_status", "children"),
    Input('state_dropdown', 'value')
)
def update_graph(status, state):
    if status.split(':')[-1].strip() != "Complete":
        return None
    # Filter the DataFrame for the selected state and create the figure
    colorscale = ["rgb(255, 0, 0)", "rgb(255, 255, 255)", "rgb(0, 0, 255)"]
    fig = px.bar(
        county_corr_df,
        x='Pearson Coefficient',
        y='Location',
        title=f'Pearson Correlation in {state}',
        color='Pearson Coefficient',
        color_continuous_scale=colorscale,
        color_continuous_midpoint=0
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0,
                      "b": 0}, width=1700, height=5 + 30 * county_corr_df.shape[0])
    return fig


app.run_server(debug=True)
