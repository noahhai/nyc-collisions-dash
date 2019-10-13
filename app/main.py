import os
import dash
import dash_core_components as dcc
import dash_daq as daq
import dash_html_components as html
import pandas as pd
import numpy as np
import math
from sqlalchemy import create_engine

from dash.dependencies import Input, Output
from plotly import graph_objs as go
from datetime import datetime as dt

def get_engine():
    return create_engine(
        'mysql+mysqlconnector://' + os.environ['DB_USER'] + ':' + os.environ['DB_PASSWORD'] + '@' + os.environ[
            'DB_HOST'] + ':' + os.environ['DB_PORT'] + '/' +  os.environ['DB_DATABASE'] , echo=False)

def distance_lat_lon_to_km(lat1, lon1, lat2, lon2):
    # approximate radius of earth in km
    R = 6373.0

    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server
engine = get_engine()


mapbox_access_token = "pk.eyJ1IjoicGxvdGx5bWFwYm94IiwiYSI6ImNqdnBvNDMyaTAxYzkzeW5ubWdpZ2VjbmMifQ.TXcBE-xg9BFdV2ocecc_7g"

print('loading station data')
stations_df = pd.read_sql("SELECT id, stationName, latitude as lat, longitude as lon FROM station", engine)
print('loaded %i stations'%len(stations_df))

print('loading incident data')
df = pd.read_sql('''SELECT latitude lat, longitude lon, date, (number_of_persons_injured + number_of_persons_killed*5) severity,
 number_of_persons_injured, number_of_persons_killed from incident
 ''', engine)
print('loaded %i incidents'%len(df))
df["desc"] = "Severity: " + df["severity"].astype(str) + "<br>Killed: " + \
             df["number_of_persons_killed"].astype(str) + "<br>Injured: " + df["number_of_persons_injured"].astype(str)
del df["number_of_persons_injured"]
del df["number_of_persons_killed"]
df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
df.index = df["date"]
totalList = []
totalCount = len(df)
for month in df.groupby(df.index.month):
    dailyList = []
    for day in month[1].groupby(month[1].index.day):
        dailyList.append(day[1])
    totalList.append(dailyList)

totalList = np.array(totalList)
print('incident data arranged')

# Calculate closest accident to station
closest_stations = {}
station_count = len(stations_df)
station_missing_closest = 0
lon_lat_search_grid = 0.001
for i, s in stations_df.iterrows():
    if i%100==0 or i==station_count-1:
        print('searching for closest incident to station %i of %i'%(i+1, station_count))
    # approximation, only search incidents within ~150m from dock. too long otherwise.
    # TODO : calculate this using per-day dataset upon filter and/or pre-calculate during db loading stage
    filter = (df['lat'] < lon_lat_search_grid + s['lat']) & (df['lat'] > -lon_lat_search_grid+ s['lat']) &\
             (df['lon'] < lon_lat_search_grid + s['lon']) & (df['lon'] > -lon_lat_search_grid + s['lon'])
    found = False
    for _, i in df[filter].iterrows():
        found = True
        c = closest_stations.get(s['id'], None)
        if c:
            # shortcut
            if abs(i['lat'] - s['lat']) > c['lat_diff'] and abs(i['lon'] - s['lon']) > c['lon_diff']:
                continue
            elif math.sqrt(math.pow(i['lat'] - s['lat'], 2) + math.pow(i['lon'] - s['lon'], 2)) > c['dist']:
                continue
        closest_stations[s['id']] = {'lat': i['lat'], 'lon': i['lon'],
                                    'lat_diff': i['lat'] - s['lat'], 'lon_diff': i['lon'] - s['lon'],
                                    'dist': math.sqrt(math.pow(i['lat'] - s['lat'], 2) + math.pow(i['lon'] - s['lon'], 2))
                                  }
    if not found:
        station_missing_closest +=1

print('found closest incidents for %i of %i stations using lon_lat_search_grid="%f"'
      '.increase param to lower missing closest incident measurements.'%
      (station_count-station_missing_closest, station_count, lon_lat_search_grid))

stations_df["desc"] = "Bikeshare: " + stations_df["stationName"].astype(str) + \
                           ["<br>Closest incident: > ~156m" if not closest_stations.get(station['id'],None) else "<br>Closest incident: " + \
                                '{:.0f}m'.format(distance_lat_lon_to_km(station["lat"], station['lon'],
                               closest_stations[station['id']]['lat'], closest_stations[station['id']]['lon'])*1000.0) for _, station in stations_df.iterrows()]

print('data ready. starting dash.')

app.layout = html.Div(
    children=[
        html.Div(
            className="row",
            children=[
                html.Div(
                    className="two columns div-user-controls",
                    children=[
                        html.H2("Motor Vehicle Collisions in New York City"),
                        html.P(
                            """Filter data by date. Unselect to see all data."""
                        ),
                        html.Div(
                            className="div-for-filter-bool",
                            children=[
                                daq.BooleanSwitch(
                                    id="filter-bool",
                                    on=True
                                )
                            ],
                        ),
                        html.Div(
                            id="date-picker-div",
                            className="div-for-dropdown",
                            hidden=False,
                            children=[
                                html.P(
                                    """Select a date to see a subset of the data. Turn off filter to see complete set."""
                                ),
                                dcc.DatePickerSingle(
                                    id="date-picker",
                                    min_date_allowed=dt(2012, 7, 1),
                                    max_date_allowed=dt(2019, 10, 8),
                                    initial_visible_month=dt(2019, 10, 1),
                                    date=dt(2019, 10, 1).date(),
                                    display_format="MMMM D, YYYY",
                                    style={"border": "0px solid black"},
                                )
                            ],
                        ),
                        # Change to side-by-side for mobile layout
                        html.Div(
                            className="row",
                            children=[
                            ],
                        ),
                        html.P(id="total-incidents"),
                        html.P(id="date-value"),
                        dcc.Markdown(
                            children=[
                                "Data Source: [NYC OpenData](https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions-Crashes/h9gi-nx95)",
                            ]
                        ),
                        dcc.Markdown(
                            children=[
                                "Template Credit: [Plotly](https://github.com/plotly/dash-sample-apps/tree/master/apps/dash-uber-rides-demo)"
                            ]
                        ),
                    ],
                ),
                html.Div(
                    className="ten columns div-for-charts bg-grey",
                    children=[
                        dcc.Graph(id="map-graph"),
                    ],
                ),
            ],
        )
    ]
)

@app.callback(Output("total-incidents", "children"),
              [Input("date-picker", "date"), Input('filter-bool', 'on')])
def update_total_incidents(datePicked, filterByDate):
    date_picked = dt.strptime(datePicked, "%Y-%m-%d")
    return "Number of incidents {}: {:,d}".format(
        "for current date" if filterByDate else "",
        totalCount if not filterByDate else len(totalList[date_picked.month][date_picked.day])
    )

@app.callback(
    dash.dependencies.Output('date-picker-div', 'hidden'),
    [dash.dependencies.Input('filter-bool', 'on')])
def update_show_date_picker(on):
    return not on


def getLatLonColor( month, day):
    listCoords = totalList[month][day]
    return listCoords

# Update based on date-picker if filter-bool enabled
@app.callback(
    Output("map-graph", "figure"),
    [
        Input("date-picker", "date"),
        Input('filter-bool', 'on'),
    ],
)
def update_graph(datePicked, filterByDate):
    zoom = 12.0
    latInitial = 40.7272
    lonInitial = -73.991251
    bearing = 0

    date_picked = dt.strptime(datePicked, "%Y-%m-%d")
    monthPicked = date_picked.month
    dayPicked = date_picked.day
    if filterByDate:
        listCoords = getLatLonColor(monthPicked, dayPicked)
    else:
        listCoords = df

    return go.Figure(
        data=[
            # Plot all incidents
            go.Scattermapbox(
                lat=listCoords["lat"],
                lon=listCoords["lon"],
                mode="markers",
                hoverinfo="lat+lon+text",
                text=listCoords["desc"],
                marker=dict(
                    showscale=True,
                    color=listCoords['severity'],
                    opacity=0.5,
                    size=5+2*(listCoords['severity']),
                    colorscale=[
                        "#F4EC15",
                        "#DAF017",
                        "#BBEC19",
                        "#9DE81B",
                        "#80E41D",
                        "#66E01F",
                        "#4CDC20",
                        "#34D822",
                        "#24D249",
                        "#25D042",
                        "#26CC58",
                        "#28C86D",
                        "#29C481",
                        "#2AC093",
                        "#2BBCA4",
                        "#613099"
                    ],
                    colorbar=dict(
                        title="Severity<br>Injured + 5*Killed",
                        x=0.93,
                        xpad=0,
                        nticks=24,
                        tickfont=dict(color="#d8d8d8"),
                        titlefont=dict(color="#d8d8d8"),
                        thicknessmode="pixels",
                    ),
                ),
            ),
            # Plot bike shares
            go.Scattermapbox(
                lat=stations_df["lat"],
                lon=stations_df["lon"],
                mode="markers",
                text=stations_df["desc"],
                hoverinfo="text",
                marker=dict(size=5, color="#ffa0a0"),
            ),
        ],
        layout=go.Layout(
            autosize=True,
            margin=go.layout.Margin(l=0, r=35, t=0, b=0),
            showlegend=False,
            mapbox=dict(
                accesstoken=mapbox_access_token,
                center=dict(lat=latInitial, lon=lonInitial),  # 40.7272  # -73.991251
                style="dark",
                bearing=bearing,
                zoom=zoom,
            ),
            updatemenus=[
                dict(
                    buttons=(
                        [
                            dict(
                                args=[
                                    {
                                        "mapbox.zoom": 12,
                                        "mapbox.center.lon": "-73.991251",
                                        "mapbox.center.lat": "40.7272",
                                        "mapbox.bearing": 0,
                                        "mapbox.style": "dark",
                                    }
                                ],
                                label="Reset Zoom",
                                method="relayout",
                            )
                        ]
                    ),
                    direction="left",
                    pad={"r": 0, "t": 0, "b": 0, "l": 0},
                    showactive=False,
                    type="buttons",
                    x=0.45,
                    y=0.02,
                    xanchor="left",
                    yanchor="bottom",
                    bgcolor="#323130",
                    borderwidth=1,
                    bordercolor="#6d6d6d",
                    font=dict(color="#FFFFFF"),
                )
            ],
        ),
    )


if __name__ == "__main__":
    app.run_server(debug=False)