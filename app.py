# -*- coding: utf-8 -*-
"""
Created on Fri Nov 16 12:23:34 2018

@author: david
"""
import sys
import logging
import dash
from dash import dcc
from dash import html
from dash import dash_table
from dash.dependencies import Input, Output, State
from dash.long_callback import DiskcacheLongCallbackManager
from datetime import datetime, timedelta
import dash.dash_table.FormatTemplate as FormatTemplate

import pandas as pd
import numpy as np
import plotly
import plotly.graph_objs as go
import plotly.subplots as subplots
import cx_Oracle
import pytz
import time
import copy
import gunicorn                     #whilst your local machine's webserver doesn't need this, Heroku's linux webserver (i.e. dyno) does
from whitenoise import WhiteNoise   #for serving static files on Heroku

# import terality as pd
# import vaex
# from dateutil import tz
# from datetime_truncate import truncate
# import json
# from textwrap import dedent as d

# PyOWM library for weather data
from pyowm import OWM

# logging
# from manageProjectLogging import manageProjectLogging
# initialize the class
# logger = manageProjectLogging()
# construct message
# _message = '** construct your message here **'
# configure logger
# logger.configureProjectLogging(loggerName = 'myProjectLogger',
#                                loggerFile = 'myProjectLogs.log',
#                                loggingLevel = 'INFO',
#                                debugConfiguration = False)

# log message
# logger.logMessage(message = _message,
#                   loggingLevel = 'INFO',
#                   printBool = False)


from uuid import uuid4
import diskcache
launch_uid = uuid4()
cache = diskcache.Cache("./cache")
long_callback_manager = DiskcacheLongCallbackManager(
    cache, cache_by=[lambda: launch_uid], expire=60,
)
# cache = diskcache.Cache("./cache")
# long_callback_manager = DiskcacheLongCallbackManager(cache)


# Retrieve data from the dataabse for the runs
db_username = 'pepco'
db_password = 'password'
db_hostname = 'neo'
db = cx_Oracle.connect(db_username, db_password, db_hostname)
cur = db.cursor()
db2 = cx_Oracle.connect(db_username, db_password, db_hostname)
cur2 = db2.cursor()

# print versions
print('')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print('~~~~~~~~~                                                ~~~~~~~~~')
print('~~~~~~~~~             Entegrity Aggregation,             ~~~~~~~~~')
print('~~~~~~~~~        Settlement, and Research Solution       ~~~~~~~~~')
print('~~~~~~~~~                 Version 2.3.1                  ~~~~~~~~~')
print('~~~~~~~~~                                                ~~~~~~~~~')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print('')
print(datetime.now())
print('')
print('Python Version ' + str(sys.version))
print('Dash Version ' + dash.__version__)
print('Pandas Version ' , pd.__version__)
print('Numpy Version ' + np.__version__)
print('Plotly Version ' + plotly.__version__)
print('cx_Oracle Version ' + db.version)
print('')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')


# API_key = '3b479d420728699226006bb7ec8d1de5'
# owm = OWM(API_key)
# obs = owm.weather_at_place('Boston,MA,US')
# print (obs)
# loc = obs.get_location()
# print(loc)

# w = obs.get_weather()
# print('Current Weather')
# print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
# print(loc.get_name())
# print(loc.get_lon())
# print(loc.get_lat())
# print(loc.get_ID())
# print('')

# print('get_reference_time: ' + w.get_reference_time(timeformat='iso'))
# print('get_clouds: ' + str(w.get_clouds()))
# print('get_rain: ' + str(w.get_rain()))
# print('get_wind: ' + str(w.get_wind()))
# print('get_humidity: ' + str(w.get_humidity()))
# print('get_temperature: ' + str(w.get_temperature('fahrenheit')))
# print(w.get_weather_icon_url())
# print(w.get_sunrise_time('iso'))
# print(w.get_sunset_time('iso'))
# print('')


# print(w)

# ________________________________________________________________________________________________________________

apptitle = 'Entegrity Aggregation, Settlement, and Research Solution'

# set colors
darkcolor = '#555580'
mediumcolor = '#9494b8'
mediumlightcolor = '#b3b3cc'
lightcolor = '#f0f0f5'

runcolor = '#2c962c'  # green
publishcolor = '#2c2c96'  # blue
unpublishcolor = '#96962c'  # gold
deletecolor = '#954040'  # red

tabgradient = 'linear-gradient(to bottom, ' + darkcolor + ', black)'
selectedtabgradient = 'linear-gradient(to bottom, ' + '#954040' + ', black)'
paperbackgroundcolor = lightcolor
plotbackgroundcolor = lightcolor
legendbackgroundcolor = lightcolor

# Retrieve DATAAGGRUN records
dataaggrun_query = '''
select * from dataaggrun
 where dataaggrunid in
       (select dataaggrunid from dataaggrunhist
         where processname = 'DATA AGGREGATION' and status = 'COMPLETE' )
 order by dataaggrunid desc'''
dataaggrun = pd.read_sql(dataaggrun_query, con=db)
run_count = dataaggrun.shape[0]

top_n = 100
hours_in_day = 24

if run_count == 0:
    apptitle = 'Entegrity ASR'
elif run_count > 0:
    dataaggrunid = dataaggrun['DATAAGGRUNID'][0]
    # print(dataaggrunid)

    fmt = '%Y-%m-%d %H:%M:%S %Z%z'
    local_tz = pytz.timezone('US/Eastern')
    operatingdate = dataaggrun['OPERATINGDATE'][0]
    operatingdate_end = operatingdate + timedelta(days=1, seconds=-1)
    operatingdate_local = local_tz.localize(operatingdate)
    operatingdate_local_end = operatingdate_local + timedelta(days=1)

    dst_dates = pd.DataFrame([local_tz._utc_transition_times]).transpose()
    dst_dates.rename({0: "TRANSITION_DATE"}, axis='columns', inplace=True)
    dst_dates.drop(dst_dates.index[0], inplace=True)
    dst_dates['TRUNCATED'] = pd.to_datetime(dst_dates['TRANSITION_DATE'], errors='coerce').dt.normalize()
    dst_dates = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

    if dst_dates.shape[0] == 1:
        if dst_dates.iloc[0][1].month <= 5:
            hours_in_day = 23
        else:
            hours_in_day = 25
    else:
        hours_in_day = 24

    interval_cols = []
    for i in range(0, hours_in_day):
        interval_start = (operatingdate_local + timedelta(hours=i))
        interval_start_dst = interval_start.astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        interval_cols.append(interval_start_dst)
        col_name = f"T{i:0>3}"


alldataaggrun_query = 'select * from dataaggrun order by dataaggrunid desc'
alldataaggrun = pd.read_sql(alldataaggrun_query, con=db)

settlementtype_query = 'select * from settlementtype order by settlementtype'
settlementtype = pd.read_sql(settlementtype_query, con=db)

market_query = 'select * from MARKET order by MARKET'
market = pd.read_sql(market_query, con=db)


# ________________________________________________________________________________________________________________
# ________________________________________________________________________________________________________________


app = dash.Dash(__name__, title=apptitle, long_callback_manager=long_callback_manager)
#, plugins=[dl.plugins.FlexibleCallbacks()])
app.config['suppress_callback_exceptions'] = True
# app.run_server(debug=True)
# app.logger.debug('Test debug')

# Define the underlying flask app (Used by gunicorn webserver in Heroku production deployment)
server = app.server 

# Enable Whitenoise for serving static files from Heroku (the /static folder is seen as root by Heroku) 
server.wsgi_app = WhiteNoise(server.wsgi_app, root='static/') 

app.layout = html.Div([
    html.Div(html.Img(src='https://www.exeloncorp.com/PublishingImages/Lists/Operating%20Companies/AllItems/pepco_logo.png'),
             style={'width': '15%',
                    'display': 'inline-block',
                    'verticalAlign': 'middle'}),

    html.H2(children=apptitle,
            style={'textAlign': 'center',
                   'width': '70%',
                   'display': 'inline-block',
                   'verticalAlign': 'middle'}),

    html.Div(html.Img(src='https://static.wixstatic.com/media/d476b1_723a5d24fe25482c8034c37511d00e1e.jpg/v1/fill/w_600,h_77,al_c,q_80,usm_0.66_1.00_0.01/d476b1_723a5d24fe25482c8034c37511d00e1e.webp',
                      width='100%', height='100%', alt='Image size dimensions'),
             style={'width': '15%',
                    'display': 'inline-block',
                    'verticalAlign': 'middle'}),

    dcc.Tabs(id="tabs",
             value='run-evaluation',
             children=[dcc.Tab(label='Agg Data Checks', value='agg-data-checks', style={'backgroundImage': tabgradient, 'color': 'white'}, selected_style={'backgroundImage': selectedtabgradient, 'color': 'white'}),
                       dcc.Tab(label='Execute Aggregation', value='execute-agg', style={'backgroundImage': tabgradient, 'color': 'white'}, selected_style={'backgroundImage': selectedtabgradient, 'color': 'white'}),
                       dcc.Tab(label='Execute PLC', value='execute-plc', style={'backgroundImage': tabgradient, 'color': 'white'}, selected_style={'backgroundImage': selectedtabgradient, 'color': 'white'}),
                       dcc.Tab(label='Run Evaluation', value='run-evaluation', style={'backgroundImage': tabgradient, 'color': 'white'}, selected_style={'backgroundImage': selectedtabgradient, 'color': 'white'}),
                       dcc.Tab(label='Aggregated Data Reporting', value='aggregated-data', style={'backgroundImage': tabgradient, 'color': 'white'}, selected_style={'backgroundImage': selectedtabgradient, 'color': 'white'}),
                       dcc.Tab(label='Service Point Data', value='service-point-data', style={'backgroundImage': tabgradient, 'color': 'white'}, selected_style={'backgroundImage': selectedtabgradient, 'color': 'white'}),
                       dcc.Tab(label='Research', value='research', style={'backgroundImage': tabgradient, 'color': 'white'}, selected_style={'backgroundImage': selectedtabgradient, 'color': 'white'}),
                       dcc.Tab(label='Browse Data', value='browse-data', style={'backgroundImage': tabgradient, 'color': 'white'}, selected_style={'backgroundImage': selectedtabgradient, 'color': 'white'}),
                       ]),

    html.Div(id='tabs-content', style={'backgroundColor': lightcolor}),

    # memory stores
    dcc.Store(id='lls_store'),
    dcc.Store(id='servicepoint_store'),
    ])


# ________________________________________________________________________________________________________________
# ________________________________________________________________________________________________________________


@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    """Create the Agg Data Checks Tab."""
    if tab == 'agg-data-checks':
        #
        #   ___               ______      _          _____ _               _
        #  / _ \              |  _  \    | |        /  __ \ |             | |
        # / /_\ \ __ _  __ _  | | | |__ _| |_ __ _  | /  \/ |__   ___  ___| | _____
        # |  _  |/ _` |/ _` | | | | / _` | __/ _` | | |   | '_ \ / _ \/ __| |/ / __|
        # | | | | (_| | (_| | | |/ / (_| | || (_| | | \__/\ | | |  __/ (__|   <\__ \
        # \_| |_/\__, |\__, | |___/ \__,_|\__\__,_|  \____/_| |_|\___|\___|_|\_\___/
        #         __/ | __/ |
        #        |___/ |___/
        #
        # select date for operating data data verification

        return html.Div([

            html.Div([
                html.Div([
                    html.Label('Operating Date',),
                    dcc.DatePickerSingle(
                        id='check-operating-date-picker',
                        min_date_allowed=datetime(2010,  1,  1),
                        max_date_allowed=datetime(2099, 12, 31),
                        initial_visible_month=datetime.today().date() - timedelta(days=1),
                        date=datetime.today().date() - timedelta(days=1),
                        day_size=47,
                        persistence=True,
                    ),
                    
                ], style={'width': '8%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': 10}),
                          
                html.Div([
                    html.Button('Check Date', 
                               id='check-date-button',),
                ], style={'width': '10%',
                          'display': 'inline-block',
                          'verticalAlign': 'bottom',
                          'color': 'red',
                          'margin': '10px 10px 10px 5px',
                          }),

            ], style={'marginTop': 0,
                      'marginBottom': 5,
                      'backgroundColor': mediumcolor,
                      'borderTopWidth': 5,
                      'borderTopStyle': 'solid',
                      'borderTopColor': 'black',
                      'borderBottomWidth': 1,
                      'borderBottomStyle': 'solid',
                      'borderBottomColor': 'black', }),

            # html.H6('Operating Day Data Verification', style={'textAlign': 'center'}),

            dcc.Loading(
                id="loading-data-checks-for-operating-date",
                children=[html.Div(id='data-checks-for-operating-date')]),

        ], style={'backgroundColor': lightcolor}
        )

    """Create the Execute Agregation Tab."""
    if tab == 'execute-agg':
        #
        #  _____                    _          ___                                   _   _
        # |  ___|                  | |        / _ \                                 | | (_)
        # | |____  _____  ___ _   _| |_ ___  / /_\ \ __ _  __ _ _ __ ___  __ _  __ _| |_ _  ___  _ __
        # |  __\ \/ / _ \/ __| | | | __/ _ \ |  _  |/ _` |/ _` | '__/ _ \/ _` |/ _` | __| |/ _ \| '_ \
        # | |___>  <  __/ (__| |_| | ||  __/ | | | | (_| | (_| | | |  __/ (_| | (_| | |_| | (_) | | | |
        # \____/_/\_\___|\___|\__,_|\__\___| \_| |_/\__, |\__, |_|  \___|\__, |\__,_|\__|_|\___/|_| |_|
        #                                            __/ | __/ |          __/ |
        #                                           |___/ |___/          |___/

        # select dataaggruns to display in dropdown
        analyis_run_query = '''
        select dataaggrunid, to_char(dataaggrunid) || ' [' || to_char(operatingdate,'yyyy-mm-dd') || ' | ' || settlementtype || ' | ' || to_char(runnumber)|| ']' as displaystring
          from dataaggrun where dataaggrunid in (select distinct dataaggrunid from dataaggrunhist) order by inserttimestamp desc'''
        analysis_runs = pd.read_sql(analyis_run_query, con=db)

        run_count = analysis_runs.shape[0]

        dataaggrunid_to_zip = analysis_runs['DATAAGGRUNID']
        displaystring_to_zip = analysis_runs['DISPLAYSTRING']

        if run_count > 0:
            chosen_dataaggrunid = analysis_runs['DATAAGGRUNID'][0]
        else:
            chosen_dataaggrunid = None

        return html.Div([

            html.Div([
                # html.H6('Execute Run' ,style={'textAlign':'center'}),
                # dcc.Loading(id="loading-run-aggregation", children=[html.Div(id='tabs-content')], type="default"),

                html.Div([
                    html.Label('Market',),
                    dcc.Dropdown(
                        id='market-radio',
                        options=[{'label': i, 'value': i} for i in market['MARKET']],
                        value='PJM',
                        persistence=True
                    )
                ], style={'width': '10%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': 10}),

                html.Div([
                    html.Label('Operating Date',),
                    dcc.DatePickerSingle(
                        id='operating-date-picker',
                        min_date_allowed=datetime(2010,  1,  1),
                        max_date_allowed=datetime(2099, 12, 31),
                        initial_visible_month=datetime.today().date() - timedelta(days=1),
                        date=datetime.today().date() - timedelta(days=1),
                        day_size=47,
                        persistence=True,
                    )
                ], style={'width': '8%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': 10}),

                html.Div([
                    html.Label('Settlement Type',),
                    dcc.Dropdown(
                        id='settlementtype-radio',
                        options=[{'label': i, 'value': i} for i in settlementtype['SETTLEMENTTYPE']],
                        value='INITIAL',
                        persistence=True
                    )
                ], style={'width': '10%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': 10}),

                html.Div([
                    html.Label('Run Number',),
                    dcc.Input(
                            id='run-number',
                            value=1,
                            type='number',
                            min=0,
                            max=999999,
                            step=1,
                            persistence=True),
                ], style={'width': '7%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': 10}),

                html.Div([
                    html.Label('Days to Run',),
                    dcc.Input(
                            id='days-to-run',
                            value=1,
                            type='number',
                            min=1,
                            max=31,
                            step=1,
                            persistence=True),
                ], style={'width': '7%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': 10}),

                html.Div([
                    html.Label('Replace Existing Run',),
                    dcc.RadioItems(
                        id='replace-radio',
                        options=[{'label': i, 'value': i} for i in {'Y', 'N'}],
                        value='Y',
                        labelStyle={'display': 'inline-block'},
                        persistence=True
                    ),
                ], style={'width': '10%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': 10}),

                html.Div([
                    html.Label('Service Point Output',),
                    dcc.RadioItems(
                        id='service-point-output-radio',
                        options=[{'label': i, 'value': i} for i in {'Y', 'N'}],
                        value='N',
                        labelStyle={'display': 'inline-block'},
                        persistence=True
                    ),
                ], style={'width': '10%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': 10}),

                html.Div([
                    html.Button('Run Aggregation', 
                               id='run-aggregation-button',),
                ], style={'width': '10%',
                          'display': 'inline-block',
                          'verticalAlign': 'bottom',
                          'color': 'red',
                          'margin': '10px 10px 10px 5px',
                          }),

                html.Div(id='dataaggrun-for-operating-date'),

            ], style={'marginTop': 0,
                      'marginBottom': 5,
                      'backgroundColor': mediumcolor,
                      'borderTopWidth': 5,
                      'borderTopStyle': 'solid',
                      'borderTopColor': 'black',
                      'borderBottomWidth': 1,
                      'borderBottomStyle': 'solid',
                      'borderBottomColor': 'black', }),

            html.Div([html.Label('Data Agg Run ID'),
                      dcc.Dropdown(id='running-dataaggrunid-dropdown',
                                   options=[{'label': display_string, 'value': dataaggrunid} for display_string, dataaggrunid in zip(displaystring_to_zip, dataaggrunid_to_zip)],
                                   value=chosen_dataaggrunid,
                                   multi=False,
                                   persistence=True
                                   )
                      ], style={'margin': '10px 10px',
                                'width': '20%',
                                'display': 'inline-block'}),

            html.Div([
                html.Button('Delete', id='delete-run-button'),
                ], style={'width': '20%',
                          'display': 'inline-block',
                          'margin': '10px 10px',
                          'verticalAlign': 'bottom'}),

            # html.Div([html.Label('Page Refresh Rate'),
            #     dcc.Dropdown(id='page-refresh-rate',
            #                  options=[{'label': i, 'value': i} for i in [10,30,60,300,3600]],
            #                  value=30,
            #                  multi=False
            #                  )
            # ], style={'margin':'10px 10px',
            #           'width':'10%',
            #           'float':'right',
            #           'display':'inline-block'}),

            # html.H6('Data Agg Run ID' ,style={'textAlign':'center',}),
            html.Div(id='running_dataaggrunid'),

            dcc.Loading(
                id="loading-run-execution-delete",
                children=[
                    html.Div(
                        id='run-execution-delete',
                        style={'textAlign': 'left', 'margin': '20px 20 px', 'verticalAlign': 'middle', 'fontSize': 16, 'backgroundColor': 'rgba(200,100,100,.5)'}),
                ]),

            html.H6('Analysis Performance and Status', style={'textAlign': 'center'}),
            dcc.Loading(
                id="loading-dataaggrunhist-for-dataaggrunid",
                children=[html.Div(id='dataaggrunhist-for-dataaggrunid')]),

            dcc.Interval(
                id='interval-component',
                interval=30*1000,  # in milliseconds
                n_intervals=0
            ),
        ], style={'backgroundColor': lightcolor}
        )

    if tab == 'execute-plc':
        #  _____                    _        ______ _     _____
        # |  ___|                  | |       | ___ \ |   /  __ \
        # | |____  _____  ___ _   _| |_ ___  | |_/ / |   | /  \/
        # |  __\ \/ / _ \/ __| | | | __/ _ \ |  __/| |   | |
        # | |___>  <  __/ (__| |_| | ||  __/ | |   | |___| \__/\
        # \____/_/\_\___|\___|\__,_|\__\___| \_|   \_____/\____/
        #

        # select dataaggruns for PLC coincident peak days
        cp_query = '''
        select dataaggrunid, to_char(dataaggrunid) || ' [' || to_char(operatingdate,'yyyy-mm-dd') || ' | ' || settlementtype || ' | ' || to_char(runnumber)|| ']' as displaystring
          from dataaggrun where dataaggrunid in (select dataaggrunid from dataaggrunhist where processname = 'DATA AGGREGATION' and STATUS = 'COMPLETE' and SETTLEMENTTYPE = 'PLC' )
               and operatingdate in (select trunc(starttime) from parameterhistory where parameter ='PLC_CP' and extract(year from starttime) = extract(year from operatingdate))
         order by operatingdate desc'''
        coincident_peaks = pd.read_sql(cp_query, con=db)

        cp_run_count = coincident_peaks.shape[0]
        if cp_run_count == 0:
            return 'No completed runs were found for the coincident peak days identified in the parameter history table (parameter PLC_CP).'

        cp_dataaggrunid_to_zip = coincident_peaks['DATAAGGRUNID']
        cp_displaystring_to_zip = coincident_peaks['DISPLAYSTRING']

        ncp_query = '''
        select dataaggrunid, to_char(dataaggrunid) || ' [' || to_char(operatingdate,'yyyy-mm-dd') || ' | ' || settlementtype || ' | ' || to_char(runnumber)|| ']' as displaystring
          from dataaggrun where dataaggrunid in (select dataaggrunid from dataaggrunhist where processname = 'DATA AGGREGATION' and STATUS = 'COMPLETE' and SETTLEMENTTYPE = 'PLC' )
               and operatingdate in (select trunc(starttime) from parameterhistory where parameter ='PLC_NCP' and extract(year from starttime) = extract(year from operatingdate))
         order by operatingdate desc'''
        noncoincident_peaks = pd.read_sql(ncp_query, con=db)

        ncp_run_count = noncoincident_peaks.shape[0]
        if ncp_run_count == 0:
            return 'No completed runs were found for the non-coincident peak days identified in the parameter history table (parameter PLC_NCP).'

        ncp_dataaggrunid_to_zip = noncoincident_peaks['DATAAGGRUNID']
        ncp_displaystring_to_zip = noncoincident_peaks['DISPLAYSTRING']

        plc_dates_query = '''
        select parameter, starttime, numvalue from parameterhistory where parameter like 'PLC%' order by starttime desc, parameter'''
        plc_dates = pd.read_sql(plc_dates_query, con=db)

        plcforecast_query = '''select *
                                 from plcforecast
                                order by PLC desc
                                fetch first 1000 rows only'''
        plcforecast = pd.read_sql(plcforecast_query, con=db)

        plcforecast_rows = plcforecast.to_dict("rows")

        servicepointplchist_query = '''select *
                                 from servicepointplchist
                                order by starttime desc, PLC desc
                                fetch first 1000 rows only'''
        servicepointplchist = pd.read_sql(servicepointplchist_query, con=db)

        servicepointplchist_rows = servicepointplchist.to_dict("rows")

        return html.Div([

            html.Div([
                html.Div([
                    html.Label('Coincident Peak Run IDs'),
                    dcc.Dropdown(id='coincident-dropdown',
                                 options=[{'label': display_string, 'value': dataaggrunid} for display_string, dataaggrunid in zip(cp_displaystring_to_zip, cp_dataaggrunid_to_zip)],
                                 multi=True,
                                 persistence=True
                                 ),
                    ], style={'width': '30%',
                              'display': 'inline-block',
                              'margin': '10px 10px',
                              'verticalAlign': 'middle'}),

                html.Div([
                    html.Label('Non-Coincident Peak Run IDs'),
                    dcc.Dropdown(id='non-coincident-dropdown',
                                 options=[{'label': display_string, 'value': dataaggrunid} for display_string, dataaggrunid in zip(ncp_displaystring_to_zip, ncp_dataaggrunid_to_zip)],
                                 multi=True,
                                 persistence=True
                                 ),
                    ], style={'width': '30%',
                              'display': 'inline-block',
                              'margin': '10px 10px',
                              'verticalAlign': 'middle'}),

                html.Div([
                    html.Button('Run PLC', id='run-plc-forecast-button'),
                    # html.Button('Publish PLC', id='publish-plc-forecast-button'),
                    ], style={'width': '7%',
                              'display': 'inline-block',
                              'verticalAlign': 'bottom',
                              'margin': '10px 10px 10px 5px',
                              }),

                html.Div([
                    html.Label('PLC Effective Start',),
                    dcc.DatePickerSingle(
                        id='plc-effective-start-picker',
                        min_date_allowed=datetime(2010,  1,  1),
                        max_date_allowed=datetime(2099, 12, 31),
                        initial_visible_month=datetime.today().date() - timedelta(days=1),
                        date=datetime.today().date() - timedelta(days=1),
                        day_size=47,
                        persistence=True,
                    ),
                    ], style={#'width': '7%',
                              'display': 'inline-block',
                              'verticalAlign': 'bottom',
                              'horizontallign': 'right',
                              'margin': '10px 10px 10px 5px',
                              }),

                html.Div([
                    html.Label('PLC Effective Stop',),
                    dcc.DatePickerSingle(
                        id='plc-effective-stop-picker',
                        min_date_allowed=datetime(2010,  1,  1),
                        max_date_allowed=datetime(2099, 12, 31),
                        initial_visible_month=datetime.today().date() - timedelta(days=1),
                        date=datetime.today().date() - timedelta(days=1),
                        day_size=47,
                        persistence=True,
                    ),
                    ], style={#'width': '7%',
                              'display': 'inline-block',
                              'verticalAlign': 'bottom',
                              'horizontallign': 'right',
                              'margin': '10px 10px 10px 5px',
                              }),

                html.Div([
                    html.Button('Publish PLC', id='publish-plc-forecast-button'),
                    ], style={#'width': '5%',
                              'display': 'inline-block',
                              'verticalAlign': 'bottom',
                              'horizontallign': 'right',
                              'margin': '10px 10px 10px 5px',
                              }),



            ], style={'marginTop': 0,
                      'marginBottom': 5,
                      'backgroundColor': mediumcolor,
                      'borderTopWidth': 5,
                      'borderTopStyle': 'solid',
                      'borderTopColor': 'black',
                      'borderBottomWidth': 1,
                      'borderBottomStyle': 'solid',
                      'borderBottomColor': 'black', }),

            html.Div([
                html.H6('PLC Peak Dates', style={'textAlign': 'center'}),
                dash_table.DataTable(
                    id='plc-dates-table',
                    columns=[
                        {"name": i, "id": i} for i in plc_dates.columns
                    ],
                    data=plc_dates.to_dict("rows"),
                    editable=False,
                    filter_action='native',
                    sort_action='native',
                    style_as_list_view=True,
                    page_size=6,
                    style_cell={
                        # all three widths are needed
                        'minWidth': '120px',
                        'whiteSpace': 'no-wrap',
                        'overflow': 'hidden',
                        'textOverflow': 'ellipsis',
                    },
                    style_data={'backgroundColor': lightcolor,
                                'border': '0px solid white',
                                'fontColor': 'black',
                                'borderTop': '2px solid ' + darkcolor,
                                },
                    style_header={
                        'backgroundColor': lightcolor,
                        'fontWeight': 'bold',
                        'border': '0px solid white',
                        },
                    css=[{
                        'selector': '.dash-cell div.dash-cell-value',
                        'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                        }],
                ),
            ]),

            dcc.Loading(id="loading-PLC-top-servicepoints", children=[
                html.Div(
                    [html.H6('Top 1,000 Service Points by PLC', style={'textAlign': 'center'}),
                     dcc.Loading(id="loading-plc-forecast-table", children=[
                         dash_table.DataTable(
                             id='plc-forecast-table',
                             columns=[{"name": i, "id": i} for i in plcforecast.columns],
                                data=plcforecast_rows,
                                editable=True,
                                filter_action='native',
                                sort_action='native',
                                row_deletable=False,
                                style_table={'overflowX': 'scroll'},
                                style_as_list_view=True,
                                page_size=20,
                                style_cell={
                                    # all three widths are needed
                                    'minWidth': '120px',
                                    'whiteSpace': 'no-wrap',
                                    'overflow': 'hidden',
                                    'textOverflow': 'ellipsis',
                                    },
                                style_data={
                                    'backgroundColor': lightcolor,
                                    'border': '0px solid white',
                                    'fontColor': 'black',
                                    'borderTop': '2px solid ' + darkcolor,
                                    },
                                style_header={
                                    'backgroundColor': lightcolor,
                                    'fontWeight': 'bold',
                                    'border': '0px solid white',
                                    },
                                css=[{
                                    'selector': '.dash-cell div.dash-cell-value',
                                    'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                    }],
                            ),
                         ])
                     ])
                ]),
            
            dcc.Loading(id="loading-servicepointplchist", children=[
                html.Div(
                    [html.H6('Top 1,000 Service Point PLC History', style={'textAlign': 'center'}),
                     dcc.Loading(id="loading-servicepointplchist-table", children=[
                         dash_table.DataTable(
                             id='servicepointplchist-table',
                             columns=[{"name": i, "id": i} for i in servicepointplchist.columns],
                                data=servicepointplchist_rows,
                                editable=True,
                                filter_action='native',
                                sort_action='native',
                                row_deletable=False,
                                style_table={'overflowX': 'scroll'},
                                style_as_list_view=True,
                                page_size=20,
                                style_cell={
                                    # all three widths are needed
                                    'minWidth': '120px',
                                    'whiteSpace': 'no-wrap',
                                    'overflow': 'hidden',
                                    'textOverflow': 'ellipsis',
                                    },
                                style_data={
                                    'backgroundColor': lightcolor,
                                    'border': '0px solid white',
                                    'fontColor': 'black',
                                    'borderTop': '2px solid ' + darkcolor,
                                    },
                                style_header={
                                    'backgroundColor': lightcolor,
                                    'fontWeight': 'bold',
                                    'border': '0px solid white',
                                    },
                                css=[{
                                    'selector': '.dash-cell div.dash-cell-value',
                                    'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                    }],
                            ),
                         ])
                     ])
                ]),
        ], style={'backgroundColor': lightcolor,
                  })

    elif tab == 'run-evaluation':
        # ______              _____           _             _   _
        # | ___ \            |  ___|         | |           | | (_)
        # | |_/ /   _ _ __   | |____   ____ _| |_   _  __ _| |_ _  ___  _ __
        # |    / | | | '_ \  |  __\ \ / / _` | | | | |/ _` | __| |/ _ \| '_ \
        # | |\ \ |_| | | | | | |___\ V / (_| | | |_| | (_| | |_| | (_) | | | |
        # \_| \_\__,_|_| |_| \____/ \_/ \__,_|_|\__,_|\__,_|\__|_|\___/|_| |_|

        # select dataaggruns to display in dropdown
        analyis_run_query = '''
        select dataaggrunid, to_char(dataaggrunid) || ' [' || to_char(operatingdate,'yyyy-mm-dd') || ' | ' || settlementtype || ' | ' || to_char(runnumber)|| ']' as displaystring
          from dataaggrun
         where dataaggrunid in (select dataaggrunid from dataaggrunhist where processname = 'DATA AGGREGATION' and status = 'COMPLETE')
         order by inserttimestamp desc'''
        analysis_runs = pd.read_sql(analyis_run_query, con=db)

        run_count = analysis_runs.shape[0]

        dataaggrunid_to_zip = analysis_runs['DATAAGGRUNID']
        displaystring_to_zip = analysis_runs['DISPLAYSTRING']

        if run_count > 0:
            chosen_dataaggrunid = analysis_runs['DATAAGGRUNID'][0]
        else:
            chosen_dataaggrunid = None

        return html.Div([
            html.Div([
                dcc.Loading(id="loading-delete-button", children=[html.Div(id='delete-button')], type="default"),

                html.Div([
                    html.Label('Data Agg Run ID'),
                    dcc.Dropdown(id='analysis-run-dropdown',
                                 options=[{'label': display_string, 'value': dataaggrunid} for display_string, dataaggrunid in zip(displaystring_to_zip, dataaggrunid_to_zip)],
                                 value=chosen_dataaggrunid,
                                 persistence=True),
                    ], style={'width': '20%',
                              'display': 'inline-block',
                              'margin': '10px 10px',
                              'verticalAlign': 'middle'}),

                html.Div([
                    html.Button('Publish', id='publish-button'),
                    html.Button('Unpublish', id='unpublish-button'),
                    html.Button('Delete', id='delete-button'),
                    ], style={'width': '40%',
                              'display': 'inline-block',
                              'margin': '10px 10px 10px 5px',
                              'verticalAlign': 'bottom'}),
                ], style={'marginTop': 0,
                          'marginBottom': 5,
                          'backgroundColor': mediumcolor,
                          'borderTopWidth': 5,
                          'borderTopStyle': 'solid',
                          'borderTopColor': 'black',
                          'borderBottomWidth': 1,
                          'borderBottomStyle': 'solid',
                          'borderBottomColor': 'black', }),

            dcc.Loading(id="loading-run-evaluation-buttons", children=[
                html.Div(id='run-evaluation-publish', style={'textAlign': 'left', 'margin': '20px 20 px', 'verticalAlign': 'middle', 'fontSize': 16, 'backgroundColor': lightcolor}),
                html.Div(id='run-evaluation-unpublish', style={'textAlign': 'left', 'margin': '20px 20 px', 'verticalAlign': 'middle', 'fontSize': 16, 'backgroundColor': lightcolor}),
                html.Div(id='run-evaluation-delete', style={'textAlign': 'left', 'margin': '20px 20 px', 'verticalAlign': 'middle', 'fontSize': 16, 'backgroundColor': lightcolor}),
                ]),

            dcc.Loading(id="loading-run-evaluation-container", children=[
                html.Div(id='run-evaluation-container'),
                ])
        ], style={'backgroundColor': lightcolor})

    elif tab == 'aggregated-data':
        #   ___                                   _           _  ______      _
        #  / _ \                                 | |         | | |  _  \    | |
        # / /_\ \ __ _  __ _ _ __ ___  __ _  __ _| |_ ___  __| | | | | |__ _| |_ __ _
        # |  _  |/ _` |/ _` | '__/ _ \/ _` |/ _` | __/ _ \/ _` | | | | / _` | __/ _` |
        # | | | | (_| | (_| | | |  __/ (_| | (_| | ||  __/ (_| | | |/ / (_| | || (_| |
        # \_| |_/\__, |\__, |_|  \___|\__, |\__,_|\__\___|\__,_| |___/ \__,_|\__\__,_|
        #         __/ | __/ |          __/ |
        #        |___/ |___/          |___/

        # select dataaggruns to display in dropdown
        analyis_run_query = '''
        select dataaggrunid, to_char(dataaggrunid) || ' [' || to_char(operatingdate,'yyyy-mm-dd') || ' | ' || settlementtype || ' | ' || to_char(runnumber)|| ']' as displaystring
          from dataaggrun where dataaggrunid in (select dataaggrunid from dataaggrunhist where processname = 'DATA AGGREGATION' and STATUS = 'COMPLETE') order by inserttimestamp desc'''
        ad_analysis_runs = pd.read_sql(analyis_run_query, con=db)

        run_count = ad_analysis_runs.shape[0]

        dataaggrunid_to_zip = ad_analysis_runs['DATAAGGRUNID']
        displaystring_to_zip = ad_analysis_runs['DISPLAYSTRING']

        if run_count > 0:
            chosen_dataaggrunid = ad_analysis_runs['DATAAGGRUNID'][0]
        else:
            chosen_dataaggrunid = None

        # Retrieve dataaggreport records
        dataaggreport_query = 'select dataaggreport from dataaggreport order by dataaggreport'
        dataaggreport = pd.read_sql(dataaggreport_query, con=db)

        return html.Div([
            html.Div([
                # html.H6('Aggregated Data Reporting' ,style={'textAlign':'center'}),

                html.Div([
                    html.Label('Data Agg Run ID'),
                    dcc.Dropdown(id='dataaggrunid-dropdown',
                                 options=[{'label': display_string, 'value': dataaggrunid} for display_string, dataaggrunid in zip(displaystring_to_zip, dataaggrunid_to_zip)],
                                 value=[chosen_dataaggrunid],
                                 multi=True,
                                 persistence=True
                                 ),
                    ], style={'width': '20%',
                              'display': 'inline-block',
                              'margin': '10px 10px',
                              'verticalAlign': 'top'}),

                html.Div([
                    html.Label('Data Agg Reports'),
                    dcc.Dropdown(id='dataaggreport-dropdown',
                                 options=[{'label': i, 'value': i} for i in dataaggreport['DATAAGGREPORT']],
                                 value=['RETAILER DISCO PROFILE UNADJ'],
                                 multi=True,
                                 persistence=True
                                 ),
                    ], style={'width': '20%',
                              'display': 'inline-block',
                              'margin': '10px 10px',
                              'verticalAlign': 'top'}),

                html.Div([
                    html.Label('Filter Output'),
                    dcc.Textarea(
                        id='filter-output',
                        placeholder="ex: RETAILER = 'RETAILER 9' and DISCO = 'DISCO 1'",
                        value='',
                        rows=1,
                        wrap='False',
                        persistence=True,
                        style={'width': '100%', 'display': 'inline-block'}),
                ], style={
                    'margin': '10px 10px',
                    'verticalAlign': 'Top',
                    'width': '30%',
                    'display': 'inline-block'
                    }),

                html.Div([
                    html.Label('Overlap Days'),
                    dcc.RadioItems(
                        id='overlap-days-radio',
                        options=[{'label': i, 'value': i} for i in {'Y', 'N'}],
                        value='Y',
                        persistence=True,
                        labelStyle={'display': 'inline-block'}
                    ),
                ], style={'width': '8%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': '10px 10px'}),

                html.Div([
                    html.Button('Retrieve Data', id='retrieve-data-button', ),
                ], style={'width': '10%',
                          'display': 'inline-block',
                          'verticalAlign': 'bottom',
                          'color': 'red',
                          'margin': '10px 10px 10px 5px',
                          }),

            ], style={'marginTop': 0,
                      'marginBottom': 5,
                      'backgroundColor': mediumcolor,
                      'borderTopWidth': 5,
                      'borderTopStyle': 'solid',
                      'borderTopColor': 'black',
                      'borderBottomWidth': 1,
                      'borderBottomStyle': 'solid',
                      'borderBottomColor': 'black',
                      }),

            dcc.Loading(id="loading-aggregated-data-container", children=[
                html.Div(id='aggregated-data-container'),
            ]),

            dcc.Loading(id="loading-report-click-container", children=[
                html.Div(id='report-click-container'),
            ]),

            dcc.Loading(id="loading-lls-click-container", children=[
                html.Div(id='lls-click-container')
            ])

        ], style={'backgroundColor': lightcolor})

    elif tab == 'service-point-data':
        #  _____                 _           ______     _       _    ______      _
        # /  ___|               (_)          | ___ \   (_)     | |   |  _  \    | |
        # \ `--.  ___ _ ____   ___  ___ ___  | |_/ /__  _ _ __ | |_  | | | |__ _| |_ __ _
        #  `--. \/ _ \ '__\ \ / / |/ __/ _ \ |  __/ _ \| | '_ \| __| | | | / _` | __/ _` |
        # /\__/ /  __/ |   \ V /| | (_|  __/ | | | (_) | | | | | |_  | |/ / (_| | || (_| |
        # \____/ \___|_|    \_/ |_|\___\___| \_|  \___/|_|_| |_|\__| |___/ \__,_|\__\__,_|
        #

        # select dataaggruns to display in dropdown
        analyis_run_query = '''
        select dataaggrunid, to_char(dataaggrunid) || ' [' || to_char(operatingdate,'yyyy-mm-dd') || ' | ' || settlementtype || ' | ' || to_char(runnumber)|| ']' as displaystring
          from dataaggrun 
         where dataaggrunid in (select dataaggrunid from servicepointdataaggrun) 
         order by inserttimestamp desc
         fetch first 10000 rows only'''
        sp_analysis_runs = pd.read_sql(analyis_run_query, con=db)

        run_count = sp_analysis_runs.shape[0]

        dataaggrunid_to_zip = sp_analysis_runs['DATAAGGRUNID']
        displaystring_to_zip = sp_analysis_runs['DISPLAYSTRING']

        if run_count > 0:
            chosen_dataaggrunid = sp_analysis_runs['DATAAGGRUNID'][0]
        else:
            chosen_dataaggrunid = None

        return html.Div([
            html.Div([
                html.Div([
                    html.Label('Data Agg Run ID'),
                    dcc.Dropdown(id='dataaggrunid-dropdown',
                                 options=[{'label': display_string, 'value': dataaggrunid} for display_string, dataaggrunid in zip(displaystring_to_zip, dataaggrunid_to_zip)],
                                 value=chosen_dataaggrunid,
                                 multi=False,
                                 persistence=True
                                 )
                    ], style={'width': '20%',
                              'display': 'inline-block',
                              'margin': '10px 10px',
                              'verticalAlign': 'top'}),

                html.Div([
                    html.Label('Sort Order'),
                    dcc.RadioItems(
                        id='sort-order-radio',
                        options=[{'label': 'Desc', 'value': 'Desc'},
                                 {'label': 'Asc', 'value': 'Asc'}],
                        value='Desc',
                        persistence=True,
                        labelStyle={'display': 'inline-block'}
                    ),
                ], style={'width': '10%',
                          'display': 'inline-block',
                          'margin': '10px 10px',
                          'verticalAlign': 'top'}),

                html.Div([
                    html.Label('Service Point Filter'),
                    dcc.Textarea(
                        id='servicepoint-filter',
                        placeholder='ex: SERVICEPOINT = 044355',
                        value='',
                        wrap='False',
                        persistence=True,
                        style={'width': '100%', 'display': 'inline-block'}),
                    ], style={
                        'margin': '10px 10px',
                        'verticalAlign': 'Top',
                        'width': '30%',
                        'display': 'inline-block'
                        }),

                html.Div([
                    html.Label('Top N'),
                    dcc.Input(id="top_n", type="number", value=250, debounce=True),
                    ], style={
                        'margin': '10px 10px',
                        'verticalAlign': 'Top',
                        'width': '30%',
                        'display': 'inline-block'
                        }),

                html.Div([
                    html.Button('Refresh Data', id='refresh-data-button'),
                    ], style={
                        'width': '10%',
                        'display': 'inline-block',
                        'verticalAlign': 'bottom',
                        'color': 'red',
                        'margin': '10px 10px 10px 5px',
                        }),

            ], style={'marginTop': 0,
                      'marginBottom': 5,
                      'backgroundColor': mediumcolor,
                      'borderTopWidth': 5,
                      'borderTopStyle': 'solid',
                      'borderTopColor': 'black',
                      'borderBottomWidth': 1,
                      'borderBottomStyle': 'solid',
                      'borderBottomColor': 'black', }),

            # Hidden div inside the app that stores the intermediate value
            dcc.Loading(
                id="loading-intermediate-top-servicepoints",
                children=[
                    html.Div(id='intermediate-top-servicepoints', style={'display': 'none'}),
                    ]),

            dcc.Loading(id="loading-servicepoint-interactivity-container", children=[
                html.Div(id='servicepoint-interactivity-container'),
            ])
        ], style={'backgroundColor': lightcolor,
                  })

    elif tab == 'research':
        # ______                              _     
        # | ___ \                            | |    
        # | |_/ /___  ___  ___  __ _ _ __ ___| |__  
        # |    // _ \/ __|/ _ \/ _` | '__/ __| '_ \ 
        # | |\ \  __/\__ \  __/ (_| | | | (__| | | |
        # \_| \_\___||___/\___|\__,_|_|  \___|_| |_|
                                          
        # select research groups in dropdown
        researchgroup_query = '''select RESEARCHGROUP from RESEARCHGROUP order by RESEARCHGROUP'''
        researchgroup = pd.read_sql(researchgroup_query, con=db)

        uom_query = '''select UOM from UOM where UOM in ('SUM', 'MEAN', 'COUNT', 'VAR_POP', 'STDDEV_POP') order by UOM'''
        uom = pd.read_sql(uom_query, con=db)

        return html.Div([
            html.Div([
                dcc.Loading(id="loading-research-button", children=[html.Div(id='run-research-button')], type="default"),

                html.Div([
                    html.Label('Research Group'),
                    dcc.Dropdown(id='researchgroup-dropdown',
                                 options=[{'label': rg, 'value': rg} for rg in researchgroup['RESEARCHGROUP']],
                                 multi=True,
                                 persistence=True),
                    ], style={'width': '10%',
                              'display': 'inline-block',
                              'margin': '10px 10px',
                              'verticalAlign': 'top'}),

                html.Div([
                    html.Label('UOM'),
                    dcc.Dropdown(id='uom-dropdown',
                                 options=[{'label': u, 'value': u} for u in uom['UOM']],
                                 multi=True,
                                 persistence=True),
                    ], style={'width': '10%',
                              'display': 'inline-block',
                              'margin': '10px 10px',
                              'verticalAlign': 'top'}),

                html.Div([
                    html.Label('Research Start',),
                    dcc.DatePickerSingle(
                        id='research-start-picker',
                        initial_visible_month=datetime.today().date() - timedelta(days=1),
                        date=datetime.today().date() - timedelta(days=3),
                        day_size=47,
                        persistence=True,
                    ),
                    ], style={#'width': '7%',
                              'display': 'inline-block',
                              'verticalAlign': 'top',
                              'horizontallign': 'right',
                              'margin': '10px 10px 10px 5px',
                              }),

                html.Div([
                    html.Label('Research Stop',),
                    dcc.DatePickerSingle(
                        id='research-stop-picker',
                        initial_visible_month=datetime.today().date() - timedelta(days=1),
                        date=datetime.today().date() - timedelta(days=1),
                        day_size=47,
                        persistence=True,
                    ),
                    ], style={#'width': '7%',
                              'display': 'inline-block',
                              'verticalAlign': 'top',
                              'horizontallign': 'right',
                              'margin': '10px 10px 10px 5px',
                              }),

                html.Div([
                    html.Label('Overlap Days'),
                    dcc.RadioItems(
                        id='overlap-days-radio',
                        options=[{'label': i, 'value': i} for i in {'Y', 'N'}],
                        value='Y',
                        persistence=True,
                        labelStyle={'display': 'inline-block'}
                    ),
                ], style={'width': '5%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': '10px 10px'}),

                html.Div([
                    html.Label('Range or Compare'),
                    dcc.RadioItems(
                        id='range-compare-radio',
                        options=[{'label': i, 'value': i} for i in {'Range', 'Compare'}],
                        value='Range',
                        persistence=True,
                        labelStyle={'display': 'inline-block'}
                    ),
                ], style={'width': '8%',
                          'display': 'inline-block',
                          'verticalAlign': 'top',
                          'margin': '10px 10px'}),

                html.Div([
                    html.Button('Run Research', id='run-research-button'),
                    html.Button('Publish to Data Agg', id='publish-research-button'),
                    ], style={'width': '40%',
                              'display': 'inline-block',
                              'margin': '10px 10px 10px 5px',
                              'verticalAlign': 'bottom'}),
                ], style={'marginTop': 0,
                          'marginBottom': 5,
                          'backgroundColor': mediumcolor,
                          'borderTopWidth': 5,
                          'borderTopStyle': 'solid',
                          'borderTopColor': 'black',
                          'borderBottomWidth': 1,
                          'borderBottomStyle': 'solid',
                          'borderBottomColor': 'black', }),

            dcc.Loading(id="loading-research-buttons", children=[
                html.Div(id='run-research-button', style={'textAlign': 'left', 'margin': '20px 20 px', 'verticalAlign': 'middle', 'fontSize': 16, 'backgroundColor': lightcolor}),
                html.Div(id='publish-research-button', style={'textAlign': 'left', 'margin': '20px 20 px', 'verticalAlign': 'middle', 'fontSize': 16, 'backgroundColor': lightcolor}),
                ]),

            dcc.Loading(id="loading-research-container", children=[
                html.Div(id='research-container'),
                html.Div(id='research-output-container'),
                ])
        ], style={'backgroundColor': lightcolor})

    elif tab == 'browse-data':
        # ______                             ______      _
        # | ___ \                            |  _  \    | |
        # | |_/ /_ __ _____      _____  ___  | | | |__ _| |_ __ _
        # | ___ \ '__/ _ \ \ /\ / / __|/ _ \ | | | / _` | __/ _` |
        # | |_/ / | | (_) \ V  V /\__ \  __/ | |/ / (_| | || (_| |
        # \____/|_|  \___/ \_/\_/ |___/\___| |___/ \__,_|\__\__,_|
        #

        # select tables in dropdown
        tables_query = '''select table_name, table_name || ' (' || num_rows || ')' as DISPLAY_NAME, NUM_ROWS from user_tables order by table_name'''
        tables = pd.read_sql(tables_query, con=db)

        table_count = tables.shape[0]

        if table_count > 0:
            return html.Div([
                html.Div([
                    html.Div([
                        html.Label('Table Name'),
                        dcc.Dropdown(id='tables-dropdown',
                                     options=[{'label': i, 'value': i} for i in tables['TABLE_NAME']],
                                     value='CHANNELINTERVAL',
                                     multi=False,
                                     persistence=True,
                                     style={'position': 'relative', 'zIndex': '999', 'width': '100%', 'display': 'inline-block'},
                                     ),
                        ], style={
                            'margin': '10px 10px',
                            # 'backgroundColor': 'rgba(100,100,100,.15)',
                            'verticalAlign': 'Top',
                            'width': '25%',
                            'display': 'inline-block'}),

                    html.Div([
                        html.Label('Where clause'),
                        dcc.Textarea(id='where-clause',
                                     placeholder='where clause',
                                     value="meter = 'M0500001' and 'M0500008' and starttime between '01-Feb-19' and '03-Feb-19'",
                                     # rows=1,
                                     wrap='False',
                                     persistence=True,
                                     style={'width': '100%', 'display': 'inline-block'}),
                        ], style={
                            'margin': '10px 10px',
                            # 'backgroundColor': 'rgba(100,100,100,.15)',
                            'verticalAlign': 'Top',
                            'width': '50%',
                            'display': 'inline-block'
                            }),

                    html.Div([
                        html.Button('Retrieve Table', id='retrieve-table-button',
                                    loading_state={'is_loading': True,
                                                   'prop_name': 'Retrieving data...'}
                                    ),
                        ], style={
                            'width': '10%',
                            'display': 'inline-block',
                            'verticalAlign': 'bottom',
                            'color': 'red',
                            'margin': '10px 10px 10px 5px',
                            # 'marginBottom':5,
                            }),

                    ], style={'marginTop': 0,
                              'marginBottom': 5,
                              'backgroundColor': mediumcolor,
                              'borderTopWidth': 5,
                              'borderTopStyle': 'solid',
                              'borderTopColor': 'black',
                              'borderBottomWidth': 1,
                              'borderBottomStyle': 'solid',
                              'borderBottomColor': 'black', }),

                dcc.Loading(id="loading-table-data", debug=True, children=[
                    html.Div([html.Div(id='browse-tables-chart')], style={'margin': '10px 0px'}),
                    html.Div([html.Div(id='browse-tables-container')], style={'margin': '10px 0px'}),
                    ]),

                dcc.Loading(id="loading-table-description", debug=True, children=[
                    html.Div([html.Div(id='description-container')], style={'margin': '10px 0px'}),
                    ]),

                ], style={'backgroundColor': lightcolor})
        else:
            return 'No tables found'


#  ██████╗ █████╗ ██╗     ██╗     ██████╗  █████╗  ██████╗██╗  ██╗███████╗
# ██╔════╝██╔══██╗██║     ██║     ██╔══██╗██╔══██╗██╔════╝██║ ██╔╝██╔════╝
# ██║     ███████║██║     ██║     ██████╔╝███████║██║     █████╔╝ ███████╗
# ██║     ██╔══██║██║     ██║     ██╔══██╗██╔══██║██║     ██╔═██╗ ╚════██║
# ╚██████╗██║  ██║███████╗███████╗██████╔╝██║  ██║╚██████╗██║  ██╗███████║
#  ╚═════╝╚═╝  ╚═╝╚══════╝╚══════╝╚═════╝ ╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝

# ┌─┐┌─┐┌─┐  ┌┬┐┌─┐┌┬┐┌─┐  ┌─┐┬ ┬┌─┐┌─┐┬┌─┌─┐
# ├─┤│ ┬│ ┬   ││├─┤ │ ├─┤  │  ├─┤├┤ │  ├┴┐└─┐
# ┴ ┴└─┘└─┘  ─┴┘┴ ┴ ┴ ┴ ┴  └─┘┴ ┴└─┘└─┘┴ ┴└─┘

@app.callback(Output('data-checks-for-operating-date', 'children'),
              [Input('check-date-button', 'n_clicks')],
              [State('check-operating-date-picker', 'date')],
              prevent_initial_call=True,
              )
def show_opday_verification(n_clicks, operating_date_picked):
    if operating_date_picked is None:
        operating_date_picked = datetime.today().date() - timedelta(days=1)
    operatingdate_query = f'''select * from dataaggrun where operatingdate = to_date('{operating_date_picked}','yyyy-mm-dd') order by inserttimestamp desc'''
    dar = pd.read_sql(operatingdate_query, con=db)

    print('Running profiles, losses, and system load query')    
    profile_loss_systemload_query = f'''select * from miscellaneousinterval where starttime = to_date('{operating_date_picked}','yyyy-mm-dd') order by identifier, version'''
    pls = pd.read_sql(profile_loss_systemload_query, con=db)
    # pls2 = pls.copy()
    pls['INDEXCOLUMN'] = pls['IDENTIFIER']
    pls.set_index(['INDEXCOLUMN'], inplace=True)
    pls2 = pls.copy()
    # print(pls)
    # print(pls2)

    local_tz = pytz.timezone('US/Eastern')
    operatingdate = operating_date_picked

    dst_dates = pd.DataFrame([local_tz._utc_transition_times]).transpose()
    dst_dates.rename({0: "TRANSITION_DATE"}, axis='columns', inplace=True)
    dst_dates.drop(dst_dates.index[0], inplace=True)
    dst_dates['TRUNCATED'] = pd.to_datetime(dst_dates['TRANSITION_DATE'], errors='coerce').dt.normalize()

    dst_dates = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

    if dst_dates.shape[0] == 1:
        if dst_dates.iloc[0][1].month <= 5:
            hours_in_day = 23
        else:
            hours_in_day = 25
    else:
        hours_in_day = 24

    interval_cols = []
    for i in range(0, hours_in_day):
        if dst_dates.shape[0] == 1:
            if operatingdate.month > 6:
                # fall DST transition date
                if i < 2:
                    interval_label = str(i).zfill(2) + ':00'
                elif i == 2:
                    # interval_start in transitiondates:
                    interval_label = str(i-1).zfill(2) + ':00*'
                elif i > 2:
                    interval_label = str(i-1).zfill(2) + ':00'
            else:
                # spring DST transition date
                if i < 2:
                    interval_label = str(i).zfill(2) + ':00'
                else:
                    interval_label = str(i+1).zfill(2) + ':00'
        else:
            # normal non DST transition day
            interval_label = str(i).zfill(2) + ':00'

        interval_cols.append(interval_label)
        col_name = f"T{i:0>3}"
        pls2.rename(columns={col_name: interval_label}, inplace=True)
        # pls.rename(columns={col_name: interval_label}, inplace=True)

    # pls_chart = pls2[interval_cols].transpose()
    pls_chart = pls2[interval_cols].transpose()

    print(str(datetime.now()) + ' Running service point count query')    
    servicepointcount_query = f'''
    select /*+ parallel */
           s.disco,
           s.metertype,
           (count(ci.starttime) + count(con.starttime))/ count(*) percent_actual,
           count(ci.starttime) + count(con.starttime) actual_count,
           count(*) service_point_count
      from spaggattributes s
           left join servicepointchannels spc on s.servicepoint = spc.servicepoint
                and spc.starttime <= to_date('{operating_date_picked}','yyyy-mm-dd')
                and (spc.stoptime is null or spc.stoptime > to_date('{operating_date_picked}','yyyy-mm-dd'))
                and s.metertype = 'INTERVAL'
           left join channelinterval ci on ci.meter = spc.meter and ci.channel = spc.channel
                and ci.starttime = to_date('{operating_date_picked}','yyyy-mm-dd')
           left join servicepointconsumption con on s.servicepoint = con.servicepoint
                and to_date('{operating_date_picked}','yyyy-mm-dd') between con.starttime and con.stoptime
                and s.metertype = 'SCALAR'
     where s.starttime <= to_date('{operating_date_picked}','yyyy-mm-dd')
           and (s.stoptime is null or s.stoptime > to_date('{operating_date_picked}','yyyy-mm-dd'))
     group by disco, rollup(metertype)
     order by disco, metertype'''
    spc = pd.read_sql(servicepointcount_query, con=db)

    # overlapping spaggattributes records
    print(str(datetime.now()) + ' Running spaggattributes overlap query')    
    spaggattributes_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.starttime as starttime_1,
           s.stoptime as stoptime_1,
           ss.starttime as starttime_2,
           ss.stoptime as stoptime_2
      from spaggattributes s
           join spaggattributes ss on s.servicepoint = ss.servicepoint
     where s.starttime < ss.starttime
           and (s.stoptime is null or s.stoptime > ss.starttime)
     order by s.servicepoint,
           s.starttime'''
    spagg_overlaps = pd.read_sql(spaggattributes_query, con=db)


    # overlapping servicepointconsumption records
    # print(str(datetime.now()) + ' Running servicepointconsumption overlap query')    
    # servicepointconsumption_query = '''
    # select /*+ parallel */
    #        s.servicepoint,
    #        s.starttime as starttime_1,
    #        s.stoptime as stoptime_1,
    #        ss.starttime as starttime_2,
    #        ss.stoptime as stoptime_2
    #   from servicepointconsumption s
    #        join servicepointconsumption ss on s.servicepoint = ss.servicepoint
    #  where s.starttime < ss.starttime
    #        and (s.stoptime is null or s.stoptime > ss.starttime)
    #  order by s.servicepoint,
    #        s.starttime'''
    # servicepointconsumption_overlaps = pd.read_sql(servicepointconsumption_query, con=db)

           
    # check invalid data assignments
    print(str(datetime.now()) + ' Running invalid retailer assignment query')
    invalid_retailer_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.retailer,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime retailer_starttime,
           r.stoptime retailer_stoptime
      from spaggattributes s
           join retailer r on s.retailer = r.retailer
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    iret = pd.read_sql(invalid_retailer_query, con=db)

    print(str(datetime.now()) + ' Running invalid disco assignment query')    
    invalid_disco_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.disco,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime disco_starttime,
           r.stoptime disco_stoptime
      from spaggattributes s
           join disco r on s.disco = r.disco
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    idisco = pd.read_sql(invalid_disco_query, con=db)

    print(str(datetime.now()) + ' Running invalid profile class assignment query')
    invalid_profileclass_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.profileclass,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime profileclass_starttime,
           r.stoptime profileclass_stoptime
      from spaggattributes s
           join profileclass r on s.profileclass = r.profileclass
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    iprof = pd.read_sql(invalid_profileclass_query, con=db)

    print(str(datetime.now()) + ' Running invalid loss class assignment query')
    invalid_lossclass_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.lossclass,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime lossclass_starttime,
           r.stoptime lossclass_stoptime
      from spaggattributes s
           join lossclass r on s.lossclass = r.lossclass
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    iloss = pd.read_sql(invalid_lossclass_query, con=db)

    print(str(datetime.now()) + ' Running invalid ufezone assignment query')
    invalid_ufezone_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.ufezone,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime ufezone_starttime,
           r.stoptime ufezone_stoptime
      from spaggattributes s
           join ufezone r on s.ufezone = r.ufezone
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    iufe = pd.read_sql(invalid_ufezone_query, con=db)

    print(str(datetime.now()) + ' Running invalid lmpbus assignment query')
    invalid_lmpbus_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.lmpbus,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime lmpbus_starttime,
           r.stoptime lmpbus_stoptime
      from spaggattributes s
           join lmpbus r on s.lmpbus = r.lmpbus
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    ilmp = pd.read_sql(invalid_lmpbus_query, con=db)

    print(str(datetime.now()) + ' Running invalid demand response zone assignment query')
    invalid_demandresponsezone_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.demandresponsezone,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime demandresponsezone_starttime,
           r.stoptime demandresponsezone_stoptime
      from spaggattributes s
           join demandresponsezone r on s.demandresponsezone = r.demandresponsezone
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    idemand = pd.read_sql(invalid_demandresponsezone_query, con=db)

    print(str(datetime.now()) + ' Running invalid rate factor assignment query')
    invalid_ratefactor_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.ratefactor,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime ratefactor_starttime,
           r.stoptime ratefactor_stoptime
      from spaggattributes s
           join ratefactor r on s.ratefactor = r.ratefactor
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    irate = pd.read_sql(invalid_ratefactor_query, con=db)

    print(str(datetime.now()) + ' Running invalid customer class assignment query')
    invalid_customerclass_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.customerclass,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime customerclass_starttime,
           r.stoptime customerclass_stoptime
      from spaggattributes s
           join customerclass r on s.customerclass = r.customerclass
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    icust = pd.read_sql(invalid_customerclass_query, con=db)

    print(str(datetime.now()) + ' Running invalid dynamic pricing assignment query')
    invalid_dynamicpricing_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.dynamicpricing,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime dynamicpricing_starttime,
           r.stoptime dynamicpricing_stoptime
      from spaggattributes s
           join dynamicpricing r on s.dynamicpricing = r.dynamicpricing
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    idynamic = pd.read_sql(invalid_dynamicpricing_query, con=db)

    print(str(datetime.now()) + ' Running invalid direct load control assignment query')
    invalid_directloadcontrol_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.directloadcontrol,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime directloadcontrol_starttime,
           r.stoptime directloadcontrol_stoptime
      from spaggattributes s
           join directloadcontrol r on s.directloadcontrol = r.directloadcontrol
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    idlc = pd.read_sql(invalid_directloadcontrol_query, con=db)

    print(str(datetime.now()) + ' Running invalid weatherzone assignment query')
    invalid_weatherzone_query = '''
    select /*+ parallel */
           s.servicepoint,
           s.weatherzone,
           s.starttime servicepoint_starttime,
           s.stoptime servicepoint_stoptime,
           r.starttime weatherzone_starttime,
           r.stoptime weatherzone_stoptime
      from spaggattributes s
           join weatherzone r on s.weatherzone = r.weatherzone
     where s.starttime < r.starttime
           or (s.stoptime is null and r.stoptime is not null)
           or s.stoptime > r.stoptime
     order by servicepoint'''
    iweather = pd.read_sql(invalid_weatherzone_query, con=db)

    # now display the data
    print(str(datetime.now()) + ' Display results of verification queries')
    return html.Div([
        html.H6('Existing Data Agg Runs', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-dataaggrun-opdate-table',
            columns=[
                {"name": i, "id": i} for i in dar.columns
            ],
            data=dar.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        html.H6('Service Points by DISCO and Meter Type', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-disco-metertype-servicepoints-table',
            # columns=[
            #     {"name": i, "id": i, "format": "Format(group(True))"} for i in spc.columns
            # ],
            columns=[
                {"name": "DISCO", "id": "DISCO", "type": "text", },
                {"name": "Meter Type", "id": "METERTYPE", "type": "text", },
                {"name": "Percent Actual", "id": "PERCENT_ACTUAL", "type": "numeric", "format": FormatTemplate.percentage(2)},
                {"name": "Actual Count", "id": "ACTUAL_COUNT", "type": "numeric", "format": FormatTemplate.money(0).symbol('')},
                {"name": "Service Point Count", "id": "SERVICE_POINT_COUNT", "type": "numeric", "format": FormatTemplate.money(0).symbol('')},
                ],
            data=spc.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        html.H6('Profiles, Losses, and System Load', style={'textAlign': 'center'}),

        html.Div([
            dcc.Graph(
                id='pls_graph',
                figure={
                    'data': [go.Scattergl(
                            x=pls_chart.index,
                            y=pls_chart[i],
                            text=pls_chart.index,
                            mode='lines',
                            opacity=1,
                            name=i
                            ) for i in pls_chart.columns],
                    'layout': dict(
                            height=500,
                            xaxis={
                                'title': 'Time',
                                'nticks': hours_in_day,
                                'showgrid': False,
                                'tickangle': -45},
                            hovermode='closest',
                            hoverlabel=dict(namelength=-1),
                            paper_bgcolor=paperbackgroundcolor,
                            plot_bgcolor=plotbackgroundcolor,
                            legend_bgcolor=legendbackgroundcolor,
                            )
                    },
            ),
            ], style={'display': 'inline-block', 'width': '100%', 'height': 500}),

        dash_table.DataTable(
            id='aggcheck-system-load-opdate-table',
            # columns=[
            #     {"name": i, "id": i, } for i in pls.columns
            # ],
            columns=[
                {"name": "Identifier", "id": "IDENTIFIER", "type": "text", },
                {"name": "Version", "id": "VERSION", "type": "numeric", },
                {"name": "UOM", "id": "UOM", "type": "text", },
                {"name": "Start Time", "id": "STARTTIME", },
                {"name": "Stop Time", "id": "STOPTIME", },
                {"name": "Interval Length", "id": "INTERVALLENGTH", "type": "numeric", },
                {"name": "Total", "id": "TOTAL", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T000", "id": "T000", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T001", "id": "T001", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T002", "id": "T002", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T003", "id": "T003", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T004", "id": "T004", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T005", "id": "T005", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T006", "id": "T006", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T007", "id": "T007", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T008", "id": "T008", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T009", "id": "T009", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T010", "id": "T010", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T011", "id": "T011", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T012", "id": "T012", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T013", "id": "T013", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T014", "id": "T014", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T015", "id": "T015", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T016", "id": "T016", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T017", "id": "T017", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T018", "id": "T018", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T019", "id": "T019", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T020", "id": "T020", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T021", "id": "T021", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T022", "id": "T022", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T023", "id": "T023", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "T024", "id": "T024", "type": "numeric", "format": FormatTemplate.money(4).symbol('')},
                {"name": "Insert User", "id": "INSERTUSER", "type": "text", },
                {"name": "Insert Time Stamp", "id": "INSERTTIMESTAMP", },
                {"name": "Update User", "id": "UPDATEUSER", "type": "text", },
                {"name": "Update Time Stamp", "id": "UPDATETIMESTAMP", },
                ],
            data=pls.to_dict("rows"),
            editable=False,
            filter_action='native',
            sort_action='native',
            sort_mode="multi",
            row_selectable="multi",
            row_deletable=True,
            selected_rows=[],
            style_table={'overflowX': 'scroll', 'overflowY': 'scroll', 'max_height': 400},
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
            export_format='xlsx',
            export_headers='display',
            merge_duplicate_headers=True,
        ),


        html.H6('Service Point Agregation Attributes Overlaps', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-spaggattributes-overlaps-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in spagg_overlaps.columns
            ],
            data=iret.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        html.H6('Service Points with Invalid Retailer Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-retailer-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in iret.columns
            ],
            data=iret.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        html.H6('Service Points with Invalid DISCO Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-disco-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in idisco.columns
            ],
            data=idisco.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),
        

        html.H6('Service Points with Invalid Profile Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-profile-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in iprof.columns
            ],
            data=iprof.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),


        html.H6('Service Points with Invalid Loss Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-loss-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in iloss.columns
            ],
            data=iloss.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),


        html.H6('Service Points with Invalid UFE Zone Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-ufezone-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in iufe.columns
            ],
            data=iufe.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),


        html.H6('Service Points with Invalid LMP Bus Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-lmpbus-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in ilmp.columns
            ],
            data=ilmp.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),


        html.H6('Service Points with Invalid Demand Response Zone Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-demandresponsezone-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in idemand.columns
            ],
            data=idemand.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        html.H6('Service Points with Invalid Rate Factor Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-ratefactor-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in irate.columns
            ],
            data=irate.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        html.H6('Service Points with Invalid Customer Class Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-customerclass-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in icust.columns
            ],
            data=icust.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        html.H6('Service Points with Invalid Dynamic Pricing Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-dynamicpricing-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in idynamic.columns
            ],
            data=idynamic.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        html.H6('Service Points with Invalid Direct Load Control Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-directloadcontrol-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in idlc.columns
            ],
            data=idlc.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        html.H6('Service Points with Invalid Weather Zone Assignments', style={'textAlign': 'center'}),

        dash_table.DataTable(
            id='aggcheck-invalid-weatherzone-servicepoints-table',
            columns=[
                {"name": i, "id": i, "format": "Format(group(True))"} for i in iweather.columns
            ],
            data=iweather.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            # style_as_list_view=True,
            style_header={
                'backgroundColor': darkcolor,
                'fontWeight': 'bold',
                'color': 'white'
                },
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data_conditional=[
                {'if': {'row_index': 'odd'},
                 'backgroundColor': lightcolor,
                 }
                ],
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),

        ], style={'marginTop': 10,
                  'marginBottom': 10})


# ┌─┐─┐ ┬┌─┐┌─┐┬ ┬┌┬┐┌─┐  ┬─┐┬ ┬┌┐┌      ┌─┐┬ ┬┌─┐┬ ┬    ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌─┐┌─┐┬─┐┬ ┬┌┐┌
# ├┤ ┌┴┬┘├┤ │  │ │ │ ├┤   ├┬┘│ ││││──────└─┐├─┤│ ││││     ││├─┤ │ ├─┤├─┤│ ┬│ ┬├┬┘│ ││││
# └─┘┴ └─└─┘└─┘└─┘ ┴ └─┘  ┴└─└─┘┘└┘      └─┘┴ ┴└─┘└┴┘─────┴┘┴ ┴ ┴ ┴ ┴┴ ┴└─┘└─┘┴└─└─┘┘└┘

@app.callback(Output('dataaggrun-for-operating-date', 'children'),
              [Input('operating-date-picker', 'date')],
              [State('running-dataaggrunid-dropdown', 'value')]
              )
def show_dataaggrun(operating_date_picked, dataaggrunid):
    if operating_date_picked is None:
        operating_date_picked = datetime.today().date() - timedelta(days=1)
    operatingdate_query = f'''select * from dataaggrun where operatingdate = to_date('{operating_date_picked}','yyyy-mm-dd') order by inserttimestamp desc'''
    dar = pd.read_sql(operatingdate_query, con=db)
    filterquerytext = '{DATAAGGRUNID} eq "' + str(dataaggrunid) + '"'

    return html.Div([
        dash_table.DataTable(
            id='dataaggrun-opdate-table',
            columns=[
                {"name": i, "id": i} for i in dar.columns
            ],
            data=dar.to_dict("rows"),
            editable=False,
            filter_action='none',
            sort_action='none',
            style_as_list_view=True,
            style_cell={
                # all three widths are needed
                'minWidth': '120px',
                'whiteSpace': 'no-wrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
            style_data={
                'backgroundColor': mediumcolor,
                'border': '0px solid white',
                'fontColor': 'black',
                'borderTop': '2px solid ' + darkcolor,
                },
            style_data_conditional=[{
                'if': {'filter_query': filterquerytext},
                'borderTop': '1px solid ' + darkcolor,
                'borderBottom': '1px solid ' + darkcolor,
                'backgroundColor': mediumlightcolor,
                },
            ],
            style_header={
                'backgroundColor': mediumcolor,
                'fontWeight': 'bold',
                'border': '0px solid white',
                'borderTop': '1px solid white',
                },
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
        ),
        ], style={'marginTop': 10,
                  'marginBottom': 10})


# ┌─┐─┐ ┬┌─┐┌─┐┬ ┬┌┬┐┌─┐  ┬─┐┬ ┬┌┐┌      ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐    ┬─┐┬ ┬┌┐┌┌─┐┌┬┐┌─┐┌┬┐┬ ┬┌─┐
# ├┤ ┌┴┬┘├┤ │  │ │ │ ├┤   ├┬┘│ ││││──────│ │├─┘ ││├─┤ │ ├┤     ├┬┘│ ││││└─┐ │ ├─┤ │ │ │└─┐
# └─┘┴ └─└─┘└─┘└─┘ ┴ └─┘  ┴└─└─┘┘└┘      └─┘┴  ─┴┘┴ ┴ ┴ └─┘────┴└─└─┘┘└┘└─┘ ┴ ┴ ┴ ┴ └─┘└─┘

@app.callback([#Output('running_dataaggrunid', 'children'),
               Output('dataaggrunhist-for-dataaggrunid', 'children'),
               Output('operating-date-picker', 'date'),
               Output('running-dataaggrunid-dropdown', 'value'),
               ],
              [Input('running-dataaggrunid-dropdown', 'value'),
               Input('interval-component', 'n_intervals')
               ],
              [State('operating-date-picker', 'date')],
              )
def update_runstatus(dataaggrunid, n, orig_operating_date):
    if dataaggrunid is None:
        if orig_operating_date is None:
            return [None, datetime.today().date() - timedelta(days=1), None]
        else:
            most_recent_run_query = 'select distinct first_value(dataaggrunid) over (order by inserttimestamp desc) as dataaggrunid from dataaggrunhist'
            most_recent_run = pd.read_sql(most_recent_run_query, con=db)
            if most_recent_run.empty:
                return [None, orig_operating_date, None]
            else:
                dataaggrunid = most_recent_run['DATAAGGRUNID'][0]
                if orig_operating_date is None:
                    # return [None, None, datetime.today().date() - timedelta(days=1)]
                    return [None, datetime.today().date() - timedelta(days=1), None]
                else:
                    # return [None, None, orig_operating_date]
                    return [None, orig_operating_date, dataaggrunid]
    else:
        current_run_query = f'''select * from dataaggrun where dataaggrunid = {dataaggrunid}'''
        dar = pd.read_sql(current_run_query, con=db)

        if dar.shape[0] > 0:
            operating_date = dar['OPERATINGDATE'].values[0].astype('M8[D]').astype('O')
            if orig_operating_date is not None:
                selected_operating_date = orig_operating_date
            else:
                selected_operating_date = orig_operating_date

            # Retrieve dataaggrunhist records for selected dataaggrunid
            dataaggrunhist_query = '''
            select a.dataaggrunid, b.operatingdate, b.SETTLEMENTTYPE, b.runnumber, a.step, a.processname, a.status,
                   nvl(substr(elapsedtime,-15,12),
                       to_char(trunc(sysdate) + (systimestamp-PROCESSSTART), 'hh24:mi:ss')||' <——'
                   ) as elapsedtime,
                   c.avgelapsedtime, c.minelapsedtime, c.maxelapsedtime, a.processstart, a.processstop, a.comments
              from dataaggrunhist a
                   join dataaggrun b on a.dataaggrunid = b.dataaggrunid
                   left join
                       (select processname,
                               regexp_substr (numtodsinterval(avg(extract(day from elapsedtime) * 86400 + extract(hour from elapsedtime) * 3600 + extract(minute from elapsedtime) * 60 + extract(second from elapsedtime)),'second'), '\d{{2}}:\d{{2}}:\d{{2}}\.\d{{3}}') avgelapsedtime,
                               regexp_substr (numtodsinterval(min(extract(day from elapsedtime) * 86400 + extract(hour from elapsedtime) * 3600 + extract(minute from elapsedtime) * 60 + extract(second from elapsedtime)),'second'), '\d{{2}}:\d{{2}}:\d{{2}}\.\d{{3}}') minelapsedtime,
                               regexp_substr (numtodsinterval(max(extract(day from elapsedtime) * 86400 + extract(hour from elapsedtime) * 3600 + extract(minute from elapsedtime) * 60 + extract(second from elapsedtime)),'second'), '\d{{2}}:\d{{2}}:\d{{2}}\.\d{{3}}') maxelapsedtime
                          from dataaggrunhist
                         where status = 'COMPLETE' and processstart >= sysdate - 90
                         group by processname) c
                       on a.processname = c.processname
             where a.dataaggrunid = {dataaggrunid}
             order by a.dataaggrunid desc, a.step'''.format(dataaggrunid=dataaggrunid)
            dataaggrunhist = pd.read_sql(dataaggrunhist_query, con=db)

        if dataaggrunhist.shape[0] == 0:
            # return ['no dataaggrunhist records were retrieved for dataaggrunid ' + str(dataaggrunid), None, operating_date]
            return ['no dataaggrunhist records were retrieved for dataaggrunid ' + str(dataaggrunid), operating_date]
        else:
            dataaggrunhist['STEP'] = dataaggrunhist['STEP'].round(2)

            # Retrieve dataaggrunhist records for performance trends
            performance_query = '''
            select dataaggrunid,
                   to_char(step,99.9) || ' ' || processname as processname,
                   (extract(day from elapsedtime) * 86400 + extract(hour from elapsedtime) * 3600 + extract(minute from elapsedtime) * 60 + extract(second from elapsedtime))/60 as elapsedtime,
                   inserttimestamp
              from dataaggrunhist
             where status = 'COMPLETE'
                   --and STEP >= 1
                   and inserttimestamp > trunc(systimestamp) - 180
             order by step'''
            performance = pd.read_sql(performance_query, con=db)

            perf_traces = []

            if performance.shape[0] > 0:
                perf_pivot = pd.pivot_table(performance, index=['DATAAGGRUNID'], columns=['PROCESSNAME'], values=['ELAPSEDTIME'])
                for i in perf_pivot.columns:
                    if i[1] == '  1.0 DATA AGGREGATION':
                        trace = go.Scattergl(
                            x=perf_pivot.index,
                            y=perf_pivot[i],
                            yaxis='y',
                            # text=i[1] + ' | ' + str(perf_pivot[i]//60) + ':' + str(perf_pivot[i]%60 * 60),
                            mode='lines+markers',
                            marker={'color': 'white', 'size': 12, 'symbol': 'circle', 'line': {'width': 3, 'color': 'darkred'}},
                            line={'color': 'darkred', 'width': 3},
                            connectgaps=False,
                            # opacity=1,
                            name=i[1],
                            )
                    elif i[1] == '  2.0 SCALAR UNADJUSTED':
                        trace = go.Scattergl(
                            x=perf_pivot.index,
                            y=perf_pivot[i],
                            yaxis='y',
                            # text=i[1] + ' | ' + str(perf_pivot[i]//60) + ':' + str(perf_pivot[i]%60 * 60),
                            mode='lines+markers',
                            marker={'color': 'white', 'size': 8, 'symbol': 'square', 'line': {'width': 1.5, 'color': 'darkgreen'}},
                            line={'color': 'darkgreen'},
                            connectgaps=False,
                            # opacity=1,
                            name=i[1],
                            )
                    elif i[1] == '  3.0 INTERVAL UNADJUSTED':
                        trace = go.Scattergl(
                            x=perf_pivot.index,
                            y=perf_pivot[i],
                            yaxis='y',
                            # text=i[1] + ' | ' + str(perf_pivot[i]//60) + ':' + str(perf_pivot[i]%60 * 60),
                            mode='lines+markers',
                            marker={'color': 'white', 'size': 8, 'symbol': 'diamond', 'line': {'width': 1.5, 'color': 'darkblue'}},
                            line={'color': 'darkblue'},
                            connectgaps=False,
                            # opacity=1,
                            name=i[1],
                            )
                    elif i[1] == ' 12.0 INTERVAL DISTLOSSADJ':
                        trace = go.Scattergl(
                            x=perf_pivot.index,
                            y=perf_pivot[i],
                            yaxis='y',
                            # text=i[1] + ' | ' + str(perf_pivot[i]//60) + ':' + str(perf_pivot[i]%60 * 60),
                            mode='lines+markers',
                            # marker={'color':'white', 'size':8, 'symbol': 'star', 'line':{'width':1.5, 'color': 'darkgoldenrod'}},
                            # line={'color': 'darkgoldenrod',},
                            connectgaps=False,
                            # opacity=1,
                            name=i[1],
                            )
                    elif i[1] == ' 13.0 INTERVAL TRANLOSSADJ':
                        trace = go.Scattergl(
                            x=perf_pivot.index,
                            y=perf_pivot[i],
                            yaxis='y',
                            # text=i[1] + ' | ' + str(perf_pivot[i]//60) + ':' + str(perf_pivot[i]%60 * 60),
                            mode='lines+markers',
                            # marker={'color':'white', 'size':8, 'symbol': 'pentagon', 'line':{'width':1.5, 'color': 'darkorchid'}},
                            # line={'color': 'darkorchid',},
                            connectgaps=False,
                            # opacity=1,
                            name=i[1],
                            )
                    elif i[1] == ' 14.0 INTERVAL UFEADJ':
                        trace = go.Scattergl(
                            x=perf_pivot.index,
                            y=perf_pivot[i],
                            yaxis='y',
                            # text=i[1] + ' | ' + str(perf_pivot[i]//60) + ':' + str(perf_pivot[i]%60 * 60),
                            mode='lines+markers',
                            # marker={'color':'white', 'size':8, 'symbol': 'hexagon', 'line':{'width':1.5, 'color': 'darkslateblue'}},
                            # line={'color': 'darkslateblue',},
                            connectgaps=False,
                            # opacity=1,
                            name=i[1],
                            )
                    elif 'DELETE' in i[1]:
                        trace = go.Scattergl(
                            x=perf_pivot.index,
                            y=perf_pivot[i],
                            yaxis='y',
                            # text=i[1] + ' | ' + str(perf_pivot[i]//60) + ':' + str(perf_pivot[i]%60 * 60),
                            mode='markers',
                            marker={'size': 6, 'symbol': 'diamond-open', 'line': {'width': 1.5}},
                            # line={'dash':'dot', 'width': 2},
                            connectgaps=False,
                            # opacity=1,
                            name=i[1],
                            )

                    else:
                        trace = go.Scattergl(
                            x=perf_pivot.index,
                            y=perf_pivot[i],
                            yaxis='y',
                            # text=i[1] + ' | ' + str(perf_pivot[i]//60) + ':' + str(perf_pivot[i]%60 * 60),
                            mode='lines',
                            connectgaps=False,
                            # opacity=1,
                            name=i[1],
                            )
                    perf_traces.append(trace)

            perf_layout = go.Layout(
                title='90 Day Performance Trends',
                height=600,
                yaxis={'title': 'Elapsed Time (minutes)',
                       'side': 'left',
                       # 'automargin': True,
                       },
                legend=dict(orientation='v'),
                xaxis={'title': 'Data Agg Run ID',
                       'showgrid': False,
                       'tickangle': -45},
                hovermode='closest',
                hoverlabel=dict(namelength=-1,),
                paper_bgcolor=paperbackgroundcolor,
                plot_bgcolor=plotbackgroundcolor,
                legend_bgcolor=legendbackgroundcolor,
                font_color='black',
                )

            # Retrieve dba scheduler running jobs
            dba_scheduler_running_jobs_query = '''select * from dba_scheduler_running_jobs /*where job_name like 'DATAAGG%'*/ '''
            dba_scheduler_running_jobs = pd.read_sql(dba_scheduler_running_jobs_query, con=db)

            # Retrieve dba scheduler job run details records
            dba_scheduler_job_run_details_query = '''
            select LOG_ID, LOG_DATE, JOB_NAME, OUTPUT, STATUS, ERROR#, ERRORS, REQ_START_DATE, ACTUAL_START_DATE, RUN_DURATION, INSTANCE_ID, SESSION_ID, SLAVE_PID, CPU_USED, ADDITIONAL_INFO
              from dba_scheduler_job_run_details
             where job_name like 'DATAAGG%'
                   and output = 'DATAAGGRUNID '||{dataaggrunid}
             order by actual_start_date'''.format(dataaggrunid=dataaggrunid)
            dba_scheduler_job_run_details = pd.read_sql(dba_scheduler_job_run_details_query, con=db)

            if dba_scheduler_running_jobs.empty:
                if dba_scheduler_job_run_details.empty:
                    # dba_scheduler_output = pd.DataFrame([['Comment', 'The job is not currently running and no record remains in the DBA Scheduler logs']], columns=['Comment'])
                    dba_scheduler_output = pd.DataFrame(['The job is not currently running and no record remains in the DBA Scheduler logs'], columns=['Comment'])
                else:
                    dba_scheduler_output = dba_scheduler_job_run_details
            else:
                dba_scheduler_output = dba_scheduler_running_jobs

            # dba_scheduler_output.columns = [x.decode("utf-8") for x in dba_scheduler_output.columns]

            return [html.Div([
                    dash_table.DataTable(
                        id='dataaggrunhist-running-table',
                        columns=[
                            {"name": i, "id": i} for i in dataaggrunhist.columns
                        ],
                        data=dataaggrunhist.to_dict("rows"),
                        editable=False,
                        filter_action='none',
                        sort_action='none',
                        row_deletable=False,
                        style_table={'overflowX': 'scroll'},
                        style_as_list_view=True,
                        style_cell={
                            # all three widths are needed
                            'minWidth': '120px',
                            'whiteSpace': 'no-wrap',
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                            },
                        style_data={
                            'backgroundColor': lightcolor,
                            'border': '0px solid white',
                            'fontColor': 'black',
                            'borderTop': '2px solid ' + darkcolor,
                            },
                        style_header={
                            'backgroundColor': lightcolor,
                            'fontWeight': 'bold',
                            'border': '0px solid white',
                            },
                        style_data_conditional=[
                            {'if': {'filter_query': '{STATUS} eq "START"'},
                             'color': 'rgb(125, 50, 50)',
                             'border': '2px solid white',
                             },
                            {'if': {'filter_query': '{PROCESSNAME} eq "DATA AGGREGATION"'},
                             'borderTop': '1px solid ' + darkcolor,
                             'borderBottom': '1px solid ' + darkcolor,
                             'backgroundColor': mediumlightcolor,
                             },
                            {'if': {'filter_query': '{STATUS} eq "FAILURE"'},
                             'backgroundColor': 'rgb(130, 75, 75)',
                             'color': 'white',
                             'fontWeight': 'bold',
                             },
                            {'if': {'filter_query': '{PROCESSNAME} contains "DELETE"'},
                             'color': '#505050',
                             }
                            ],
                        css=[
                            {'selector': '.dash-cell div.dash-cell-value',
                             'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                             }
                            ],
                        export_format='xlsx',
                        export_headers='display',
                        merge_duplicate_headers=True
                    ),


                    html.H6('DBA Scheduler Running Jobs', style={'textAlign': 'center'}),

                    html.Div([
                        dash_table.DataTable(
                            id='dba-scheduler-running-jobs-table',
                            columns=[
                                {"name": i, "id": i} for i in dba_scheduler_running_jobs.columns
                            ],
                            data=dba_scheduler_running_jobs.to_dict("rows"),
                            editable=False,
                            sort_action='none',
                            row_deletable=False,
                            style_table={'overflowX': 'scroll'},
                            style_as_list_view=True,
                            style_cell={
                                # all three widths are needed
                                'minWidth': '120px',
                                'whiteSpace': 'no-wrap',
                                'overflow': 'hidden',
                                'textOverflow': 'ellipsis',
                            },
                            style_data={'backgroundColor': lightcolor,
                                        'border': '0px solid white',
                                        'fontColor': 'black',
                                        'borderTop': '2px solid ' + darkcolor,
                                        },
                            style_header={
                                'backgroundColor': lightcolor,
                                'fontWeight': 'bold',
                                'border': '0px solid white',
                                },
                            style_data_conditional=[
                                {'if': {'column_id': 'Comment'},
                                 'textAlign': 'left',
                                 },
                            ],
                            css=[{
                                'selector': '.dash-cell div.dash-cell-value',
                                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                }],
                            ),
                        ]),

                    html.H6('DBA Scheduler Job Run Details', style={'textAlign': 'center'}),

                    html.Div([
                        dash_table.DataTable(
                            id='dba-scheduler-job-run-details-table',
                            columns=[
                                {"name": i, "id": i} for i in dba_scheduler_job_run_details.columns
                            ],
                            data=dba_scheduler_job_run_details.to_dict("rows"),
                            editable=False,
                            # filter_action='native',
                            # filter_query='{OUTPUT} eq "DATAAGGRUNID 85"}',
                            sort_action='none',
                            row_deletable=False,
                            style_table={'overflowX': 'scroll'},
                            style_as_list_view=True,
                            style_cell={
                                # all three widths are needed
                                'minWidth': '120px',
                                'whiteSpace': 'no-wrap',
                                'overflow': 'hidden',
                                'textOverflow': 'ellipsis',
                            },
                            style_data={'backgroundColor': lightcolor,
                                        'border': '0px solid white',
                                        'fontColor': 'black',
                                        'borderTop': '2px solid ' + darkcolor,
                                        },
                            style_header={
                                'backgroundColor': lightcolor,
                                'fontWeight': 'bold',
                                'border': '0px solid white',
                                },
                            style_data_conditional=[
                                {'if': {'column_id': 'Comment'},
                                 'textAlign': 'left',
                                 },
                            ],
                            css=[{
                                'selector': '.dash-cell div.dash-cell-value',
                                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                }],
                            ),
                        ]),

                    dcc.Graph(
                        id='performance-trends',
                        figure={
                            'data': perf_traces,
                            'layout': perf_layout
                            },
                        ),
                    ]),

                    selected_operating_date,
                    
                    ]


# ┌─┐─┐ ┬┌─┐┌─┐┬ ┬┌┬┐┌─┐  ┬─┐┬ ┬┌┐┌      ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐    ┌─┐┬ ┬┌┬┐┌─┐┬ ┬┌┬┐
# ├┤ ┌┴┬┘├┤ │  │ │ │ ├┤   ├┬┘│ ││││──────│ │├─┘ ││├─┤ │ ├┤     │ ││ │ │ ├─┘│ │ │
# └─┘┴ └─└─┘└─┘└─┘ ┴ └─┘  ┴└─└─┘┘└┘      └─┘┴  ─┴┘┴ ┴ ┴ └─┘────└─┘└─┘ ┴ ┴  └─┘ ┴

@app.callback(Output('running-dataaggrunid-dropdown', 'value'),
              [Input('run-aggregation-button', 'n_clicks')],
              [State('market-radio', 'value'),
               State('operating-date-picker', 'date'),
               State('settlementtype-radio', 'value'),
               State('run-number', 'value'),
               State('days-to-run', 'value'),
               State('replace-radio', 'value'),
               State('service-point-output-radio', 'value'),
               ],
              prevent_initial_call=True,
              )
def update_output(n_clicks, market, operating_date, settlementtype, run_number, days_to_run, replace, servicepoint_output):
    # check if dataaggrun record exists for input variables
    procedure_out = cur2.var(str)
    # to_date_operating_date = "to_date('{operating_date}', 'yyyy-mm-dd')".format(operating_date=operating_date)
    cur2.callproc('CREATE_DATAAGGRUN_RECORD', [operating_date, settlementtype, run_number, days_to_run, market, replace, servicepoint_output, procedure_out])

    current_dataaggrunid = procedure_out.getvalue()

    print('__________')
    print(datetime.now(), ' : dataaggrunid = ' , procedure_out.getvalue())
    print(datetime.now(), ' : market=', market, ', operating date=', operating_date, 'settlement type=',settlementtype, ' run number=', run_number, ' days to run=', days_to_run, ' replace=', replace, ' servicepoint output=', servicepoint_output)
    print()

    return current_dataaggrunid


# ┌─┐─┐ ┬┌─┐┌─┐┬ ┬┌┬┐┌─┐  ┬─┐┬ ┬┌┐┌     ┌┬┐┌─┐┬  ┌─┐┌┬┐┌─┐    ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌─┐┌─┐┬─┐┬ ┬┌┐┌    ┌─┐─┐ ┬┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐
# ├┤ ┌┴┬┘├┤ │  │ │ │ ├┤   ├┬┘│ ││││──────││├┤ │  ├┤  │ ├┤      ││├─┤ │ ├─┤├─┤│ ┬│ ┬├┬┘│ ││││    ├┤ ┌┴┬┘├┤ │  ├─┘├─┤│ ┬├┤
# └─┘┴ └─└─┘└─┘└─┘ ┴ └─┘  ┴└─└─┘┘└┘     ─┴┘└─┘┴─┘└─┘ ┴ └─┘─────┴┘┴ ┴ ┴ ┴ ┴┴ ┴└─┘└─┘┴└─└─┘┘└┘────└─┘┴ └─└─┘└─┘┴  ┴ ┴└─┘└─┘

@app.callback(Output('run-execution-delete', "children"),
              [Input('delete-run-button', 'n_clicks')],
              [State('running-dataaggrunid-dropdown', 'value')],
              prevent_initial_call=True,
              )
def delete_dataaggrun_execpage(n_clicks, dataaggrunid):
    procedure_out = cur2.var(str)
    cur2.callproc('DELETE_DATAAGGRUN', [dataaggrunid, procedure_out])
    dataaggrunid = dataaggrunid

    print('__________')
    print(datetime.now(), ' : The output from Data Agg Run ID ', dataaggrunid, ' was deleted.' , procedure_out.getvalue())
    print()
    
    return dcc.Markdown('''###### The output from Data Agg Run ID **{dataaggrunid}** was deleteded'''.format(dataaggrunid=dataaggrunid))


# ┌─┐─┐ ┬┌─┐┌─┐┬ ┬┌┬┐┌─┐  ┌─┐┬  ┌─┐    ┌─┐┌─┐┬  ┌─┐    ┌─┐┬  ┌─┐
# ├┤ ┌┴┬┘├┤ │  │ │ │ ├┤   ├─┘│  │──────│  ├─┤│  │      ├─┘│  │
# └─┘┴ └─└─┘└─┘└─┘ ┴ └─┘  ┴  ┴─┘└─┘    └─┘┴ ┴┴─┘└─┘────┴  ┴─┘└─┘

@app.callback(Output('plc-forecast-table', 'rows'),
              [Input('run-plc-forecast-button', 'n_clicks')],
              [State('coincident-dropdown', 'value'),
               State('non-coincident-dropdown', 'value'),
               ],
              )
def calc_plc(n_clicks, coincident_peaks, noncoincident_peaks):
    if n_clicks is not None:
        if n_clicks > 0:
            # check if dataaggrun record exists for input variables
            print()
            print('execute calc_plc procedure')
            print('coincident peaks dataaggrunids')
            print(coincident_peaks)
            print('noncoincident peaks dataaggrunids')
            print(noncoincident_peaks)
            coincident_peaks_string = ','.join(map(str, coincident_peaks))
            noncoincident_peaks_string = ','.join(map(str, noncoincident_peaks))
            # print('Coincident Peaks: ' + coincident_peaks_string)
            # print('Non-Coincident Peaks: ' + noncoincident_peaks_string)

            cur2.callproc('CALC_PLC', [coincident_peaks_string, noncoincident_peaks_string, ''])

            plcforecast_query = '''select * from plcforecast order by PLC desc fetch first 1000 rows only'''
            plcforecast = pd.read_sql(plcforecast_query, con=db)

            return plcforecast.to_dict('rows')


# # ┌─┐─┐ ┬┌─┐┌─┐┬ ┬┌┬┐┌─┐  ┌─┐┬  ┌─┐    ┌─┐┌─┐┬  ┌─┐    ┌─┐┬  ┌─┐
# # ├┤ ┌┴┬┘├┤ │  │ │ │ ├┤   ├─┘│  │──────│  ├─┤│  │      ├─┘│  │
# # └─┘┴ └─└─┘└─┘└─┘ ┴ └─┘  ┴  ┴─┘└─┘    └─┘┴ ┴┴─┘└─┘────┴  ┴─┘└─┘

@app.callback(Output('servicepointplchist-table', 'rows'),
              [Input('publish-plc-forecast-button', 'n_clicks')],
              [State('plc-effective-start-picker', 'date'),
               State('plc-effective-stop-picker', 'date')],
              )
def publish_plc(n_clicks, start, stop):
    if n_clicks is not None:
        if n_clicks > 0:
            print('publish_plc', str(start), str(stop))
            cur2.callproc('PUBLISH_PLC', ['Y', str(start), str(stop), ''])

            servicepointplchist_query = '''select * from servicepointplchist where inserttimestamp > systimestamp - 1/24 order by PLC desc fetch first 1000 rows only'''
            servicepointplchist = pd.read_sql(servicepointplchist_query, con=db)

            return servicepointplchist.to_dict('rows')


# ┬─┐┬ ┬┌┐┌  ┌─┐┬  ┬┌─┐┬  ┬ ┬┌─┐┌┬┐┬┌─┐┌┐┌      ┬─┐┬ ┬┌┐┌    ┌─┐┬  ┬┌─┐┬  ┬ ┬┌─┐┌┬┐┬┌─┐┌┐┌
# ├┬┘│ ││││  ├┤ └┐┌┘├─┤│  │ │├─┤ │ ││ ││││──────├┬┘│ ││││    ├┤ └┐┌┘├─┤│  │ │├─┤ │ ││ ││││
# ┴└─└─┘┘└┘  └─┘ └┘ ┴ ┴┴─┘└─┘┴ ┴ ┴ ┴└─┘┘└┘      ┴└─└─┘┘└┘────└─┘ └┘ ┴ ┴┴─┘└─┘┴ ┴ ┴ ┴└─┘┘└┘

@app.callback(
    Output('run-evaluation-container', "children"),
    [Input('analysis-run-dropdown', 'value')]
    )
def run_evaluation(dataaggrunid):
    # load data for dataaggrunid
    if dataaggrunid is None:
        return 'No runs available for review'
    else:
        dataaggrun_query = '''
        select * from dataaggrun where dataaggrunid = {dataaggrunid}'''.format(dataaggrunid=dataaggrunid)
        dataaggrun = pd.read_sql(dataaggrun_query, con=db)

        # check for the existence of the dataaggrun record
        if dataaggrun.shape[0] == 0:
            # no runs retrieved. Do not show graphs
            return 'No run selected'
        else:
            # get operating date
            local_tz = pytz.timezone('US/Eastern')
            operatingdate = dataaggrun['OPERATINGDATE'][0]
            # operatingdate_local = local_tz.localize(operatingdate)

            dst_dates = pd.DataFrame([local_tz._utc_transition_times]).transpose()
            dst_dates.rename({0: "TRANSITION_DATE"}, axis='columns', inplace=True)
            dst_dates.drop(dst_dates.index[0], inplace=True)
            dst_dates['TRUNCATED'] = pd.to_datetime(dst_dates['TRANSITION_DATE'], errors='coerce').dt.normalize()
            dst_dates = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

            if dst_dates.shape[0] == 1:
                if dst_dates.iloc[0][1].month <= 5:
                    hours_in_day = 23
                else:
                    hours_in_day = 25
            else:
                hours_in_day = 24

            # Retrieve dataaggrunhist records for selected dataaggrunid
            dataaggrunhist_query = '''
            select a.dataaggrunid, b.operatingdate, b.SETTLEMENTTYPE, b.runnumber, a.step, a.processname, a.status,
                   case elapsedtime
                    when null then '>>'||substr((systimestamp-PROCESSSTART) day to second (3),-15,12)
                    else substr(elapsedtime,-15,12)
                   end as elapsedtime,
                   c.avgelapsedtime, c.minelapsedtime, c.maxelapsedtime, a.processstart, a.processstop, a.comments
              from dataaggrunhist a
                   join dataaggrun b on a.dataaggrunid = b.dataaggrunid
                   left join
                       (select processname,
                               regexp_substr (numtodsinterval(avg(extract(day from elapsedtime) * 86400 + extract(hour from elapsedtime) * 3600 + extract(minute from elapsedtime) * 60 + extract(second from elapsedtime)),'second'), '\d{{2}}:\d{{2}}:\d{{2}}\.\d{{3}}') avgelapsedtime,
                               regexp_substr (numtodsinterval(min(extract(day from elapsedtime) * 86400 + extract(hour from elapsedtime) * 3600 + extract(minute from elapsedtime) * 60 + extract(second from elapsedtime)),'second'), '\d{{2}}:\d{{2}}:\d{{2}}\.\d{{3}}') minelapsedtime,
                               regexp_substr (numtodsinterval(max(extract(day from elapsedtime) * 86400 + extract(hour from elapsedtime) * 3600 + extract(minute from elapsedtime) * 60 + extract(second from elapsedtime)),'second'), '\d{{2}}:\d{{2}}:\d{{2}}\.\d{{3}}') maxelapsedtime
                          from dataaggrunhist
                         where status = 'COMPLETE' and processstart >= sysdate - 90
                         group by processname) c
                       on a.processname = c.processname
             where a.dataaggrunid = {dataaggrunid}
             order by a.dataaggrunid desc, a.step'''.format(dataaggrunid=dataaggrunid)
            dataaggrunhist = pd.read_sql(dataaggrunhist_query, con=db)

            # check for dataaggrunhist records for the selected dataaggrunid
            if dataaggrunhist.shape[0] == 0:
                # no runs retrieved. Do not show graphs
                return html.Div([

                    html.Div(id='dataaggrun-table-container'),

                    dash_table.DataTable(
                        id='dataaggrun-table',
                        columns=[
                            {"name": i, "id": i} for i in dataaggrun.columns
                        ],
                        data=dataaggrun.to_dict("rows"),
                        # row_selectable="single",
                        # selected_rows=[],
                        style_as_list_view=True,
                        style_cell={
                            # all three widths are needed
                            'minWidth': '120px',
                            'whiteSpace': 'no-wrap',
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                        },
                        style_data={
                            'backgroundColor': lightcolor,
                            'border': '0px solid white',
                            'fontColor': 'black',
                            'borderTop': '2px solid ' + darkcolor,
                            },
                        style_header={
                            'backgroundColor': lightcolor,
                            'fontWeight': 'bold',
                            'border': '0px solid white',
                            },
                        css=[{
                            'selector': '.dash-cell div.dash-cell-value',
                            'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                            }],
                        # export_format='xlsx',
                        # export_headers='display',
                        # merge_duplicate_headers=True
                    ),
                ])
            else:
                # Retrieve dba scheduler job run details records
                dba_scheduler_job_run_details_query = '''
                select LOG_ID, LOG_DATE, JOB_NAME, OUTPUT, STATUS, ERROR#, ERRORS, REQ_START_DATE, ACTUAL_START_DATE, RUN_DURATION, INSTANCE_ID, SESSION_ID, SLAVE_PID, CPU_USED, ADDITIONAL_INFO
                  from dba_scheduler_job_run_details
                 where job_name like 'DATAAGG%'
                       and output = 'DATAAGGRUNID ' || to_char({dataaggrunid})
                 order by actual_start_date'''.format(dataaggrunid=dataaggrunid)
                dba_scheduler_job_run_details = pd.read_sql(dba_scheduler_job_run_details_query, con=db)

                # Retrieve recent dba scheduler job run details records
                # dba_scheduler_job_run_recent_query = '''
                # select LOG_ID, LOG_DATE, OWNER, JOB_NAME, JOB_SUBNAME, STATUS, ERROR#, REQ_START_DATE, ACTUAL_START_DATE, RUN_DURATION, INSTANCE_ID, SESSION_ID, SLAVE_PID, CPU_USED, ADDITIONAL_INFO, ERRORS, OUTPUT
                #   from dba_scheduler_job_run_details
                # where job_name like 'DATAAGG%'
                #       and actual_start_date >= trunc(sysdate) - 4
                # order by actual_start_date'''
                # dba_scheduler_job_run_recent = pd.read_sql(dba_scheduler_job_run_recent_query, con=db)

                dataaggrunhist['STEP'] = dataaggrunhist['STEP'].round(2)
                # if the process isn't complete, just show the dataaggrun and dataaggrunhist tables
                status = dataaggrunhist.loc[dataaggrunhist['PROCESSNAME'] == 'DATA AGGREGATION']['STATUS'].values[0]

                processname_index = dataaggrunhist.loc[dataaggrunhist['PROCESSNAME'] == 'DATA AGGREGATION'].index.values

                if status != 'COMPLETE':
                    return html.Div([

                        html.Div(id='dataaggrun-table-container'),

                        dash_table.DataTable(
                            id='dataaggrun-table',
                            columns=[
                                {"name": i, "id": i} for i in dataaggrun.columns
                            ],
                            data=dataaggrun.to_dict("rows"),
                            # row_selectable="single",
                            # selected_rows=[],
                            style_as_list_view=True,
                            style_cell={
                                # all three widths are needed
                                'minWidth': '120px',
                                'whiteSpace': 'no-wrap',
                                'overflow': 'hidden',
                                'textOverflow': 'ellipsis',
                            },
                            style_data={
                                'backgroundColor': lightcolor,
                                'border': '0px solid white',
                                'fontColor': 'black',
                                'borderTop': '2px solid ' + darkcolor,
                                },
                            style_header={
                                'backgroundColor': lightcolor,
                                'fontWeight': 'bold',
                                'border': '0px solid white',
                                },
                            css=[{
                                'selector': '.dash-cell div.dash-cell-value',
                                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                }],
                            # export_format='xlsx',
                            # export_headers='display',
                            # merge_duplicate_headers=True
                        ),

                        html.Div(id='dataaggrunhist-table-container'),

                        html.H6('Analysis Performance Statistics', style={'textAlign': 'center'}),

                        dash_table.DataTable(
                            id='dataaggrunhist-table',
                            columns=[
                                {"name": i, "id": i} for i in dataaggrunhist.columns
                            ],
                            data=dataaggrunhist.to_dict("rows"),
                            style_table={'overflowX': 'scroll'},
                            style_as_list_view=True,
                            style_cell={
                                # all three widths are needed
                                'minWidth': '120px',
                                'whiteSpace': 'no-wrap',
                                'overflow': 'hidden',
                                'textOverflow': 'ellipsis',
                            },
                            style_data={
                                'backgroundColor': lightcolor,
                                'border': '0px solid white',
                                'fontColor': 'black',
                                'borderTop': '2px solid ' + darkcolor,
                                },
                            style_header={
                                'backgroundColor': lightcolor,
                                'fontWeight': 'bold',
                                'border': '0px solid white',
                                },
                            style_data_conditional=[
                                {'if': {'filter_query': '{STATUS} eq "START"'},
                                 'color': 'rgb(125, 50, 50)',
                                 'border': '2px solid white',
                                 },
                                {'if': {'filter_query': '{PROCESSNAME} eq "DATA AGGREGATION"'},
                                 'borderTop': '1px solid ' + darkcolor,
                                 'borderBottom': '1px solid ' + darkcolor,
                                 'backgroundColor': mediumlightcolor,
                                 },
                                {'if': {'filter_query': '{STATUS} eq "FAILURE"'},
                                 'backgroundColor': 'rgb(130, 75, 75)',
                                 'color': 'white',
                                 'fontWeight': 'bold',
                                 },
                                {'if': {'filter_query': '{PROCESSNAME} contains "DELETE"'},
                                 # 'backgroundColor': 'rgb(130, 75, 75)',
                                 'color': '#505050',
                                 # 'fontWeight': 'italic',
                                 }
                            ],
                            css=[{
                                'selector': '.dash-cell div.dash-cell-value',
                                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                }],
                            export_format='xlsx',
                            export_headers='display',
                            merge_duplicate_headers=True
                        ),

                        html.Div(id='oracle-job-container'),

                        html.H6('DBA Scheduler Job Run Details', style={'textAlign': 'center'}),

                        dash_table.DataTable(
                            id='dba-scheduler-job-run-details-table',
                            columns=[
                                {"name": i, "id": i} for i in dba_scheduler_job_run_details.columns
                            ],
                            data=dba_scheduler_job_run_details.to_dict("rows"),
                            style_table={'overflowX': 'scroll'},
                            style_as_list_view=True,
                            style_cell={
                                # all three widths are needed
                                'minWidth': '120px',
                                'whiteSpace': 'no-wrap',
                                'overflow': 'hidden',
                                'textOverflow': 'ellipsis',
                            },
                            style_data={'backgroundColor': lightcolor,
                                        'border': '0px solid white',
                                        'fontColor': 'black',
                                        'borderTop': '2px solid ' + darkcolor,
                                        },
                            style_header={
                                'backgroundColor': lightcolor,
                                'fontWeight': 'bold',
                                'border': '0px solid white',
                                },
                            css=[{
                                'selector': '.dash-cell div.dash-cell-value',
                                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                }],
                            # export_format='xlsx',
                            # export_headers='display',
                            # merge_duplicate_headers=True
                        ),

                        # html.H6('DBA Scheduler Recent Job Run Details', style={'textAlign':'center'}),

                        # dash_table.DataTable(
                        #     id='dba-scheduler-recent-job-run-details-table',
                        #     columns=[
                        #         {"name": i, "id": i} for i in dba_scheduler_job_run_recent.columns
                        #     ],
                        #     data=dba_scheduler_job_run_recent.to_dict("rows"),
                        #     style_table={'overflowX': 'scroll',  },
                        #     style_as_list_view=True,
                        #     style_cell={
                        #         # all three widths are needed
                        #         'minWidth': '120px', #'width': '180px', 'maxWidth': '180px',
                        #         'whiteSpace': 'no-wrap',
                        #         'overflow': 'hidden',
                        #         'textOverflow': 'ellipsis',
                        #     },
                        #     style_cell_conditional=[
                        #             {
                        #         'if': {'row_index': 'odd'},
                        #         'backgroundColor': lightcolor
                        #         }
                        #         ],
                        #     style_header={
                        #         'backgroundColor': darkcolor,
                        #         'fontWeight': 'bold',
                        #         'color': 'white'
                        #         },
                        #     css=[{
                        #         'selector': '.dash-cell div.dash-cell-value',
                        #         'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                        #         }],
                        # ),

                    ])
                else:
                    #
                    # Retrieve dataagginterval records for selected dataaggrunids
                    dataagginterval_query = '''
                    select /*+ parallel */ * from dataagginterval where dataaggrunid = {dataaggrunid}
                     order by MARKET, RETAILER, DISCO, PROFILECLASS, ufezone, UFEZONE, LMPBUS, DEMANDRESPONSEZONE, RATEFACTOR, CUSTOMERCLASS, DYNAMICPRICING, DIRECTLOADCONTROL, METERTYPE, WEATHERSENSITIVITY, WEATHERZONE, METHOD'''.format(dataaggrunid=dataaggrunid)
                    dataagginterval = pd.read_sql(dataagginterval_query, con=db)

                    dataaggreport_query = '''
                    select * from dataaggreport
                     where starttime <= (select operatingdate from dataaggrun where dataaggrunid = {dataaggrunid})
                           and (stoptime is null or stoptime >= (select operatingdate from dataaggrun where dataaggrunid = {dataaggrunid}))
                     order by dataaggreport'''.format(dataaggrunid=dataaggrunid)
                    dataaggreport_df = pd.read_sql(dataaggreport_query, con=db)

                    same_day_ufe_pct_query = '''
                    select /*+ parallel */ a.*, b.settlementtype, b.runnumber from dataagginterval a join dataaggrun b on a.dataaggrunid = b.dataaggrunid where a.dataaggreport = 'UFEZONE UFE PERCENT'
                    and a.dataaggrunid <> {dataaggrunid} and b.operatingdate = (select operatingdate from dataaggrun where dataaggrunid = {dataaggrunid})'''.format(dataaggrunid=dataaggrunid)
                    same_day_ufe_pct = pd.read_sql(same_day_ufe_pct_query, con=db)

                    interval_cols = []

                    for i in range(0, hours_in_day):
                        # interval_start = (operatingdate_local + timedelta(hours=i))
                        # interval_start_dst = interval_start.astimezone(local_tz).strftime("%H:%M")
                        if dst_dates.shape[0] == 1:
                            if operatingdate.month > 6:
                                # fall DST transition date
                                if i < 2:
                                    interval_label = str(i).zfill(2) + ':00'
                                elif i == 2:  # interval_start in transitiondates:
                                    interval_label = str(i-1).zfill(2) + ':00*'
                                elif i > 2:
                                    interval_label = str(i-1).zfill(2) + ':00'
                            else:
                                # spring DST transition date
                                if i < 2:
                                    interval_label = str(i).zfill(2) + ':00'
                                else:
                                    interval_label = str(i+1).zfill(2) + ':00'
                        else:
                            # normal non DST transition day
                            interval_label = str(i).zfill(2) + ':00'

                        interval_cols.append(interval_label)
                        col_name = f"T{i:0>3}"

                        dataagginterval.rename(columns={col_name: interval_label}, inplace=True)
                        same_day_ufe_pct.rename(columns={col_name: interval_label}, inplace=True)

                    dataagginterval['DATAAGGRUNID_STR'] = dataagginterval['DATAAGGRUNID'].astype(str)

                    if hours_in_day < 25:
                        dataagginterval.drop(['T024'], axis=1, inplace=True)

                    if hours_in_day < 24:
                        dataagginterval.drop(['T023'], axis=1, inplace=True)

                    dataagginterval['AGGCHARS'] = dataagginterval[['MARKET', 'RETAILER', 'DISCO', 'PROFILECLASS', 'LOSSCLASS', 'UFEZONE', 'LMPBUS', 'DEMANDRESPONSEZONE', 'RATEFACTOR', 'CUSTOMERCLASS', 'DYNAMICPRICING', 'DIRECTLOADCONTROL', 'METERTYPE', 'WEATHERSENSITIVITY', 'WEATHERZONE', 'METHOD', 'DATATYPE', 'CALCULATIONTYPE']].apply(lambda x: x.str.cat(sep=' | '), axis=1)
                    # print(dataagginterval)
                    dataagginterval.set_index(['AGGCHARS'], inplace=True)

                    colors_dic = {'Interval | Actual': 'rgba( 38,  24,  74, 0.7)',
                                  'Interval | Estimated': 'rgba( 71,  58, 131, 0.7)',
                                  'Interval | Default': 'rgba(122, 120, 168, 0.7)',
                                  'Scalar | Actual': 'rgba( 74,  24,  38, 0.7)',
                                  'Scalar | Estimated': 'rgba(131,  58,  71, 0.7)',
                                  'Scalar | Default': 'rgba(168, 120, 122, 0.7)', }

                    # ufe_dic = {'UFE ZONE 1': 'UFE Zone 1',
                    #            'UFE ZONE 2': 'UFE Zone 2',
                    #            'UFE ZONE 3': 'UFE Zone 3'}

                    # report metertype method percentages
                    metertypemethod = dataagginterval.loc[(dataagginterval['DATAAGGREPORT'] == 'METERTYPE METHOD UFEZONE UNADJ') & (dataagginterval['DATATYPE'] == 'UNADJ')].sort_values(['DATAAGGRUNID', 'METERTYPE', 'METHOD', 'UFEZONE'])
                    metertypemethod['METERTYPEMETHOD'] = metertypemethod['METERTYPE'].str.title() + ' | ' + metertypemethod['METHOD'].str.title()
                    metertypemethod['KWH_PERCENT'] = metertypemethod['TOTAL'].div(metertypemethod.groupby(['UFEZONE', 'DATAAGGRUNID'])['TOTAL'].transform('sum'))
                    metertypemethod['SPCOUNT_PERCENT'] = metertypemethod['SPCOUNT'].div(metertypemethod.groupby(['UFEZONE', 'DATAAGGRUNID'])['SPCOUNT'].transform('sum'))
                    metertypemethod = metertypemethod[['METERTYPEMETHOD', 'KWH_PERCENT', 'SPCOUNT_PERCENT', 'UFEZONE', 'TOTAL', 'SPCOUNT', 'DATAAGGRUNID']]
                    metertypemethod['UFEZONEMETERTYPEMETHOD'] = metertypemethod['DATAAGGRUNID'].astype(str) + ' | ' + metertypemethod['UFEZONE'] + ' | ' + metertypemethod['METERTYPEMETHOD']
                    metertypemethod.sort_values(['UFEZONEMETERTYPEMETHOD'])
                    metertypemethod.set_index(['UFEZONEMETERTYPEMETHOD'], inplace=True)
                    metertypemethod['COLOR'] = metertypemethod['METERTYPEMETHOD'].map(colors_dic)
                    metertypemethod['KWHTEXT'] = metertypemethod['METERTYPEMETHOD'] + ' | kWh' + '<br>' + metertypemethod['KWH_PERCENT'].map('{:,.2%}'.format) + ' (' + metertypemethod['TOTAL'].map('{:,.0f}'.format) + ')'
                    metertypemethod['COUNTTEXT'] = metertypemethod['METERTYPEMETHOD'] + ' | Count' + '<br>' + metertypemethod['SPCOUNT_PERCENT'].map('{:,.2%}'.format) + ' (' + metertypemethod['SPCOUNT'].map('{:,.0f}'.format) + ')'

                    # graph it
                    blank_trace = go.Bar(
                            x=[0, 0, 0, 0, 0, 0],
                            y=['']*6,
                            marker=dict(color=list(colors_dic.values()),
                                        line=dict(color='rgb(255, 255, 255)',
                                                  width=1),
                                        ),
                            orientation='h',
                            width=0.1,
                            hoverinfo='none',
                            )

                    kwh_trace = go.Bar(
                            x=metertypemethod['KWH_PERCENT'].map('{:,.1%}'.format),
                            # y=metertypemethod['UFEZONE'].map(ufe_dic) + '<br>Total kWh',
                            y=metertypemethod['UFEZONE'] + '<br>Total kWh',
                            text=metertypemethod['KWHTEXT'],
                            textposition='inside',
                            marker=dict(color=metertypemethod['COLOR'],
                                        line=dict(color='rgb(255, 255, 255)',
                                                  width=1)),
                            orientation='h',
                            width=1.0,
                            hoverinfo='text',
                            )

                    count_trace = go.Bar(
                            x=metertypemethod['SPCOUNT_PERCENT'].map('{:,.1%}'.format),
                            # y=metertypemethod['UFEZONE'].map(ufe_dic) + '<br>Count',
                            y=metertypemethod['UFEZONE'] + '<br>Count',
                            text=metertypemethod['COUNTTEXT'],
                            textposition='inside',
                            marker=dict(color=metertypemethod['COLOR'],
                                        line=dict(color='rgb(255, 255, 255)',
                                                  width=1)),
                            orientation='h',
                            width=1.0,
                            hoverinfo='text',
                            )

                    kwh_layout = go.Layout(
                            barmode='stack',
                            title='Total Kwh and Count by Meter Type and Method',
                            yaxis=dict(side='left',
                                       hoverformat=',.0f',
                                       tickformat=',.3s',
                                       showgrid=False,
                                       ),
                            xaxis=dict(nticks=10,
                                       tick0=0,
                                       dtick=10,
                                       tickangle=-45),
                            showlegend=False,
                            hovermode='closest',
                            hoverlabel=dict(namelength=-1),
                            height=500,
                            paper_bgcolor=paperbackgroundcolor,
                            plot_bgcolor=plotbackgroundcolor,
                            legend_bgcolor=legendbackgroundcolor,
                            )

                    # ________________________________________________________________________________________________________________

                    ufe_percent = dataagginterval[dataagginterval['DATAAGGREPORT'].isin(['UFEZONE UFE PERCENT'])].copy()

                    # ufe_percent['UFE Zone'] = ufe_percent['UFEZONE'].map(ufe_dic)
                    #ufe_percent.set_index(['UFE Zone'], inplace=True)
                    ufe_percent.set_index(['UFEZONE'], inplace=True)
                    ufe_percent.sort_index(axis=1, inplace=True)

                    ufe_percent_chart = ufe_percent[interval_cols].transpose()
                    ufe_percent_chart.sort_index(axis=1, inplace=True)

                    ufe_avg_percent_series = ufe_percent_chart.mean(axis=0)
                    ufe_avg_percent_series.sort_index(axis=0, inplace=True)

                    # ufe_avg_percent = pd.DataFrame({'UFE Zone': ufe_avg_percent_series.index, 'Average UFE %': ufe_avg_percent_series.values})
                    ufe_avg_percent = pd.DataFrame({'UFEZONE': ufe_avg_percent_series.index, 'Average UFE %': ufe_avg_percent_series.values})

                    # same_day_ufe_pct['UFE Zone'] = same_day_ufe_pct['UFEZONE'].map(ufe_dic)
                    same_day_ufe_pct['MYINDEX'] = 'DATAAGGRUNID ' + same_day_ufe_pct['DATAAGGRUNID'].astype(str) + ' | ' + same_day_ufe_pct['UFEZONE'] #.map(ufe_dic)
                    same_day_ufe_pct.set_index(['MYINDEX'], inplace=True)
                    same_day_ufe_pct.sort_index(axis=1, inplace=True)

                    same_day_ufe_pct_chart = same_day_ufe_pct[interval_cols].transpose()
                    same_day_ufe_pct_chart.sort_index(axis=1, inplace=True)

                    system_load = dataagginterval[(dataagginterval['DATAAGGREPORT'].isin(['UFEZONE TOTAL UFEADJ']))].copy()
                    # system_load['UFE Zone'] = system_load['UFEZONE'].map(ufe_dic)
                    # system_load.set_index(['UFE Zone'], inplace=True)
                    system_load.set_index(['UFEZONE'], inplace=True)
                    system_load_stats = system_load[['SPCOUNT', 'SUMPLC', 'SUMTPLC', 'TOTAL']].copy()
                    system_load_stats.sort_index(axis=0, inplace=True)

                    system_load_chart = system_load[interval_cols].transpose().copy()
                    system_load_chart.sort_index(axis=1, inplace=True)
                    peak_load = system_load_chart.max()
                    peak_load_df = peak_load.to_frame()
                    peak_load_df.columns = ["Peak Load"]

                    # merged_stats = pd.merge(left=system_load_stats, right=peak_load_df, left_on='UFE Zone', right_on='UFE Zone')
                    merged_stats = pd.merge(left=system_load_stats, right=peak_load_df, left_on='UFEZONE', right_on='UFEZONE')
                    merged_stats.columns = ['Service Point Count', 'Sum PLC', 'Sum TPLC', 'Total Daily kWh', 'Peak Load']

                    # merged_stats = pd.merge(left=ufe_avg_percent, right=merged_stats, left_on='UFE Zone', right_on='UFE Zone')
                    merged_stats = pd.merge(left=ufe_avg_percent, right=merged_stats, left_on='UFEZONE', right_on='UFEZONE')

                    pct_traces = []

                    for i in ufe_percent_chart.columns:
                        trace = go.Scattergl(
                            x=ufe_percent_chart.index,
                            y=ufe_percent_chart[i],
                            yaxis='y',
                            mode='lines',
                            name=i,
                            )
                        pct_traces.append(trace)

                    for i in same_day_ufe_pct_chart.columns:
                        trace = go.Scattergl(
                            x=same_day_ufe_pct_chart.index,
                            y=same_day_ufe_pct_chart[i],
                            yaxis='y',
                            mode='lines',
                            name=i,
                            line=dict(width=2, color='rgba(140,140,140,.4)', shape='linear')
                            )
                        pct_traces.append(trace)

                    pct_layout = go.Layout(
                        title='UFE Percent',
                        yaxis={'title': 'UFE %',
                               'side': 'left',
                               'tickformat': '.1%'},
                        xaxis={'showgrid': False},
                        legend=dict(orientation='v'),
                        # xaxis=dict(nticks=12), #tickangle = -45),
                        hovermode='closest',
                        hoverlabel=dict(namelength=-1),
                        paper_bgcolor=paperbackgroundcolor,
                        plot_bgcolor=plotbackgroundcolor,
                        legend_bgcolor=legendbackgroundcolor,
                        )

                    # ________________________________________________________________________________________________________________

                    # UFE traces by UFEZONE
                    ufe_components = dataagginterval[(dataagginterval['DATAAGGREPORT'].isin(['UFEZONE SYSTEM LOAD', 'UFEZONE TOTAL TRANLOSSADJ']))].copy().sort_values(by='UFEZONE')
                    ufe_components['MYINDEX'] = ufe_components['DATAAGGREPORT'].str.replace('UFEZONE ', '') + ' | ' + ufe_components['UFEZONE']
                    ufe_components.set_index(['MYINDEX'], inplace=True)

                    ufe_zones = ufe_components['UFEZONE'].unique()
                    ufe_zones_list = ufe_zones.tolist()

                    reports_df = pd.DataFrame(enumerate(ufe_components['DATAAGGREPORT'].unique(), start=1), columns=['COUNTER', 'DATAAGGREPORT'])

                    # make subplots by UFE Zone
                    ufe_fig = subplots.make_subplots(
                        rows=1, cols=3,
                        shared_xaxes=False, shared_yaxes=True,
                        horizontal_spacing=0.02,
                        subplot_titles=ufe_zones_list
                        )

                    for counter, zone in enumerate(ufe_zones, start=1):
                        # filter by UFE Zone
                        filtered_data = ufe_components[ufe_components['UFEZONE'] == zone].sort_values(by='UFEZONE', ascending=False)
                        chart_data = filtered_data[interval_cols].transpose()

                        for i in chart_data.columns:
                            stuff = chart_data[i].name.split(' | ', 2)
                            dataaggreport = 'UFEZONE ' + stuff[0]

                            report_trace = go.Scattergl(
                                x=chart_data[i].index,
                                y=chart_data[i],
                                yaxis='y',
                                mode='lines',
                                opacity=1,
                                name=i,
                                legendgroup=int(reports_df.loc[reports_df['DATAAGGREPORT'] == dataaggreport, 'COUNTER'].iloc[0]),
                                )
                            ufe_fig.append_trace(report_trace, 1, counter)

                    ufe_fig['layout'].update(
                        height=500,
                        title='UFE Components by UFE Zone',
                        paper_bgcolor=paperbackgroundcolor,
                        plot_bgcolor=plotbackgroundcolor,
                        legend_bgcolor=legendbackgroundcolor,
                        xaxis={'showgrid': False},
                        xaxis2={'showgrid': False},
                        xaxis3={'showgrid': False},
                        )

                    # ________________________________________________________________________________________________________________

                    # Create graphs for reports with WEBREPORT = 'Y'

                    # get list of WEBREPORT = 'Y' reports
                    # webreport_list = dataaggreport[(dataaggreport['WEBREPORT'].isin('Y'))].copy()  # .sort_values(by='WEBORDER')
                    webreport_df = dataaggreport_df[dataaggreport_df.WEBREPORT.eq('Y')].copy().sort_values(by='WEBORDER')
                    dynamic_chart_list = []

                    for index, row in webreport_df.iterrows():
                        report_name = row['DATAAGGREPORT']
                        splitby_field = row['SPLITBY']
                        # print(row)

                        # subset data for report
                        report_components = dataagginterval[(dataagginterval['DATAAGGREPORT'] == report_name)].copy()

                        if report_components.empty:
                            continue

                        if splitby_field is not None:
                            splitby_df = pd.DataFrame(report_components[splitby_field].unique(), columns=['COLUMN'])
                            splitby_count = len(splitby_df)

                            if splitby_count == 0:
                                # nothing to split
                                # continue
                                column_number = 1
                            elif splitby_count == 1:
                                # Just need one chart
                                column_number = 1
                            elif (splitby_count == 2 or splitby_count == 4):
                                # create 2-wide charts
                                column_number = 2
                            else:
                                # create 3-wide charts
                                column_number = 3

                            chart_divmod = divmod(splitby_count, column_number)
                            splitby_fig = subplots.make_subplots(
                                rows=int(chart_divmod[0]), cols=column_number,
                                shared_xaxes=False, shared_yaxes=True,
                                horizontal_spacing=0.02,
                                subplot_titles=splitby_df['COLUMN'])

                            for counter, column in enumerate(splitby_df['COLUMN'], start=1):
                                # filter by splitby variable
                                filtered_data = report_components[report_components[splitby_field] == column].sort_values(by=splitby_field, ascending=False)
                                chart_data = filtered_data[interval_cols].transpose()

                                for i in chart_data.columns:
                                    chart_row = (counter + column_number - 1)//column_number
                                    chart_column = (counter + column_number - 1) % column_number + 1

                                    splitby_trace = go.Scatter(
                                        x=chart_data[i].index,
                                        y=chart_data[i],
                                        yaxis='y',
                                        mode='lines',
                                        opacity=1,
                                        name=i,
                                        # legendgroup=int(filtered_data.loc[filtered_data[splitby_field] == column, 'COUNTER'].iloc[0]),
                                        )
                                    splitby_fig.append_trace(splitby_trace, chart_row, chart_column)

                            splitby_fig['layout'].update(
                                height=500 + 200 * (int(chart_divmod[0]) - 1),
                                title=report_name,
                                paper_bgcolor=paperbackgroundcolor,
                                plot_bgcolor=plotbackgroundcolor,
                                legend_bgcolor=legendbackgroundcolor,
                                # xaxis={'showgrid': False},
                                # xaxis2={'showgrid': False},
                                # xaxis3={'showgrid': False},
                                # xaxis4={'showgrid': False},
                            )

                            dynamic_chart_list.append(splitby_fig)

                        else:
                            splitby_fig = subplots.make_subplots(
                                rows=1, cols=1,
                                shared_xaxes=False, shared_yaxes=True,
                                horizontal_spacing=0.02,
                                # subplot_titles=report_name
                                )

                            chart_data = report_components[interval_cols].transpose()

                            for i in chart_data.columns:
                                chart_row = 1
                                chart_column = 1

                                splitby_trace = go.Scatter(
                                    x=chart_data[i].index,
                                    y=chart_data[i],
                                    yaxis='y',
                                    mode='lines',
                                    opacity=1,
                                    name=i,
                                    # legendgroup=int(splitby_df.loc[splitby_df[splitby_field] == column, 'COUNTER'].iloc[0]),
                                    )
                                splitby_fig.append_trace(splitby_trace, chart_row, chart_column)

                            splitby_fig['layout'].update(
                                height=500,
                                title=report_name,
                                paper_bgcolor=paperbackgroundcolor,
                                plot_bgcolor=plotbackgroundcolor,
                                legend_bgcolor=legendbackgroundcolor,
                                # xaxis={'showgrid': False},
                                # xaxis2={'showgrid': False},
                                # xaxis3={'showgrid': False},
                                # xaxis4={'showgrid': False},
                            )

                            dynamic_chart_list.append(splitby_fig)

                    # ________________________________________________________________________________________________________________

                    # # Retailer traces by UFEZONE
                    # retailer_components = dataagginterval[(dataagginterval['DATAAGGREPORT'].isin(['RETAILER UFEZONE UFEADJ']))].copy().sort_values(by='UFEZONE')
                    # retailer_components['MYINDEX'] = retailer_components['RETAILER'] + ' | ' + retailer_components['UFEZONE']
                    # retailer_components.set_index(['MYINDEX'], inplace=True)

                    # ufe_zones = retailer_components['UFEZONE'].unique()
                    # # print(ufe_zones)
                    # retailers_df = pd.DataFrame(enumerate(retailer_components['RETAILER'].unique(), start=1), columns=['COUNTER', 'RETAILER'])

                    # # make subplots by UFE Zone
                    # retailer_fig = subplots.make_subplots(
                    #     rows=1, cols=3,
                    #     shared_xaxes=False, shared_yaxes=True,
                    #     horizontal_spacing=0.005,
                    #     subplot_titles=ufe_zones_list)

                    # for counter, zone in enumerate(ufe_zones, start=1):
                    #     # filter by UFE Zone
                    #     filtered_data = retailer_components[retailer_components['UFEZONE'] == zone].sort_values(by='TOTAL', ascending=False)
                    #     chart_data = filtered_data[interval_cols].transpose()

                    #     for i in chart_data.columns:
                    #         stuff = chart_data[i].name.split(' | ', 2)

                    #         retailer = stuff[0]

                    #         retailer_trace = go.Scatter(
                    #             x=chart_data[i].index,
                    #             y=chart_data[i],
                    #             yaxis='y',
                    #             mode='lines',
                    #             opacity=1,
                    #             name=i,
                    #             legendgroup=int(retailers_df.loc[retailers_df['RETAILER'] == retailer, 'COUNTER'].iloc[0]),
                    #             )
                    #         retailer_fig.append_trace(retailer_trace, 1, counter)

                    # retailer_fig['layout'].update(
                    #     height=500,
                    #     title='Retailer by UFE Zone',
                    #     paper_bgcolor=paperbackgroundcolor,
                    #     plot_bgcolor=plotbackgroundcolor,
                    #     legend_bgcolor=legendbackgroundcolor,
                    #     xaxis={'showgrid': False},
                    #     xaxis2={'showgrid': False},
                    #     xaxis3={'showgrid': False},
                    #     )

                    # ________________________________________________________________________________________________________________

                    # highlight data aggregation row in dataaggrunhist table

                    fig = go.Figure()

                    fig.add_trace(
                        go.Indicator(
                            value=200,
                            delta={'reference': 160},
                            gauge={'axis': {'visible': False}},
                            domain={'row': 0, 'column': 0}
                            )
                        )

                    fig.add_trace(
                        go.Indicator(
                            value=120,
                            gauge={
                                'shape': "bullet",
                                'axis': {'visible': False}},
                            domain={'x': [0.05, 0.5], 'y': [0.15, 0.35]}
                            )
                        )

                    fig.add_trace(
                        go.Indicator(
                            mode="number+delta",
                            value=300,
                            domain={'row': 0, 'column': 1}
                            )
                        )

                    fig.add_trace(
                        go.Indicator(
                            mode="delta",
                            value=40,
                            domain={'row': 1, 'column': 1}
                            )
                        )

                    return html.Div([
                        html.Div(id='dataaggrun-table-container'),

                        dash_table.DataTable(
                            id='dataaggrun-table',
                            columns=[{"name": i, "id": i} for i in dataaggrun.columns],
                            data=dataaggrun.to_dict("rows"),
                            style_as_list_view=True,
                            style_cell={
                                # all three widths are needed
                                'minWidth': '120px',  # 'width': '180px', 'maxWidth': '180px',
                                'whiteSpace': 'no-wrap',
                                'overflow': 'hidden',
                                'textOverflow': 'ellipsis',
                            },
                            style_data={'backgroundColor': lightcolor,
                                        'border': '0px solid white',
                                        'fontColor': 'black',
                                        'borderTop': '2px solid ' + darkcolor,
                                        },
                            style_header={
                                'backgroundColor': lightcolor,
                                'fontWeight': 'bold',
                                'border': '0px solid white',
                                },
                            css=[{
                                'selector': '.dash-cell div.dash-cell-value',
                                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                }],
                        ),

                        html.Div([
                            dash_table.DataTable(
                                id='merged-stats',
                                # columns=[{"name": i, "id": i, "type": "numeric", "format": FormatTemplate.money(0).symbol('')} for i in merged_stats.columns],
                                columns=[
                                    {"name": "UFE Zone", "id": "UFE Zone", "type": "text", },
                                    {"name": "Average UFE %", "id": "Average UFE %", "type": "numeric", "format": FormatTemplate.percentage(2)},
                                    {"name": "Service Point Count", "id": "Service Point Count", "type": "numeric", "format": FormatTemplate.money(0).symbol('')},
                                    {"name": "Sum PLC", "id": "Sum PLC", "type": "numeric", "format": FormatTemplate.money(0).symbol('')},
                                    {"name": "Sum TPLC", "id": "Sum TPLC", "type": "numeric", "format": FormatTemplate.money(0).symbol('')},
                                    {"name": "Total Daily kWh", "id": "Total Daily kWh", "type": "numeric", "format": FormatTemplate.money(0).symbol('')},
                                    {"name": "Peak Load", "id": "Peak Load", "type": "numeric", "format": FormatTemplate.money(0).symbol('')},
                                    ],
                                data=merged_stats.to_dict("rows"),
                                style_as_list_view=True,
                                style_cell={
                                    # all three widths are needed
                                    'minWidth': '120px',  # 'width': '180px', 'maxWidth': '180px',
                                    'whiteSpace': 'no-wrap',
                                    'overflow': 'hidden',
                                    'textOverflow': 'ellipsis',
                                    },
                                style_data={
                                    'backgroundColor': lightcolor,
                                    'border': '0px solid white',
                                    'fontColor': 'black',
                                    'borderTop': '2px solid ' + darkcolor,
                                    },
                                style_header={
                                    'backgroundColor': lightcolor,
                                    'fontWeight': 'bold',
                                    'border': '0px solid white',
                                    },
                                css=[{
                                    'selector': '.dash-cell div.dash-cell-value',
                                    'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                    }],
                            ),
                            ], style={'display': 'inline-block',
                                      'width': '100%',
                                      'paper_bgcolor': 'rgba(127, 127, 177, .2)',
                                      'border': '0px solid white',
                                      # 'outline': '5px solid white',
                                      'margin': '30 px'}),

                        html.Div(id='dataaggrun-records-container'),
                        html.Div([
                            dcc.Graph(
                                id='ufe_percent',
                                figure={
                                    'data': pct_traces,
                                    'layout': pct_layout
                                    },
                                ),
                            ], style={'display': 'inline-block', 'width': '100%', 'height': 450}),

                        html.Div([dcc.Graph(figure=ufe_fig, id='ufe-subplots')]),

                        # html.Div([dcc.Graph(figure=retailer_fig, id='retailer-subplots')]),

                        html.Div([dcc.Graph(figure=list_fig) for list_fig in dynamic_chart_list]),

                        html.Div([
                            dcc.Graph(
                                id='metertype-method-graph1',
                                figure={'data': [count_trace, blank_trace, kwh_trace, ],
                                        'layout': kwh_layout
                                        }
                                )
                            ], style={'display': 'inline-block', 'width': '100%', 'height': 450}),

                        html.Div(id='dataaggrunhist-table-container'),

                        html.H6('Analysis Performance Statistics', style={'textAlign': 'center'}),

                        dash_table.DataTable(
                            id='dataaggrunhist-table',
                            columns=[{"name": i, "id": i} for i in dataaggrunhist.columns],
                            data=dataaggrunhist.to_dict("rows"),
                            style_table={'overflowX': 'scroll'},
                            style_as_list_view=True,
                            style_cell={
                                # all three widths are needed
                                'minWidth': '120px',  # 'width': '180px', 'maxWidth': '180px',
                                'whiteSpace': 'no-wrap',
                                'overflow': 'hidden',
                                'textOverflow': 'ellipsis',
                            },
                            style_data={'backgroundColor': lightcolor,
                                        'border': '0px solid white',
                                        'fontColor': 'black',
                                        'borderTop': '2px solid ' + darkcolor,
                                        },
                            style_header={
                                'backgroundColor': lightcolor,
                                'fontWeight': 'bold',
                                'border': '0px solid white',
                                },
                            style_data_conditional=[
                                {'if': {'filter_query': '{STATUS} eq "START"'},
                                    'color': 'rgb(125, 50, 50)',
                                    'border': '2px solid white',
                                 },
                                {'if': {'filter_query': '{PROCESSNAME} eq "DATA AGGREGATION"'},
                                    'borderTop': '1px solid ' + darkcolor,
                                    'borderBottom': '1px solid ' + darkcolor,
                                    'backgroundColor': mediumlightcolor,
                                 },
                                {'if': {'filter_query': '{STATUS} eq "FAILURE"'},
                                    'backgroundColor': 'rgb(130, 75, 75)',
                                    'color': 'white',
                                    'fontWeight': 'bold',
                                 },
                                {'if': {'filter_query': '{PROCESSNAME} contains "DELETE"'},
                                    'color': '#505050',
                                 }
                            ],
                            css=[{
                                'selector': '.dash-cell div.dash-cell-value',
                                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                }],
                            export_format='xlsx',
                            export_headers='display',
                            merge_duplicate_headers=True
                        ),

                        html.Div(id='oracle-job-container'),

                        html.H6('DBA Scheduler Job Run Details', style={'textAlign': 'center'}),

                        dash_table.DataTable(
                            id='dba-scheduler-job-run-details-table',
                            columns=[{"name": i, "id": i} for i in dba_scheduler_job_run_details.columns],
                            data=dba_scheduler_job_run_details.to_dict("rows"),
                            style_table={'overflowX': 'scroll'},
                            style_as_list_view=True,
                            style_cell={
                                # all three widths are needed
                                'minWidth': '120px',
                                'whiteSpace': 'no-wrap',
                                'overflow': 'hidden',
                                'textOverflow': 'ellipsis',
                                },
                            style_data={
                                'backgroundColor': lightcolor,
                                'border': '0px solid white',
                                'fontColor': 'black',
                                'borderTop': '2px solid ' + darkcolor,
                                },
                            style_header={
                                'backgroundColor': lightcolor,
                                'fontWeight': 'bold',
                                'border': '0px solid white',
                                },
                            css=[{
                                'selector': '.dash-cell div.dash-cell-value',
                                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                                }],
                            export_format='xlsx',
                            export_headers='display',
                            merge_duplicate_headers=True
                        ),
                    ]),


# ┬─┐┬ ┬┌┐┌  ┌─┐┬  ┬┌─┐┬  ┬ ┬┌─┐┌┬┐┬┌─┐┌┐┌      ┌─┐┬ ┬┌┐ ┬  ┬┌─┐┬ ┬    ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌─┐┌─┐┬─┐┬ ┬┌┐┌
# ├┬┘│ ││││  ├┤ └┐┌┘├─┤│  │ │├─┤ │ ││ ││││──────├─┘│ │├┴┐│  │└─┐├─┤     ││├─┤ │ ├─┤├─┤│ ┬│ ┬├┬┘│ ││││
# ┴└─└─┘┘└┘  └─┘ └┘ ┴ ┴┴─┘└─┘┴ ┴ ┴ ┴└─┘┘└┘      ┴  └─┘└─┘┴─┘┴└─┘┴ ┴─────┴┘┴ ┴ ┴ ┴ ┴┴ ┴└─┘└─┘┴└─└─┘┘└┘

@app.callback(Output('run-evaluation-publish', "children"),
              [Input('publish-button', 'n_clicks')],
              [State('analysis-run-dropdown', 'value')]
              )
def publish_dataaggrun(n_clicks, dataaggrunid):
    if n_clicks is not None:
        if n_clicks > 0:
            procedure_out = cur2.var(str)
            cur2.callproc('PUBLISH_DATAAGGRUN', [dataaggrunid, procedure_out])
            dataaggrunid = dataaggrunid

            # output UFE adjusted load to PJM

            # retrieve dataaggrun info
            dataaggrun_query = f''' select * from dataaggrun where dataaggrunid = {dataaggrunid}'''
            dataaggrun = pd.read_sql(dataaggrun_query, con=db)
            operatingdate = dataaggrun['OPERATINGDATE'][0]
            # operatingdate_end = operatingdate + timedelta(days=1, seconds=-1)
            # operatingdate_local = local_tz.localize(operatingdate)
            # operatingdate_local_end = operatingdate_local + timedelta(days=1)

            # Retrieve dataagginterval records for selected dataaggrunids`
            pjm_output_query = '''
            select /*+ parallel */ * from dataagginterval where dataaggrunid = {dataaggrunid} and dataaggreport = 'RETAILER UFEZONE UFEADJ' '''.format(dataaggrunid=dataaggrunid)
            pjm_output = pd.read_sql(pjm_output_query, con=db)

            interval_cols = []
            # for i in range(0, hours_in_day):
            #     interval_start = (operatingdate_local + timedelta(hours = i))
            #     interval_start_dst = interval_start.astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")
            #     interval_cols.append(interval_start_dst)
            #     col_name = "T{0:0>3}".format(i)
            for i in range(0, hours_in_day):
                if dst_dates.shape[0] == 1:
                    if operatingdate.month > 6:
                        # fall DST transition date
                        if i < 2:
                            interval_label = str(i).zfill(2) + ':00'
                        elif i == 2:
                            # interval_start in transitiondates:
                            interval_label = str(i-1).zfill(2) + ':00*'
                        elif i > 2:
                            interval_label = str(i-1).zfill(2) + ':00'
                    else:
                        # spring DST transition date
                        if i < 2:
                            interval_label = str(i).zfill(2) + ':00'
                        else:
                            interval_label = str(i+1).zfill(2) + ':00'
                else:
                    # normal non DST transition day
                    interval_label = str(i).zfill(2) + ':00'

                interval_cols.append(interval_label)
                col_name = f"T{i:0>3}"

                pjm_output.rename(columns={col_name: interval_label}, inplace=True)

            pjm_output['DATAAGGRUNID_STR'] = pjm_output['DATAAGGRUNID'].astype(str)

            if hours_in_day < 25:
                pjm_output.drop(['T024'], axis=1, inplace=True)

            if hours_in_day < 24:
                pjm_output.drop(['T023'], axis=1, inplace=True)

            pjm_output['AGGCHARS'] = pjm_output[['MARKET', 'RETAILER', 'DISCO', 'PROFILECLASS', 'LOSSCLASS', 'UFEZONE', 'LMPBUS', 'DEMANDRESPONSEZONE', 'RATEFACTOR', 'CUSTOMERCLASS', 'DYNAMICPRICING', 'DIRECTLOADCONTROL', 'METERTYPE', 'WEATHERSENSITIVITY', 'WEATHERZONE', 'METHOD', 'DATATYPE', 'CALCULATIONTYPE']].apply(lambda x: x.str.cat(sep=' | '), axis=1)
            pjm_output.set_index(['AGGCHARS'], inplace=True)
            pjm_output['HEADER'] = '* ENGREC *'
            pjm_output[['HEADER', 'AGGCHARS', 'OPERATINGDATE', ]]

            # output file for InSchedule
            pjm_directory = 'C:\\Users\\david\\Documents\\Entegrity\\Skunkworx\\pjm_uploads\\'
            pjm_filename = str(operatingdate.strftime("%Y-%m-%d")) + '_' + dataaggrun['SETTLEMENTTYPE'][0] + '_pjm_upload__' + str(datetime.now().strftime("%Y-%m-%dT%H.%M.%S")) + '.txt'
#            file = open(pjm_directory + pjm_filename, 'w')
#            file.write('\t'.join(pjm_output[1:]) + '\n')
#            file.close()

#            pjm_filename = 'pjm_' + str(operatingdate.strftime("%Y-%m-%d")) + '_' + dataaggrun['SETTLEMENTTYPE'] + '_dataaggrunid_' + str(dataaggrunid) = '.txt'
            with open(pjm_directory + pjm_filename, "w") as f:
                f.write('\t'.join(pjm_output[1:]) + '\n')

            return dcc.Markdown('''
            ###### Data Agg Run ID **{dataaggrunid}** has been published, output to file ({pjm_filename}) and uploaded to PJM (*need client login to upload*).
            (Refresh the tab to clear this message)
            '''.format(dataaggrunid=dataaggrunid, pjm_filename=pjm_filename))


# ┬─┐┬ ┬┌┐┌  ┌─┐┬  ┬┌─┐┬  ┬ ┬┌─┐┌┬┐┬┌─┐┌┐┌      ┬ ┬┌┐┌┌─┐┬ ┬┌┐ ┬  ┬┌─┐┬ ┬    ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌─┐┌─┐┬─┐┬ ┬┌┐┌
# ├┬┘│ ││││  ├┤ └┐┌┘├─┤│  │ │├─┤ │ ││ ││││──────│ ││││├─┘│ │├┴┐│  │└─┐├─┤     ││├─┤ │ ├─┤├─┤│ ┬│ ┬├┬┘│ ││││
# ┴└─└─┘┘└┘  └─┘ └┘ ┴ ┴┴─┘└─┘┴ ┴ ┴ ┴└─┘┘└┘      └─┘┘└┘┴  └─┘└─┘┴─┘┴└─┘┴ ┴─────┴┘┴ ┴ ┴ ┴ ┴┴ ┴└─┘└─┘┴└─└─┘┘└┘

@app.callback(Output('run-evaluation-unpublish', "children"),
              [Input('unpublish-button', 'n_clicks')],
              [State('analysis-run-dropdown', 'value')]
              )
def unpublish_dataaggrun(n_clicks, dataaggrunid):
    if n_clicks is not None:
        if n_clicks > 0:
            procedure_out = cur2.var(str)
            cur2.callproc('UNPUBLISH_DATAAGGRUN', [dataaggrunid, procedure_out])
            dataaggrunid = dataaggrunid
            return dcc.Markdown('''
###### The **unpublish_dataaggrun** procedure was run on Data Agg Run ID **{dataaggrunid}**.
(Refresh the tab to clear this message)
'''.format(dataaggrunid=dataaggrunid))


# ┬─┐┬ ┬┌┐┌  ┌─┐┬  ┬┌─┐┬  ┬ ┬┌─┐┌┬┐┬┌─┐┌┐┌     ┌┬┐┌─┐┬  ┌─┐┌┬┐┌─┐    ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌─┐┌─┐┬─┐┬ ┬┌┐┌
# ├┬┘│ ││││  ├┤ └┐┌┘├─┤│  │ │├─┤ │ ││ ││││──────││├┤ │  ├┤  │ ├┤      ││├─┤ │ ├─┤├─┤│ ┬│ ┬├┬┘│ ││││
# ┴└─└─┘┘└┘  └─┘ └┘ ┴ ┴┴─┘└─┘┴ ┴ ┴ ┴└─┘┘└┘     ─┴┘└─┘┴─┘└─┘ ┴ └─┘─────┴┘┴ ┴ ┴ ┴ ┴┴ ┴└─┘└─┘┴└─└─┘┘└┘

@app.callback(Output('run-evaluation-delete', "children"),
              [Input('delete-button', 'n_clicks')],
              [State('analysis-run-dropdown', 'value'),
               ]
              )
def delete_dataaggrun(n_clicks,dataaggrunid):
    if n_clicks is not None:
        if n_clicks > 0:
            procedure_out = cur2.var(str)
            cur2.callproc('DELETE_DATAAGGRUN', [dataaggrunid, procedure_out])
            dataaggrunid = dataaggrunid
            return dcc.Markdown('''
###### The **delete_dataaggrun** procedure was run on Data Agg Run ID **{dataaggrunid}**.
(Refresh the tab to clear this message)
'''.format(dataaggrunid=dataaggrunid))


# ┌─┐┌─┐┌─┐┬─┐┌─┐┌─┐┌─┐┌┬┐┌─┐┌┬┐  ┌┬┐┌─┐┌┬┐┌─┐  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┬┌┐┌┌─┐      ┌─┐┌─┐┌┬┐   ┌┐ ┬ ┬┌┬┐┌┬┐┌─┐┌┐┌    ┌─┐┌┐┌┌─┐┌┐ ┬  ┌─┐┌┬┐    ┌─┐┌┬┐┌─┐┌┬┐┌─┐
# ├─┤│ ┬│ ┬├┬┘├┤ │ ┬├─┤ │ ├┤  ││   ││├─┤ │ ├─┤  ├┬┘├┤ ├─┘│ │├┬┘ │ │││││ ┬──────└─┐├┤  │    ├┴┐│ │ │  │ │ ││││    ├┤ │││├─┤├┴┐│  ├┤  ││    └─┐ │ ├─┤ │ ├┤
# ┴ ┴└─┘└─┘┴└─└─┘└─┘┴ ┴ ┴ └─┘─┴┘  ─┴┘┴ ┴ ┴ ┴ ┴  ┴└─└─┘┴  └─┘┴└─ ┴ ┴┘└┘└─┘      └─┘└─┘ ┴────└─┘└─┘ ┴  ┴ └─┘┘└┘────└─┘┘└┘┴ ┴└─┘┴─┘└─┘─┴┘────└─┘ ┴ ┴ ┴ ┴ └─┘

@app.callback(Output('retrieve-data-button', 'disabled'),
              [Input('dataaggrunid-dropdown', 'value')])
def set_button_enabled_state(dataaggrunid_list):
    """Disable button until dataaggrunid is selected."""
    if dataaggrunid_list == [None]:
        return True


# ┌─┐┌─┐┌─┐┬─┐┌─┐┌─┐┌─┐┌┬┐┌─┐┌┬┐  ┌┬┐┌─┐┌┬┐┌─┐  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┬┌┐┌┌─┐      ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐   ┌─┐┌─┐┌─┐┬─┐┌─┐┌─┐┌─┐┌┬┐┌─┐┌┬┐    ┌┬┐┌─┐┌┬┐┌─┐
# ├─┤│ ┬│ ┬├┬┘├┤ │ ┬├─┤ │ ├┤  ││   ││├─┤ │ ├─┤  ├┬┘├┤ ├─┘│ │├┬┘ │ │││││ ┬──────├┬┘├┤ ├─┘│ │├┬┘ │    ├─┤│ ┬│ ┬├┬┘├┤ │ ┬├─┤ │ ├┤  ││     ││├─┤ │ ├─┤
# ┴ ┴└─┘└─┘┴└─└─┘└─┘┴ ┴ ┴ └─┘─┴┘  ─┴┘┴ ┴ ┴ ┴ ┴  ┴└─└─┘┴  └─┘┴└─ ┴ ┴┘└┘└─┘      ┴└─└─┘┴  └─┘┴└─ ┴────┴ ┴└─┘└─┘┴└─└─┘└─┘┴ ┴ ┴ └─┘─┴┘─────┴┘┴ ┴ ┴ ┴ ┴

@app.callback(
    [Output('aggregated-data-container', 'children'),
     Output('lls_store', 'data')
     ],
    [Input('retrieve-data-button', 'n_clicks')],
    [State('dataaggrunid-dropdown', 'value'),
     State('dataaggreport-dropdown', 'value'),
     State('filter-output', 'value'),
     State('overlap-days-radio', 'value'),
     ],
    prevent_initial_call=True,
    )
def report_aggregated_data(n_clicks, dataaggrunid_list, dataaggreport_list, filter_output, overlap_days):
    """Display report for selected dataaggrunid."""
    if n_clicks is not None and n_clicks > 0:
        # dropdown filters
        if dataaggrunid_list == [None]:
            dataaggrunid_search = ' dataaggrunid is not null'
        else:
            dataaggrunid_search = " dataaggrunid in " + str(dataaggrunid_list).replace('[', '(').replace(']', ')')

        if dataaggreport_list == [None]:
            dataaggreport_search = ' '
        else:
            dataaggreport_search = " and dataaggreport in " + str(dataaggreport_list).replace('[', '(').replace(']', ')')

        if filter_output is None or filter_output == '':
            filter_output = ' '
        else:
            filter_output = ' and ' + filter_output

    # Retrieve dataagginterval records for selected dataaggrunids`
    if dataaggrunid_list == [None]:
        return ['No data selected', [None]]
    else:
        dataagginterval_query = '''
        select /*+ parallel */ * from dataagginterval where {dataaggrunid_search} {dataaggreport_search} {filter_output} order by dataaggreport, dataaggrunid, inserttimestamp'''.format(dataaggrunid_search=dataaggrunid_search, dataaggreport_search=dataaggreport_search, filter_output=filter_output)
        dff = pd.read_sql(dataagginterval_query, con=db)
        print(dataagginterval_query)

        if dff.shape[0] == 0:
            return ['No data selected', [None]]
        else:
            # define columns
            dff['DATAAGGRUNID_STR'] = dff['DATAAGGRUNID'].astype(str)
            if overlap_days == 'N':
                dff['AGGCHARS'] = dff[['MARKET', 'RETAILER', 'DISCO', 'PROFILECLASS', 'LOSSCLASS', 'UFEZONE', 'LMPBUS', 'DEMANDRESPONSEZONE', 'RATEFACTOR', 'CUSTOMERCLASS', 'DYNAMICPRICING', 'DIRECTLOADCONTROL', 'METERTYPE', 'WEATHERSENSITIVITY', 'WEATHERZONE', 'METHOD', 'DATATYPE', 'CALCULATIONTYPE']].apply(lambda x: x.str.cat(sep=' | '), axis=1)
            else:
                dff['AGGCHARS'] = dff[['DATAAGGRUNID_STR', 'MARKET', 'RETAILER', 'DISCO', 'PROFILECLASS', 'LOSSCLASS', 'UFEZONE', 'LMPBUS', 'DEMANDRESPONSEZONE', 'RATEFACTOR', 'CUSTOMERCLASS', 'DYNAMICPRICING', 'DIRECTLOADCONTROL', 'METERTYPE', 'WEATHERSENSITIVITY', 'WEATHERZONE', 'METHOD', 'DATATYPE', 'CALCULATIONTYPE']].apply(lambda x: x.str.cat(sep=' | '), axis=1)
            dff.set_index(['AGGCHARS'], inplace=True)

            # gather DST transition dates
            local_tz = pytz.timezone('US/Eastern')
            dst_dates = pd.DataFrame([local_tz._utc_transition_times]).transpose()
            dst_dates.rename({0: "TRANSITION_DATE"}, axis='columns', inplace=True)
            dst_dates.drop(dst_dates.index[0], inplace=True)
            dst_dates['TRUNCATED'] = pd.to_datetime(dst_dates['TRANSITION_DATE'], errors='coerce').dt.normalize()

            if overlap_days == 'N':
                for counter, opday in enumerate(dff['OPERATINGDATE'].unique(), 1):
                    # sunset to first operating date
                    daychunk = dff.loc[(dff['OPERATINGDATE'] == opday)]

                    # get operating date
                    operatingdate = daychunk['OPERATINGDATE'][0]
                    operatingdate_local = local_tz.localize(operatingdate)

                    # determine if DST transition date
                    transition_date = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

                    if transition_date.shape[0] == 1:
                        if transition_date.iloc[0][1].month <= 5:
                            hours_in_day = 23
                        else:
                            hours_in_day = 25
                    else:
                        hours_in_day = 24

                    # rename interval columns with datetime
                    interval_cols = []
                    for i in range(0, hours_in_day):
                        interval_start = (operatingdate_local + timedelta(hours=i))

                        interval_cols.append(interval_start)
                        col_name = f"T{i:0>3}"

                        daychunk.rename(columns={col_name: interval_start}, inplace=True)

                    if hours_in_day < 25:
                        daychunk.drop(['T024'], axis=1, inplace=True)

                    if hours_in_day < 24:
                        daychunk.drop(['T023'], axis=1, inplace=True)

                    if counter == 1:
                        chart_dff = daychunk[interval_cols].transpose()
                    else:
                        chart_dff = chart_dff.append(daychunk[interval_cols].transpose())
                chart_dff.sort_index(inplace=True)
            else:
                # get operating date
                operatingdate = dff['OPERATINGDATE'][0]
                operatingdate_local = local_tz.localize(operatingdate)

                # determine if DST transition date
                transition_date = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

                if transition_date.shape[0] == 1:
                    if transition_date.iloc[0][1].month <= 5:
                        hours_in_day = 23
                    else:
                        hours_in_day = 25
                else:
                    hours_in_day = 24

                interval_cols = []
                for i in range(0, hours_in_day):
                    # interval_start = (operatingdate_local + timedelta(hours=i))
                    if dst_dates.shape[0] == 1:
                        if operatingdate.month > 6:
                            # fall DST transition date
                            if i < 2:
                                interval_label = str(i).zfill(2) + ':00'
                            elif i == 2:  # interval_start in transitiondates:
                                interval_label = str(i-1).zfill(2) + ':00*'
                            elif i > 2:
                                interval_label = str(i-1).zfill(2) + ':00'
                        else:
                            # spring DST transition date
                            if i < 2:
                                interval_label = str(i).zfill(2) + ':00'
                            else:
                                interval_label = str(i+1).zfill(2) + ':00'
                    else:
                        # normal non DST transition day
                        interval_label = str(i).zfill(2) + ':00'

                    interval_cols.append(interval_label)
                    col_name = f"T{i:0>3}"

                    dff.rename(columns={col_name: interval_label}, inplace=True)

                if hours_in_day < 25:
                    dff.drop(['T024'], axis=1, inplace=True)

                if hours_in_day < 24:
                    dff.drop(['T023'], axis=1, inplace=True)

                chart_dff = dff[interval_cols].transpose()

            return [html.Div(
                [dcc.Graph(
                    id='filtered_graph',
                    figure={
                        'data': [go.Scattergl(
                            x=chart_dff.index,
                            y=chart_dff[i],
                            # text=chart_dff[i],
                            mode='lines',
                            opacity=1,
                            name=i,
                            connectgaps=False
                            ) for i in chart_dff.columns],
                        'layout': dict(
                            height=600,
                            xaxis={'title': 'Time',
                                   'nticks': hours_in_day,
                                   'showgrid': False,
                                   'tickangle': -45},
                            title=' | '.join(map(str, dataaggreport_list)) + ' (' + str(len(dff.index)) + ' curves)',
                            hovermode='closest',
                            hoverlabel=dict(namelength=-1),
                            paper_bgcolor=paperbackgroundcolor,
                            plot_bgcolor=plotbackgroundcolor,
                            legend_bgcolor=legendbackgroundcolor,
                            clickmode='event+select',
                            )
                        },
                    ),

                    dash_table.DataTable(
                        id='datatable-interactivity',
                        columns=[{"name": i, "id": i, "deletable": True} for i in dff.columns],
                        data=dff.to_dict("rows"),
                        editable=False,
                        filter_action='native',
                        sort_action='native',
                        sort_mode="multi",
                        row_selectable="multi",
                        row_deletable=True,
                        selected_rows=[],
                        style_table={'overflowX': 'scroll', 'overflowY': 'scroll', 'max_height': 300, 'width': '100%'},
                        # style_as_list_view=True,
                        style_cell={
                            # all three widths are needed
                            'minWidth': '120px',
                            'whiteSpace': 'no-wrap',
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                            },
                        style_data={
                            'backgroundColor': lightcolor,
                            'border': '0px solid white',
                            'fontColor': 'black',
                            'borderTop': '2px solid ' + darkcolor,
                            },
                        style_header={
                            'backgroundColor': lightcolor,
                            'fontWeight': 'bold',
                            'border': '0px solid white',
                            },
                        style_data_conditional=[
                            {'if': {'column_id': 'TOTAL',
                                    'filter_query': 'TOTAL < 0'},
                             'backgroundColor': 'rgb(210, 230, 256)',
                             # 'color': 'white',
                             },
                            {'if': {'column_id': 'PLC',
                                    'filter_query': 'PLC < 0'},
                             'backgroundColor': 'rgb(210, 230, 256)',
                             # 'color': 'white',
                             },
                            {'if': {'column_id': 'TPLC',
                                    'filter_query': 'TPLC < 0'},
                             'backgroundColor': 'rgb(210, 230, 256)',
                             # 'color': 'white',
                             },
                        ],
                        css=[{
                            'selector': '.dash-cell div.dash-cell-value',
                            'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                            }],
                        export_format='xlsx',
                        export_headers='display',
                        merge_duplicate_headers=True
                        ),

                    html.Div(id='lls-click-container'),

                    html.Div([
                        html.Label('Service Point Click Data Sort Order'),
                        dcc.RadioItems(
                            id='clickdata-sort-order-radio',
                            options=[{'label': 'Desc', 'value': 'Desc'},
                                     {'label': 'Asc', 'value': 'Asc'},
                                     ],
                            value='Desc',
                            labelStyle={'display': 'inline-block'}
                        ),
                        ], style={
                            'width': '15%',
                            'display': 'inline-block',
                            'margin': '10px 10px',
                            'verticalAlign': 'middle'}),
                 ]),

                # store lls records in browser memory
                dff.to_dict('records')
            ]


# ┌─┐┌─┐┌─┐┬─┐┌─┐┌─┐┌─┐┌┬┐┌─┐┌┬┐  ┌┬┐┌─┐┌┬┐┌─┐  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐┬┌┐┌┌─┐     ┌┬┐┬┌─┐┌─┐┬  ┌─┐┬ ┬   ┬  ┬  ┌─┐    ┌─┐┬  ┬┌─┐┬┌─    ┌┬┐┌─┐┌┬┐┌─┐
# ├─┤│ ┬│ ┬├┬┘├┤ │ ┬├─┤ │ ├┤  ││   ││├─┤ │ ├─┤  ├┬┘├┤ ├─┘│ │├┬┘ │ │││││ ┬──────│││└─┐├─┘│  ├─┤└┬┘   │  │  └─┐    │  │  ││  ├┴┐     ││├─┤ │ ├─┤
# ┴ ┴└─┘└─┘┴└─└─┘└─┘┴ ┴ ┴ └─┘─┴┘  ─┴┘┴ ┴ ┴ ┴ ┴  ┴└─└─┘┴  └─┘┴└─ ┴ ┴┘└┘└─┘     ─┴┘┴└─┘┴  ┴─┘┴ ┴ ┴────┴─┘┴─┘└─┘────└─┘┴─┘┴└─┘┴ ┴─────┴┘┴ ┴ ┴ ┴ ┴

@app.callback(
    [Output('report-click-container', 'children')],
    [Input('filtered_graph', 'clickData'),
     Input('lls_store', 'modified_timestamp'),
     Input('clickdata-sort-order-radio', 'value'),
     ],
    [State('lls_store', 'data'),
     ]
    )
def display_lls_click_data(clickData, lls_timestamp, sp_sort_order, lls_store):
    if clickData is None:
        raise dash.exceptions.PreventUpdate

    # retrieve data from
    dff = pd.DataFrame(lls_store)
    # get selected curve number

    curve_number = clickData['points'][0]['curveNumber']
    curve_info = dff.iloc[curve_number]
    top_n = 100
    sort_order = sp_sort_order

    dataaggrunid_clause = 'DATAAGGRUNID = ' + curve_info['DATAAGGRUNID'].astype(str)
    operatingdate_query = "select to_char(operatingdate, 'yyyy-mm-dd') as operatingdate from dataaggrun where " + dataaggrunid_clause
    operatingdate_df = pd.read_sql(operatingdate_query, con=db)
    operatingdate = operatingdate_df['OPERATINGDATE'][0]
    # dataaggreport_clause = " and DATAAGGREPORT = '" + curve_info['DATAAGGREPORT'] + "'"
    # datatype_clause = " and DATATYPE = '" + curve_info['DATATYPE'] + "'"
    datatype_clause = " and DATATYPE = 'UNADJ'"
    operatingdate_clause = " and OPERATINGDATE = to_date('" + operatingdate + "', 'yyyy-mm-dd')"

    where_clause = " where " + dataaggrunid_clause + operatingdate_clause
    name_clause = None
    lls_click_list = []
    for CHARACTERISTIC in ['RETAILER', 'DISCO', 'PROFILECLASS', 'LOSSCLASS', 'UFEZONE', 'LMPBUS', 'DEMANDRESPONSEZONE', 'RATEFACTOR', 'CUSTOMERCLASS', 'DYNAMICPRICING', 'DIRECTLOADCONTROL', 'METERTYPE', 'WEATHERSENSITIVITY', 'WEATHERZONE', 'METHOD']:
        if curve_info[CHARACTERISTIC] is not None:
            where_clause = where_clause + " and " + CHARACTERISTIC + " = '" + curve_info[CHARACTERISTIC] + "'"
            lls_click_list.append(curve_info[CHARACTERISTIC])

            if name_clause is None:
                name_clause = curve_info[CHARACTERISTIC]
            else:
                name_clause = name_clause + " " + curve_info[CHARACTERISTIC]

    llsinterval_query = "select * from LLSINTERVAL " + where_clause + datatype_clause + " order by total desc"
    print()
    print(llsinterval_query)
    llsinterval = pd.read_sql(llsinterval_query, con=db)

    servicepoint_query = """
    select /*+ parallel */
           rank() over (order by total {sort_order} ) KWHRANK,
           DATAAGGRUNID,            DATATYPE,               OPERATINGDATE,           SERVICEPOINT,
           MARKET,                  RETAILER,               DISCO,                   PROFILECLASS,
           LOSSCLASS,               UFEZONE,                LMPBUS,                  DEMANDRESPONSEZONE,
           RATEFACTOR,              CUSTOMERCLASS,          DYNAMICPRICING,          DIRECTLOADCONTROL,
           METERTYPE,               WEATHERSENSITIVITY,     WEATHERZONE,             METHOD,
           INTERVALLENGTH,          METER,                  CHANNEL,                 UOM,
           PROXYDAY,                PROXYDAYRANK,           CHANNELAGGMETHOD,        CHANNELRANK,
           CHANNELCOUNT,            SCALARSTARTTIME,        SCALARSTOPTIME,          USAGE,
           PROFILEUSAGE,            USAGEFACTOR,            PLC,                     TPLC,
           TOTAL,      T000,       T001,       T002,       T003,       T004,       T005,
           T006,       T007,       T008,       T009,       T010,       T011,       T012,       T013,
           T014,       T015,       T016,       T017,       T018,       T019,       T020,       T021,
           T022,       T023,       T024
      from servicepointdataaggrun
      {where_clause}
      {datatype_clause}
     order by dataaggrunid, total {sort_order}
     fetch first {top_n} rows only""".format(where_clause=where_clause, datatype_clause=datatype_clause, sort_order=sort_order, top_n=top_n)
    servicepoint = pd.read_sql(servicepoint_query, con=db)
    servicepoint = pd.DataFrame(servicepoint)

    servicepoint_count_query = f"""select count(*) as count from servicepointdataaggrun {where_clause} {datatype_clause}"""
    servicepoint_counter = pd.read_sql(servicepoint_count_query, con=db)
    servicepoint_count = servicepoint_counter['COUNT'][0]

    # get operating date
    local_tz = pytz.timezone('US/Eastern')
    operatingdate = llsinterval['OPERATINGDATE'][0]

    dst_dates = pd.DataFrame([local_tz._utc_transition_times]).transpose()
    dst_dates.rename({0: "TRANSITION_DATE"}, axis='columns', inplace=True)
    dst_dates.drop(dst_dates.index[0], inplace=True)
    dst_dates['TRUNCATED'] = pd.to_datetime(dst_dates['TRANSITION_DATE'], errors='coerce').dt.normalize()

    dst_dates = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

    if dst_dates.shape[0] == 1:
        if dst_dates.iloc[0][1].month <= 5:
            hours_in_day = 23
        else:
            hours_in_day = 25
    else:
        hours_in_day = 24

    interval_cols = []
    for i in range(0, hours_in_day):
        if dst_dates.shape[0] == 1:
            if operatingdate.month > 6:
                # fall DST transition date
                if i < 2:
                    interval_label = str(i).zfill(2) + ':00'
                elif i == 2:
                    # interval_start in transitiondates:
                    interval_label = str(i-1).zfill(2) + ':00*'
                elif i > 2:
                    interval_label = str(i-1).zfill(2) + ':00'
            else:
                # spring DST transition date
                if i < 2:
                    interval_label = str(i).zfill(2) + ':00'
                else:
                    interval_label = str(i+1).zfill(2) + ':00'
        else:
            # normal non DST transition day
            interval_label = str(i).zfill(2) + ':00'

        interval_cols.append(interval_label)
        col_name = f"T{i:0>3}"

        llsinterval.rename(columns={col_name: interval_label}, inplace=True)
        servicepoint.rename(columns={col_name: interval_label}, inplace=True)

    llsinterval['DATAAGGRUNID_STR'] = llsinterval['DATAAGGRUNID'].astype(str)
    servicepoint['DATAAGGRUNID_STR'] = servicepoint['DATAAGGRUNID'].astype(str)

    if hours_in_day < 25:
        llsinterval.drop(['T024'], axis=1, inplace=True)
        servicepoint.drop(['T024'], axis=1, inplace=True)

    if hours_in_day < 24:
        llsinterval.drop(['T023'], axis=1, inplace=True)
        servicepoint.drop(['T023'], axis=1, inplace=True)

    llsinterval['AGGCHARS'] = llsinterval[['RETAILER', 'DISCO', 'PROFILECLASS', 'LOSSCLASS', 'UFEZONE', 'LMPBUS', 'DEMANDRESPONSEZONE', 'RATEFACTOR', 'CUSTOMERCLASS', 'DYNAMICPRICING', 'DIRECTLOADCONTROL', 'METERTYPE', 'WEATHERSENSITIVITY', 'WEATHERZONE', 'METHOD', 'DATATYPE', ]].apply(lambda x: x.str.cat(sep=' | '), axis=1)
    llsinterval.set_index(['AGGCHARS'], inplace=True)

    servicepoint['AGGCHARS'] = servicepoint['SERVICEPOINT'] + ' | Rank ' + servicepoint['KWHRANK'].astype(str)
    servicepoint.set_index(['AGGCHARS'], inplace=True)

    chart_lls = llsinterval[interval_cols].transpose()
    chart_servicepoint = servicepoint[interval_cols].transpose()

    return [[dcc.Loading(id="loading-lls_graph", children=[
            html.Div([
                dcc.Graph(
                        id='lls_graph',
                        figure={
                            'data': [go.Scattergl(
                                    x=chart_lls.index,
                                    y=chart_lls[i],
                                    mode='lines',
                                    opacity=1,
                                    name=i
                                    ) for i in chart_lls.columns],
                            'layout': dict(
                                    height=600,
                                    xaxis={'title': 'Time',
                                           'nticks': hours_in_day,
                                           'tickangle': -45},
                                    title=name_clause + ' (' + str(len(llsinterval.index)) + ' LLS Interval Series)',
                                    hovermode='closest',
                                    hoverlabel=dict(namelength=-1),
                                    paper_bgcolor=paperbackgroundcolor,
                                    plot_bgcolor=plotbackgroundcolor,
                                    legend_bgcolor=legendbackgroundcolor,
                                    clickmode='event+select',
                                    showlegend=False,
                                    )
                            },
                    )], style={'display': 'inline-block', 'width': '100%', 'height': 600})
            ]),

            html.Div([
                dcc.Graph(
                        id='servicepoint_graph',
                        figure={
                            'data': [go.Scattergl(
                                    x=chart_servicepoint.index,
                                    y=chart_servicepoint[i],
                                    mode='lines',
                                    opacity=1,
                                    name=i
                                    ) for i in chart_servicepoint.columns],
                            'layout': dict(
                                    height=600,
                                    xaxis={'title': 'Time',
                                           'nticks': hours_in_day,
                                           'tickangle': -45},
                                    title='Top ' + str(min(100, servicepoint_count)) + ' of ' + str(max(100, servicepoint_count)) + ' Service Points for ' + name_clause,
                                    hovermode='closest',
                                    hoverlabel=dict(namelength=-1),
                                    paper_bgcolor=paperbackgroundcolor,
                                    plot_bgcolor=plotbackgroundcolor,
                                    legend_bgcolor=legendbackgroundcolor,
                                    clickmode='event+select',
                                    showlegend=True,
                                    )
                            },
                    )], style={'display': 'inline-block', 'width': '100%', 'height': 600}),

            html.Div([
                dash_table.DataTable(
                    id='top-servicepoints',
                    columns=[{"name": i, "id": i} for i in servicepoint.columns],
                    data=servicepoint.to_dict("rows"),
                    editable=True,
                    filter_action='native',
                    sort_action='native',
                    sort_mode="multi",
                    row_selectable="multi",
                    row_deletable=True,
                    selected_rows=[],
                    style_table={'overflowX': 'scroll', 'overflowY': 'scroll', 'max_height': 600, 'width': '100%'},
                    # style_as_list_view=True,
                    style_cell={
                        # all three widths are needed
                        'minWidth': '120px',
                        'whiteSpace': 'no-wrap',
                        'overflow': 'hidden',
                        'textOverflow': 'ellipsis',
                    },
                    style_data={'backgroundColor': lightcolor,
                                'border': '0px solid white',
                                'fontColor': 'black',
                                'borderTop': '2px solid ' + darkcolor,
                                },
                    style_header={
                        'backgroundColor': lightcolor,
                        'fontWeight': 'bold',
                        'border': '0px solid white',
                        },
                    style_data_conditional=[
                        {'if': {'column_id': 'SERVICEPOINT'},
                            'backgroundColor': 'rgba(26, 102, 127, .3)',
                            # 'color': 'white',
                         },
                        {'if': {'column_id': 'TOTAL',
                                'filter_query': '{TOTAL} < 0'},
                            'backgroundColor': 'rgb(210, 150, 150)',
                            # 'color': 'white',
                         },
                        {'if': {'column_id': 'PLC',
                                'filter_query': '{PLC} < 0'},
                            'backgroundColor': 'rgb(210, 150, 150)',
                            # 'color': 'white',
                         },
                        {'if': {'column_id': 'TPLC',
                                'filter_query': '{TPLC} < 0'},
                            'backgroundColor': 'rgb(210, 150, 150)',
                            # 'color': 'white',
                         },
                    ],
                    css=[{
                        'selector': '.dash-cell div.dash-cell-value',
                        'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                        }],
                    export_format='xlsx',
                    export_headers='display',
                    merge_duplicate_headers=True
            )], style={'display': 'inline-block', 'width': '100%', 'height': 600}) ,
            ],
            ]


# ┌─┐┌─┐┬─┐┬  ┬┬┌─┐┌─┐  ┌─┐┌─┐┬┌┐┌┌┬┐  ┌┬┐┌─┐┌┬┐┌─┐      ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐    ┌─┐┌─┐┬─┐┬  ┬┬┌─┐┌─┐┌─┐┌─┐┬┌┐┌┌┬┐┌┬┐┌─┐┌┬┐┌─┐    ┌─┐┌─┐┌─┐┌─┐
# └─┐├┤ ├┬┘└┐┌┘││  ├┤   ├─┘│ │││││ │    ││├─┤ │ ├─┤──────│ │├─┘ ││├─┤ │ ├┤     └─┐├┤ ├┬┘└┐┌┘││  ├┤ ├─┘│ │││││ │  ││├─┤ │ ├─┤    ├─┘├─┤│ ┬├┤
# └─┘└─┘┴└─ └┘ ┴└─┘└─┘  ┴  └─┘┴┘└┘ ┴   ─┴┘┴ ┴ ┴ ┴ ┴      └─┘┴  ─┴┘┴ ┴ ┴ └─┘────└─┘└─┘┴└─ └┘ ┴└─┘└─┘┴  └─┘┴┘└┘ ┴ ─┴┘┴ ┴ ┴ ┴ ┴────┴  ┴ ┴└─┘└─┘

@app.callback(
    Output('intermediate-top-servicepoints', 'children'),
    [Input('refresh-data-button', 'n_clicks')
     ],
    [State('servicepoint-filter', 'value'),
     State('dataaggrunid-dropdown', 'value'),
     State('sort-order-radio', 'value'),
     State('top_n', 'value')
     ])
def update_servicepointdata_page (nclicks, servicepoint_filter, dataaggrunid_list, sort_order, top_n):
    # Retrieve biggest service points from dataaggruns
    # load data for dataaggrunid
    if dataaggrunid_list is None:
        dataaggrunid_search = ' dataaggrunid is not null'
        emptydf = pd.DataFrame(columns=['KWHRANK', 'DATAAGGRUNID', 'DATATYPE',  'OPERATINGDATE',  'SERVICEPOINT',
                                        'MARKET', 'RETAILER', 'DISCO', 'PROFILECLASS', 'LOSSCLASS', 'UFEZONE', 'LMPBUS', 'DEMANDRESPONSEZONE', 'RATEFACTOR',
                                        'CUSTOMERCLASS', 'DYNAMICPRICING', 'DIRECTLOADCONTROL', 'METERTYPE', 'WEATHERSENSITIVITY', 'WEATHERZONE', 'METHOD',
                                        'INTERVALLENGTH', 'METER', 'CHANNEL', 'UOM', 'PROXYDAY', 'PROXYDAYRANK', 'CHANNELAGGMETHOD', 'CHANNELRANK',
                                        'CHANNELCOUNT', 'SCALARSTARTTIME', 'SCALARSTOPTIME', 'USAGE', 'PROFILEUSAGE', 'USAGEFACTOR', 'PLC', 'TPLC', 'TOTAL',
                                        'T000', 'T001', 'T002', 'T003', 'T004', 'T005', 'T006', 'T007', 'T008', 'T009', 'T010', 'T011', 'T012', 'T013', 'T014',
                                        'T015', 'T016', 'T017', 'T018', 'T019', 'T020', 'T021', 'T022', 'T023', 'T024'])
        # return emptydf.to_json(date_format='iso', orient='split')
        return emptydf.to_json(date_format='iso', orientation='split')
    else:
        dataaggrunid_search = " dataaggrunid in " + str(dataaggrunid_list).replace('[','(').replace(']',')')

        if servicepoint_filter is None or servicepoint_filter == '':
            servicepoint_filter = ' '
        else:
            servicepoint_filter = ' and ' + servicepoint_filter

        top_servicepoints_query = """
        select /*+   */
               rank() over (order by total {sort_order}) KWHRANK,
               DATAAGGRUNID,           DATATYPE,               OPERATINGDATE,          SERVICEPOINT,
               MARKET,                 RETAILER,               DISCO,                  PROFILECLASS,
               LOSSCLASS,              UFEZONE,                LMPBUS,                 DEMANDRESPONSEZONE,
               RATEFACTOR,             CUSTOMERCLASS,          DYNAMICPRICING,         DIRECTLOADCONTROL,
               METERTYPE,              WEATHERSENSITIVITY,     WEATHERZONE,            METHOD,
               INTERVALLENGTH,         METER,                  CHANNEL,                UOM,
               PROXYDAY,               PROXYDAYRANK,           CHANNELAGGMETHOD,       CHANNELRANK,
               CHANNELCOUNT,           SCALARSTARTTIME,        SCALARSTOPTIME,         USAGE,
               PROFILEUSAGE,           USAGEFACTOR,            PLC,                    TPLC,
               TOTAL,      T000,       T001,       T002,       T003,       T004,       T005,
               T006,       T007,       T008,       T009,       T010,       T011,       T012,       T013,
               T014,       T015,       T016,       T017,       T018,       T019,       T020,       T021,
               T022,       T023,       T024
          from servicepointdataaggrun
         where {dataaggrunid_search}
               and DATATYPE = 'UNADJ'
               {servicepoint_filter}
         order by dataaggrunid, total {sort_order}
         fetch first {top_n} rows only""".format(dataaggrunid_search=dataaggrunid_search, top_n=top_n, sort_order=sort_order, servicepoint_filter=servicepoint_filter)
        top_servicepoints = pd.read_sql(top_servicepoints_query, con=db)

        if top_servicepoints.empty:
            print('top servicepoints dataframe is empty')
            top_servicepoints.append(['' for c in top_servicepoints.columns])
            return top_servicepoints.to_json(date_format='iso', orientation='split')
        else:
            operatingdate = top_servicepoints['OPERATINGDATE'][0]

            dst_dates = pd.DataFrame([local_tz._utc_transition_times]).transpose()
            dst_dates.rename({0: "TRANSITION_DATE"}, axis='columns', inplace=True)
            dst_dates.drop(dst_dates.index[0], inplace=True)
            dst_dates['TRUNCATED'] = pd.to_datetime(dst_dates['TRANSITION_DATE'], errors='coerce').dt.normalize()

            dst_dates = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

            if dst_dates.shape[0] == 1:
                if dst_dates.iloc[0][1].month <= 5:
                    hours_in_day = 23
                else:
                    hours_in_day = 25
            else:
                hours_in_day = 24

            for i in range(0, hours_in_day):
                if dst_dates.shape[0] == 1:
                    if operatingdate.month > 6:
                        # fall DST transition date
                        if i < 2:
                            interval_label = str(i).zfill(2) + ':00'
                        elif i == 2:
                            # interval_start in transitiondates:
                            interval_label = str(i-1).zfill(2) + ':00*'
                        elif i > 2:
                            interval_label = str(i-1).zfill(2) + ':00'
                    else:
                        # spring DST transition date
                        if i < 2:
                            interval_label = str(i).zfill(2) + ':00'
                        else:
                            interval_label = str(i+1).zfill(2) + ':00'
                else:
                    # normal non DST transition day
                    interval_label = str(i).zfill(2) + ':00'

                interval_cols.append(interval_label)
                col_name = f"T{i:0>3}"

                top_servicepoints.rename(columns={col_name: interval_label}, inplace=True)

            # top_servicepoints.rename(columns={"T000":"00:00", "T001":"01:00", "T002":"02:00", "T003":"03:00", "T004":"04:00", "T005":"05:00", "T006":"06:00", "T007":"07:00", "T008":"08:00", "T009":"09:00", "T010":"10:00", "T011":"11:00", "T012":"12:00", "T013":"13:00", "T014":"14:00", "T015":"15:00", "T016":"16:00", "T017":"17:00", "T018":"18:00", "T019":"19:00", "T020":"20:00", "T021":"21:00", "T022":"22:00", "T023":"23:00", }, inplace=True)
            if hours_in_day < 25:
                top_servicepoints.drop(['T024'], axis=1, inplace=True)

            if hours_in_day < 24:
                top_servicepoints.drop(['T023'], axis=1, inplace=True)

            if run_count == 1:
                top_servicepoints['AGGCHARS'] = top_servicepoints['SERVICEPOINT'] + ' | Rank ' + top_servicepoints['KWHRANK'].astype(str) + ' | ' + top_servicepoints['UFEZONE']
            else:
                top_servicepoints['AGGCHARS'] = top_servicepoints['SERVICEPOINT'] + ' | ' + top_servicepoints['DATAAGGRUNID'].astype(str) + ' | Rank ' + top_servicepoints['KWHRANK'].astype(str) + ' | ' + top_servicepoints['UFEZONE']

            top_servicepoints.set_index(['AGGCHARS'], inplace=True)
            print('top_servicepoints')
            print(top_servicepoints)
            
            return top_servicepoints.to_json(date_format='iso', orientation='split')


# ┌─┐┌─┐┬─┐┬  ┬┬┌─┐┌─┐  ┌─┐┌─┐┬┌┐┌┌┬┐  ┌┬┐┌─┐┌┬┐┌─┐      ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐    ┌─┐┬─┐┌─┐┌─┐┬ ┬    ┌─┐┬  ┬┌┬┐┌─┐┬─┐
# └─┐├┤ ├┬┘└┐┌┘││  ├┤   ├─┘│ │││││ │    ││├─┤ │ ├─┤──────│ │├─┘ ││├─┤ │ ├┤     │ ┬├┬┘├─┤├─┘├─┤    └─┐│  │ ││├┤ ├┬┘
# └─┘└─┘┴└─ └┘ ┴└─┘└─┘  ┴  └─┘┴┘└┘ ┴   ─┴┘┴ ┴ ┴ ┴ ┴      └─┘┴  ─┴┘┴ ┴ ┴ └─┘────└─┘┴└─┴ ┴┴  ┴ ┴────└─┘┴─┘┴─┴┘└─┘┴└─

@app.callback(
    Output('servicepoint-interactivity-container', 'children'),
    [Input('top_n', 'value'),
     Input('intermediate-top-servicepoints', 'children')],
    # [State('dataaggrunid-dropdown', 'value')]
    )
# def update_graph_slider (top_n, top_servicepoint_data, dataaggrunid):
def update_graph_slider (top_n, top_servicepoint_data):
    # json.loads(jsonified_cleaned_data)
    top_servicepoints = pd.read_json(top_servicepoint_data, orientation='split')
    print('update_graph_slider Top service points')
    print(top_servicepoints)

    if top_servicepoints.shape[0] == 0:
        return 'No data found'
    else:
        firstvalue = 1
        lastvalue = top_n

        spp = top_servicepoints[top_servicepoints['KWHRANK'] >= firstvalue]
        spp = spp[spp['KWHRANK'] <= lastvalue]
        spp['OPERATINGDATE'] = pd.to_datetime(spp['OPERATINGDATE']).dt.tz_convert(None)

        # get operating date
        local_tz = pytz.timezone('US/Eastern')
        operatingdate = spp['OPERATINGDATE'][0]

        dst_dates = pd.DataFrame([local_tz._utc_transition_times]).transpose()
        dst_dates.rename({0: "TRANSITION_DATE"}, axis='columns', inplace=True)
        dst_dates.drop(dst_dates.index[0], inplace=True)
        # dst_dates['TRUNCATED'] = pd.to_datetime(dst_dates['TRANSITION_DATE']#, errors='coerce').dt.normalize()
        dst_dates['TRUNCATED'] = dst_dates['TRANSITION_DATE'].dt.normalize()

        dst_dates = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

        if dst_dates.shape[0] == 1:
            if dst_dates.iloc[0][1].month <= 5:
                hours_in_day = 23
            else:
                hours_in_day = 25
        else:
            hours_in_day = 24

        interval_cols = []
        for i in range(0, hours_in_day):
            if dst_dates.shape[0] == 1:
                if operatingdate.month > 6:
                    # fall DST transition date
                    if i < 2:
                        interval_label = str(i).zfill(2) + ':00'
                    elif i == 2:
                        # interval_start in transitiondates:
                        interval_label = str(i-1).zfill(2) + ':00*'
                    elif i > 2:
                        interval_label = str(i-1).zfill(2) + ':00'
                else:
                    # spring DST transition date
                    if i < 2:
                        interval_label = str(i).zfill(2) + ':00'
                    else:
                        interval_label = str(i+1).zfill(2) + ':00'
            else:
                # normal non DST transition day
                interval_label = str(i).zfill(2) + ':00'

            interval_cols.append(interval_label)

        top_servicepoints_chart = spp[interval_cols].transpose()

        return html.Div([
            dcc.Loading(id="loading-top_servicepoints_graph", children=[
                dcc.Graph(
                    id='top_servicepoints_graph',
                    figure={
                        'data': [go.Scattergl(
                                x=top_servicepoints_chart.index,
                                y=top_servicepoints_chart[i],
                                text=top_servicepoints_chart.index,
                                mode='lines',
                                opacity=1,
                                name=i
                                ) for i in top_servicepoints_chart.columns],
                        'layout': dict(
                                height=600,
                                xaxis={
                                    'title': 'Time',
                                    'nticks': hours_in_day,
                                    'showgrid': False,
                                    'tickangle': -45},
                                hovermode='closest',
                                hoverlabel=dict(namelength=-1),
                                title='Large Service Points (' + str(firstvalue) + ' to ' + str(lastvalue) + ')',
                                paper_bgcolor=paperbackgroundcolor,
                                plot_bgcolor=plotbackgroundcolor,
                                legend_bgcolor=legendbackgroundcolor,
                                )
                        },
                ),
            ]),

            dcc.Loading(id="loading-servicepoint-interactivity", children=[
                dash_table.DataTable(
                    id='servicepoint-interactivity',
                    columns=[{"name": i, "id": i} for i in spp.columns],
                    data=spp.to_dict("rows"),
                    editable=True,
                    filter_action='native',
                    sort_action='native',
                    sort_mode="multi",
                    row_selectable="multi",
                    row_deletable=True,
                    selected_rows=[],
                    style_table={'overflowX': 'scroll', 'overflowY': 'scroll', 'max_height': 600, 'width': '100%'},
                    # style_as_list_view=True,
                    style_cell={
                        # all three widths are needed
                        'minWidth': '120px',
                        'whiteSpace': 'no-wrap',
                        'overflow': 'hidden',
                        'textOverflow': 'ellipsis',
                    },
                    style_data={'backgroundColor': lightcolor,
                                'border': '0px solid white',
                                'fontColor': 'black',
                                'borderTop': '2px solid ' + darkcolor,
                                },
                    style_header={
                        'backgroundColor': lightcolor,
                        'fontWeight': 'bold',
                        'border': '0px solid white',
                        },
                    style_data_conditional=[
                        {'if': {'column_id': 'SERVICEPOINT'},
                            'backgroundColor': 'rgba(26, 102, 127, .3)',
                         },
                        {'if': {'column_id': 'TOTAL',
                                'filter_query': '{TOTAL} < 0'},
                            'backgroundColor': 'rgb(210, 150, 150)',
                         },
                        {'if': {'column_id': 'PLC',
                                'filter_query': '{PLC} < 0'},
                            'backgroundColor': 'rgb(210, 150, 150)',
                         },
                        {'if': {'column_id': 'TPLC',
                                'filter_query': '{TPLC} < 0'},
                            'backgroundColor': 'rgb(210, 150, 150)',
                         },
                    ],
                    css=[{
                        'selector': '.dash-cell div.dash-cell-value',
                        'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                        }],
                    export_format='xlsx',
                    export_headers='display',
                    merge_duplicate_headers=True
                )
            ]),
        ])


# ┬─┐┬ ┬┌┐┌  ┬─┐┌─┐┌─┐┌─┐┌─┐┬─┐┌─┐┬ ┬
# ├┬┘│ ││││  ├┬┘├┤ └─┐├┤ ├─┤├┬┘│  ├─┤
# ┴└─└─┘┘└┘  ┴└─└─┘└─┘└─┘┴ ┴┴└─└─┘┴ ┴

@app.callback(Output('research-container', "children"),
              [Input('run-research-button', 'n_clicks')],
              [State('researchgroup-dropdown', 'value'),
               State('research-start-picker', 'date'),
               State('research-stop-picker', 'date'),
               ],
              prevent_initial_call=True,
              )
def run_research(n_clicks, researchgroup, research_start, research_stop):
    print('__________')
    print(datetime.now(), ' : execute analysis for research group ', researchgroup, ' from ', research_start, ' to ', research_stop)
    procedure_out = None
    procedure_out = cur2.var(str)
    print(researchgroup)
    if researchgroup == []:
        cur2.callproc('RESEARCH', ['ALL', str(research_start), str(research_stop), procedure_out])
        print(datetime.now(), ' : analysis output = ' , procedure_out.getvalue())
        print()
    else:
        for rg in researchgroup:
            cur2.callproc('RESEARCH', [rg, str(research_start), str(research_stop), procedure_out])
            print(datetime.now(), ' : analysis output = ' , procedure_out.getvalue())
            print()

    # errors = cur2.fetchall()
    # print('__________')
    # print(errors)
    # if errors:
    #     for info in errors:
    #         print("Error at line {} position {}:\n{}".format(*info))

    return dcc.Markdown('''###### Completed analysis for research group **{researchgroup}** from **{research_start}** to **{research_stop}**.'''.format(researchgroup=researchgroup, research_start=research_start, research_stop=research_stop))



# ┌─┐┬ ┬┌─┐┬ ┬  ┬─┐┌─┐┌─┐┌─┐┌─┐┬─┐┌─┐┬ ┬
# └─┐├─┤│ ││││  ├┬┘├┤ └─┐├┤ ├─┤├┬┘│  ├─┤
# └─┘┴ ┴└─┘└┴┘  ┴└─└─┘└─┘└─┘┴ ┴┴└─└─┘┴ ┴

@app.callback(Output('research-output-container', "children"),
              [Input('researchgroup-dropdown', 'value'),
               Input('uom-dropdown', 'value'),
               Input('research-start-picker', 'date'),
               Input('research-stop-picker', 'date'),
               Input('overlap-days-radio', 'value'),
               Input('range-compare-radio', 'value'),
               ],
              # prevent_initial_call=True,
              )
def show_research(researchgroup_list, uom_list, research_start, research_stop, overlap_days, range_compare):
    """Display research output for selected inputs"""

    # Retrieve researchinterval records for selected inputs
    if researchgroup_list == [None] or len(researchgroup_list) == 0:
        research_search = ''
    else:
        if 'ALL' in researchgroup_list:
            research_search = ''
        else:
            research_search = "researchgroup in " + str(researchgroup_list).replace('[', '(').replace(']', ')') + " and "

    if uom_list == [None] or len(uom_list) == 0:
        uom_search = ''
    else:
        uom_search = " UOM in " + str(uom_list).replace('[', '(').replace(']', ')') + " and "
    
    if range_compare == 'Compare':
        range_search = '''operatingdate in (to_date('{research_start}', 'yyyy-mm-dd') , to_date('{research_stop}', 'yyyy-mm-dd')) '''.format(research_start=research_start, research_stop=research_stop)
    else:
        range_search = '''operatingdate between to_date('{research_start}', 'yyyy-mm-dd') and to_date('{research_stop}', 'yyyy-mm-dd') '''.format(research_start=research_start, research_stop=research_stop)

        
    researchinterval_query = '''
    select /*+ parallel */ * from researchinterval 
     where {research_search} {uom_search} {range_search}
     order by researchgroup, operatingdate'''.format(research_search=research_search, uom_search=uom_search, range_search=range_search)
    rg = pd.read_sql(researchinterval_query, con=db)
    # print('__________')
    # print(researchinterval_query)
    # print(rg)

    if rg.empty:
        return ['No data found']
    else:
        # define columns
        if overlap_days == 'N':
            rg['RESEARCHGROUP_INDEX'] = rg['RESEARCHGROUP'] + ' | ' + rg['UOM']
            # rg['OPERATINGDATE_STR'] = 
        elif overlap_days == 'Y':
            rg['RESEARCHGROUP_INDEX'] = rg['RESEARCHGROUP'] + ' | ' + rg['UOM'] + ' | ' +rg['OPERATINGDATE'].astype(str)
        
        # print('research group with index')
        # print(rg)
        rg.set_index(['RESEARCHGROUP_INDEX',], inplace=True)

        # gather DST transition dates
        local_tz = pytz.timezone('US/Eastern')
        dst_dates = pd.DataFrame([local_tz._utc_transition_times]).transpose()
        dst_dates.rename({0: "TRANSITION_DATE"}, axis='columns', inplace=True)
        dst_dates.drop(dst_dates.index[0], inplace=True)
        dst_dates['TRUNCATED'] = pd.to_datetime(dst_dates['TRANSITION_DATE'], errors='coerce').dt.normalize()

        if overlap_days == 'N':
            for counter, opday in enumerate(rg['OPERATINGDATE'].unique(), 1):
                # sunset to first operating date
                daychunk = rg.loc[(rg['OPERATINGDATE'] == opday)]

                # get operating date
                operatingdate = daychunk['OPERATINGDATE'][0]
                operatingdate_local = local_tz.localize(operatingdate)

                # determine if DST transition date
                transition_date = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

                if transition_date.shape[0] == 1:
                    if transition_date.iloc[0][1].month <= 5:
                        hours_in_day = 23
                    else:
                        hours_in_day = 25
                else:
                    hours_in_day = 24

                # rename interval columns with datetime
                interval_cols = []
                for i in range(0, hours_in_day):
                    interval_start = (operatingdate_local + timedelta(hours=i))

                    interval_cols.append(interval_start)
                    col_name = f"T{i:0>3}"

                    daychunk.rename(columns={col_name: interval_start}, inplace=True)

                if hours_in_day < 25:
                    daychunk.drop(['T024'], axis=1, inplace=True)

                if hours_in_day < 24:
                    daychunk.drop(['T023'], axis=1, inplace=True)

                if counter == 1:
                    chart_rg = daychunk[interval_cols].transpose()
                else:
                    chart_rg = chart_rg.append(daychunk[interval_cols].transpose())
            chart_rg.sort_index(inplace=True)
        else:
            # get operating date
            operatingdate = rg['OPERATINGDATE'][0]
            operatingdate_local = local_tz.localize(operatingdate)

            # determine if DST transition date
            transition_date = dst_dates.loc[(dst_dates['TRUNCATED'] == operatingdate)]

            if transition_date.shape[0] == 1:
                if transition_date.iloc[0][1].month <= 5:
                    hours_in_day = 23
                else:
                    hours_in_day = 25
            else:
                hours_in_day = 24

            interval_cols = []
            for i in range(0, hours_in_day):
                # interval_start = (operatingdate_local + timedelta(hours=i))
                if dst_dates.shape[0] == 1:
                    if operatingdate.month > 6:
                        # fall DST transition date
                        if i < 2:
                            interval_label = str(i).zfill(2) + ':00'
                        elif i == 2:  # interval_start in transitiondates:
                            interval_label = str(i-1).zfill(2) + ':00*'
                        elif i > 2:
                            interval_label = str(i-1).zfill(2) + ':00'
                    else:
                        # spring DST transition date
                        if i < 2:
                            interval_label = str(i).zfill(2) + ':00'
                        else:
                            interval_label = str(i+1).zfill(2) + ':00'
                else:
                    # normal non DST transition day
                    interval_label = str(i).zfill(2) + ':00'

                interval_cols.append(interval_label)
                col_name = f"T{i:0>3}"

                rg.rename(columns={col_name: interval_label}, inplace=True)

            if hours_in_day < 25:
                rg.drop(['T024'], axis=1, inplace=True)

            if hours_in_day < 24:
                rg.drop(['T023'], axis=1, inplace=True)

            chart_rg = rg[interval_cols].transpose()
            
        # print(chart_rg)
        return [html.Div(
            [dcc.Graph(
                id='research_graph',
                figure={
                    'data': [go.Scattergl(
                        x=chart_rg.index,
                        y=chart_rg[i],
                        # text=chart_dff[i],
                        mode='lines',
                        opacity=1,
                        name=i,
                        connectgaps=False
                        ) for i in chart_rg.columns],
                    'layout': dict(
                        height=600,
                        xaxis={'title': 'Time',
                               'nticks': hours_in_day,
                               'showgrid': False,
                               'tickangle': -45},
                        title=researchgroup_list,
                        hovermode='closest',
                        hoverlabel=dict(namelength=-1),
                        paper_bgcolor=paperbackgroundcolor,
                        plot_bgcolor=plotbackgroundcolor,
                        legend_bgcolor=legendbackgroundcolor,
                        clickmode='event+select',
                        )
                    },
                ),

                dash_table.DataTable(
                    id='research-datatable-interactivity',
                    columns=[{"name": i, "id": i, "deletable": True} for i in rg.columns],
                    data=rg.to_dict("rows"),
                    editable=False,
                    filter_action='native',
                    sort_action='native',
                    sort_mode="multi",
                    row_selectable="multi",
                    row_deletable=True,
                    selected_rows=[],
                    style_table={'overflowX': 'scroll', 'overflowY': 'scroll', 'max_height': 300, 'width': '100%'},
                    # style_as_list_view=True,
                    style_cell={
                        # all three widths are needed
                        'minWidth': '120px',
                        'whiteSpace': 'no-wrap',
                        'overflow': 'hidden',
                        'textOverflow': 'ellipsis',
                        },
                    style_data={
                        'backgroundColor': lightcolor,
                        'border': '0px solid white',
                        'fontColor': 'black',
                        'borderTop': '2px solid ' + darkcolor,
                        },
                    style_header={
                        'backgroundColor': lightcolor,
                        'fontWeight': 'bold',
                        'border': '0px solid white',
                        },
                    style_data_conditional=[
                        {'if': {'column_id': 'TOTAL',
                                'filter_query': 'TOTAL < 0'},
                         'backgroundColor': 'rgb(210, 230, 256)',
                         # 'color': 'white',
                         },
                        {'if': {'column_id': 'PLC',
                                'filter_query': 'PLC < 0'},
                         'backgroundColor': 'rgb(210, 230, 256)',
                         # 'color': 'white',
                         },
                        {'if': {'column_id': 'TPLC',
                                'filter_query': 'TPLC < 0'},
                         'backgroundColor': 'rgb(210, 230, 256)',
                         # 'color': 'white',
                         },
                    ],
                    css=[{
                        'selector': '.dash-cell div.dash-cell-value',
                        'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                        }],
                    export_format='xlsx',
                    export_headers='display',
                    merge_duplicate_headers=True
                    ),

             ]),

            # store lls records in browser memory
            # dff.to_dict('records')
        ]




# ┌┐ ┬─┐┌─┐┬ ┬┌─┐┌─┐  ┌┬┐┌─┐┌┬┐┌─┐     ┌┬┐┌─┐┌─┐┌─┐┬─┐┬┌┐ ┌─┐   ┌┬┐┌─┐┌┐ ┬  ┌─┐
# ├┴┐├┬┘│ ││││└─┐├┤    ││├─┤ │ ├─┤──────││├┤ └─┐│  ├┬┘│├┴┐├┤     │ ├─┤├┴┐│  ├┤
# └─┘┴└─└─┘└┴┘└─┘└─┘  ─┴┘┴ ┴ ┴ ┴ ┴     ─┴┘└─┘└─┘└─┘┴└─┴└─┘└─┘────┴ ┴ ┴└─┘┴─┘└─┘

@app.callback(
    Output('description-container', "children"),
    [Input('tables-dropdown', 'value')]
    )
def describe_table (table_name):
    # load description for table
    description_query = '''select column_name, data_type, data_length, nullable, virtual_column from user_tab_cols where table_name = '{table_name}' order by column_id'''.format(table_name=table_name)
    description = pd.read_sql(description_query, con=db)

    row_count = description.shape[0]
    if row_count == 0:
        # no no rows
        return html.Div(['No description for ' + table_name])
    else:
        return html.Div([
            dash_table.DataTable(
                id='table-description',
                columns=[{"name": i, "id": i} for i in description.columns],
                data=description.to_dict("rows"),
                selected_rows=[],
                style_table={'width': '60%',
                             'maxHeight': '900px',
                             'overflowY': 'scroll'},
                style_as_list_view=True,
                style_cell={
                    'whiteSpace': 'no-wrap',
                    'overflow': 'hidden',
                    'textAlign': 'left',
                    'textOverflow': 'ellipsis',
                },
                style_data={'backgroundColor': lightcolor,
                            'border': '0px solid white',
                            'fontColor': 'black',
                            'borderTop': '2px solid ' + darkcolor,
                            },
                style_header={
                    'backgroundColor': lightcolor,
                    'fontWeight': 'bold',
                    'border': '0px solid white',
                    },
                style_data_conditional=[
                    {'if': {'column_id': 'COLUMN_NAME'},
                     'width': '180px'},
                    {'if': {'column_id': 'DATA_TYPE'},
                     'width': '100px'},
                    {'if': {'column_id': 'DATA_LENGTH'},
                     'width': '100px'},
                    {'if': {'column_id': 'NULLABLE'},
                     'width': '70px'},
                    {'if': {'column_id': 'VIRTUAL_COLUMN'},
                     'width': '100px'},
                    ],
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                    }],
            ),
        ])



# ┌┐ ┬─┐┌─┐┬ ┬┌─┐┌─┐  ┌┬┐┌─┐┌┬┐┌─┐      ┌┐ ┬─┐┌─┐┬ ┬┌─┐┌─┐    ┌┬┐┌─┐┌┬┐┌─┐
# ├┴┐├┬┘│ ││││└─┐├┤    ││├─┤ │ ├─┤──────├┴┐├┬┘│ ││││└─┐├┤      ││├─┤ │ ├─┤
# └─┘┴└─└─┘└┴┘└─┘└─┘  ─┴┘┴ ┴ ┴ ┴ ┴      └─┘┴└─└─┘└┴┘└─┘└─┘─────┴┘┴ ┴ ┴ ┴ ┴

@app.callback(
    Output('browse-tables-container', "children"),
    [Input('retrieve-table-button', 'n_clicks')],
    [State('tables-dropdown', 'value'),
     State('where-clause', 'value')]
    )
def browse_data(n_clicks, table_name, where_clause):
    """Browse database tables and present/graph the queried data."""
    if n_clicks is not None:
        # load data for table
        if where_clause:  # abbreviatd form of if where_clause != ''
            # trim leading spaces if any and see if it starts with "where"
            where_clause_start = where_clause.strip()[0:5]
            if where_clause_start.casefold() != 'where'.casefold():
                where_clause = 'where ' + where_clause

        tabledata_query = '''select * from {table_name} {where_clause} order by 1'''.format(table_name=table_name, where_clause=where_clause)
        tabledata = pd.read_sql(tabledata_query, con=db)

        if 'T000' in tabledata.columns:
            # interval table selected, display graph of selected rows
            if table_name == 'CHANNELINTERVAL':
                identifier_fields = '''meter||' | '||to_char(channel)'''
                date_field = 'STARTTIME'
            elif table_name == 'DATAAGGINTERVAL':
                identifier_fields = '''DATAAGGRUNID ||' | '|| (case when CALCULATIONTYPE = 'INTERNAL' then DATAAGGREPORT ||' | ' end) || nvl2(MARKET, MARKET ||' | ', null) || nvl2(RETAILER, RETAILER ||' | ', null) || nvl2(DISCO, DISCO ||' | ', null) || nvl2(PROFILECLASS, PROFILECLASS ||' | ', null) || nvl2(LOSSCLASS, LOSSCLASS ||' | ', null) ||
                    nvl2(UFEZONE, UFEZONE ||' | ', null) || nvl2(LMPBUS, LMPBUS ||' | ', null) || nvl2(DEMANDRESPONSEZONE, DEMANDRESPONSEZONE ||' | ', null) || nvl2(RATEFACTOR, RATEFACTOR ||' | ', null) || nvl2(CUSTOMERCLASS, CUSTOMERCLASS ||' | ', null) ||
                    nvl2(DYNAMICPRICING, DYNAMICPRICING ||' | ', null) || nvl2(DIRECTLOADCONTROL, DIRECTLOADCONTROL ||' | ', null) || nvl2(METERTYPE, METERTYPE ||' | ', null) || nvl2(WEATHERSENSITIVITY, WEATHERSENSITIVITY ||' | ', null) ||
                    nvl2(WEATHERZONE, WEATHERZONE ||' | ', null) || nvl2(METHOD, METHOD ||' | ', null) || DATATYPE ||' | '|| CALCULATIONTYPE'''
                date_field = 'OPERATINGDATE'
            elif table_name == 'LLSINTERVAL':
                identifier_fields = '''DATAAGGRUNID ||' | '|| nvl2(MARKET, MARKET ||' | ', null) || nvl2(RETAILER, RETAILER ||' | ', null) || nvl2(DISCO, DISCO ||' | ', null) || nvl2(PROFILECLASS, PROFILECLASS ||' | ', null) || nvl2(LOSSCLASS, LOSSCLASS ||' | ', null) ||
                    nvl2(UFEZONE, UFEZONE ||' | ', null) || nvl2(LMPBUS, LMPBUS ||' | ', null) || nvl2(DEMANDRESPONSEZONE, DEMANDRESPONSEZONE ||' | ', null) || nvl2(RATEFACTOR, RATEFACTOR ||' | ', null) || nvl2(CUSTOMERCLASS, CUSTOMERCLASS ||' | ', null) ||
                    nvl2(DYNAMICPRICING, DYNAMICPRICING ||' | ', null) || nvl2(DIRECTLOADCONTROL, DIRECTLOADCONTROL ||' | ', null) || nvl2(METERTYPE, METERTYPE ||' | ', null) || nvl2(WEATHERSENSITIVITY, WEATHERSENSITIVITY ||' | ', null) ||
                    nvl2(WEATHERZONE, WEATHERZONE ||' | ', null) || nvl2(METHOD, METHOD ||' | ', null) || DATATYPE'''
                date_field = 'OPERATINGDATE'
            elif table_name == 'MISCELLANEOUSINTERVAL':
                identifier_fields = '''IDENTIFIER'''
                date_field = 'STARTTIME'
            elif table_name == 'SERVICEPOINTDATAAGGRUN':
                identifier_fields = '''dataaggrunid ||' | '|| servicepoint ||' | '|| datatype'''
                date_field = 'OPERATINGDATE'

            if table_name == 'SERVICEPOINTDATAAGGRUN':
                tablechart_query = '''
                select identifier, starttime_tz + numtodsinterval(intervallength * value, 'second') starttime, quantity
                  from
                      (
                      select * from
                         (select {identifier_fields} as identifier,
                                 intervallength,
                                 from_tz(cast({date_field} as timestamp), 'US/Eastern' ) as STARTTIME_tz,
                                 T000, T001, T002, T003, T004, T005, T006, T007, T008, T009, T010, T011, T012, T013, T014, T015, T016, T017, T018, T019,
                                 T020, T021, T022, T023, T024
                            from {table_name}
                           {where_clause}  )
                      unpivot
                         ( (quantity)
                            for value in
                               (T000 as '0',     T001 as '1',     T002 as '2',     T003 as '3',     T004 as '4',     T005 as '5',     T006 as '6',     T007 as '7',     T008 as '8',     T009 as '9',
                                T010 as '10',    T011 as '11',    T012 as '12',    T013 as '13',    T014 as '14',    T015 as '15',    T016 as '16',    T017 as '17',    T018 as '18',    T019 as '19',
                                T020 as '20',    T021 as '21',    T022 as '22',    T023 as '23',    T024 as '24'
                                )
                          )
                     )
                 order by identifier, starttime'''.format(table_name=table_name, where_clause=where_clause, identifier_fields=identifier_fields, date_field=date_field)
            else:
                tablechart_query = '''
                select identifier, starttime_tz + numtodsinterval(intervallength * value, 'second') starttime, quantity
                  from
                      (
                      select * from
                         (select {identifier_fields} as identifier,
                                 intervallength,
                                 from_tz(cast({date_field} as timestamp), 'US/Eastern' ) as STARTTIME_tz,
                                 T000, T001, T002, T003, T004, T005, T006, T007, T008, T009, T010, T011, T012, T013, T014, T015, T016, T017, T018, T019,
                                 T020, T021, T022, T023, T024, T025, T026, T027, T028, T029, T030, T031, T032, T033, T034, T035, T036, T037, T038, T039,
                                 T040, T041, T042, T043, T044, T045, T046, T047, T048, T049, T050, T051, T052, T053, T054, T055, T056, T057, T058, T059,
                                 T060, T061, T062, T063, T064, T065, T066, T067, T068, T069, T070, T071, T072, T073, T074, T075, T076, T077, T078, T079,
                                 T080, T081, T082, T083, T084, T085, T086, T087, T088, T089, T090, T091, T092, T093, T094, T095, T096, T097, T098, T099,
                                 --
                                 T100, T101, T102, T103, T104, T105, T106, T107, T108, T109, T110, T111, T112, T113, T114, T115, T116, T117, T118, T119,
                                 T120, T121, T122, T123, T124, T125, T126, T127, T128, T129, T130, T131, T132, T133, T134, T135, T136, T137, T138, T139,
                                 T140, T141, T142, T143, T144, T145, T146, T147, T148, T149, T150, T151, T152, T153, T154, T155, T156, T157, T158, T159,
                                 T160, T161, T162, T163, T164, T165, T166, T167, T168, T169, T170, T171, T172, T173, T174, T175, T176, T177, T178, T179,
                                 T180, T181, T182, T183, T184, T185, T186, T187, T188, T189, T190, T191, T192, T193, T194, T195, T196, T197, T198, T199,
                                 --
                                 T200, T201, T202, T203, T204, T205, T206, T207, T208, T209, T210, T211, T212, T213, T214, T215, T216, T217, T218, T219,
                                 T220, T221, T222, T223, T224, T225, T226, T227, T228, T229, T230, T231, T232, T233, T234, T235, T236, T237, T238, T239,
                                 T240, T241, T242, T243, T244, T245, T246, T247, T248, T249, T250, T251, T252, T253, T254, T255, T256, T257, T258, T259,
                                 T260, T261, T262, T263, T264, T265, T266, T267, T268, T269, T270, T271, T272, T273, T274, T275, T276, T277, T278, T279,
                                 T280, T281, T282, T283, T284, T285, T286, T287, T288, T289, T290, T291, T292, T293, T294, T295, T296, T297, T298, T299
                            from {table_name}
                           {where_clause}  )
                      unpivot
                         ( (quantity)
                            for value in
                               (T000 as '0',     T001 as '1',     T002 as '2',     T003 as '3',     T004 as '4',     T005 as '5',     T006 as '6',     T007 as '7',     T008 as '8',     T009 as '9',
                                T010 as '10',    T011 as '11',    T012 as '12',    T013 as '13',    T014 as '14',    T015 as '15',    T016 as '16',    T017 as '17',    T018 as '18',    T019 as '19',
                                T020 as '20',    T021 as '21',    T022 as '22',    T023 as '23',    T024 as '24',    T025 as '25',    T026 as '26',    T027 as '27',    T028 as '28',    T029 as '29',
                                T030 as '30',    T031 as '31',    T032 as '32',    T033 as '33',    T034 as '34',    T035 as '35',    T036 as '36',    T037 as '37',    T038 as '38',    T039 as '39',
                                T040 as '40',    T041 as '41',    T042 as '42',    T043 as '43',    T044 as '44',    T045 as '45',    T046 as '46',    T047 as '47',    T048 as '48',    T049 as '49',
                                T050 as '50',    T051 as '51',    T052 as '52',    T053 as '53',    T054 as '54',    T055 as '55',    T056 as '56',    T057 as '57',    T058 as '58',    T059 as '59',
                                T060 as '60',    T061 as '61',    T062 as '62',    T063 as '63',    T064 as '64',    T065 as '65',    T066 as '66',    T067 as '67',    T068 as '68',    T069 as '69',
                                T070 as '70',    T071 as '71',    T072 as '72',    T073 as '73',    T074 as '74',    T075 as '75',    T076 as '76',    T077 as '77',    T078 as '78',    T079 as '79',
                                T080 as '80',    T081 as '81',    T082 as '82',    T083 as '83',    T084 as '84',    T085 as '85',    T086 as '86',    T087 as '87',    T088 as '88',    T089 as '89',
                                T090 as '90',    T091 as '91',    T092 as '92',    T093 as '93',    T094 as '94',    T095 as '95',    T096 as '96',    T097 as '97',    T098 as '98',    T099 as '99',
                                --
                                T100 as '100',   T101 as '101',   T102 as '102',   T103 as '103',   T104 as '104',   T105 as '105',   T106 as '106',   T107 as '107',   T108 as '108',   T109 as '109',
                                T110 as '110',   T111 as '111',   T112 as '112',   T113 as '113',   T114 as '114',   T115 as '115',   T116 as '116',   T117 as '117',   T118 as '118',   T119 as '119',
                                T120 as '120',   T121 as '121',   T122 as '122',   T123 as '123',   T124 as '124',   T125 as '125',   T126 as '126',   T127 as '127',   T128 as '128',   T129 as '129',
                                T130 as '130',   T131 as '131',   T132 as '132',   T133 as '133',   T134 as '134',   T135 as '135',   T136 as '136',   T137 as '137',   T138 as '138',   T139 as '139',
                                T140 as '140',   T141 as '141',   T142 as '142',   T143 as '143',   T144 as '144',   T145 as '145',   T146 as '146',   T147 as '147',   T148 as '148',   T149 as '149',
                                T150 as '150',   T151 as '151',   T152 as '152',   T153 as '153',   T154 as '154',   T155 as '155',   T156 as '156',   T157 as '157',   T158 as '158',   T159 as '159',
                                T160 as '160',   T161 as '161',   T162 as '162',   T163 as '163',   T164 as '164',   T165 as '165',   T166 as '166',   T167 as '167',   T168 as '168',   T169 as '169',
                                T170 as '170',   T171 as '171',   T172 as '172',   T173 as '173',   T174 as '174',   T175 as '175',   T176 as '176',   T177 as '177',   T178 as '178',   T179 as '179',
                                T180 as '180',   T181 as '181',   T182 as '182',   T183 as '183',   T184 as '184',   T185 as '185',   T186 as '186',   T187 as '187',   T188 as '188',   T189 as '189',
                                T190 as '190',   T191 as '191',   T192 as '192',   T193 as '193',   T194 as '194',   T195 as '195',   T196 as '196',   T197 as '197',   T198 as '198',   T199 as '199',
                                --
                                T100 as '200',   T101 as '201',   T102 as '202',   T103 as '203',   T104 as '204',   T105 as '205',   T106 as '206',   T107 as '207',   T108 as '208',   T109 as '209',
                                T110 as '210',   T111 as '211',   T112 as '212',   T113 as '213',   T114 as '214',   T115 as '215',   T116 as '216',   T117 as '217',   T118 as '218',   T119 as '219',
                                T120 as '220',   T121 as '221',   T122 as '222',   T123 as '223',   T124 as '224',   T125 as '225',   T126 as '226',   T127 as '227',   T128 as '228',   T129 as '229',
                                T130 as '230',   T131 as '231',   T132 as '232',   T133 as '233',   T134 as '234',   T135 as '235',   T136 as '236',   T137 as '237',   T138 as '238',   T139 as '239',
                                T140 as '240',   T141 as '241',   T142 as '242',   T143 as '243',   T144 as '244',   T145 as '245',   T146 as '246',   T147 as '247',   T148 as '248',   T149 as '249',
                                T150 as '250',   T151 as '251',   T152 as '252',   T153 as '253',   T154 as '254',   T155 as '255',   T156 as '256',   T157 as '257',   T158 as '258',   T159 as '259',
                                T160 as '260',   T161 as '261',   T162 as '262',   T163 as '263',   T164 as '264',   T165 as '265',   T166 as '266',   T167 as '267',   T168 as '268',   T169 as '269',
                                T170 as '270',   T171 as '271',   T172 as '272',   T173 as '273',   T174 as '274',   T175 as '275',   T176 as '276',   T177 as '277',   T178 as '278',   T179 as '279',
                                T180 as '280',   T181 as '281',   T182 as '282',   T183 as '283',   T184 as '284',   T185 as '285',   T186 as '286',   T187 as '287',   T188 as '288',   T189 as '289',
                                T190 as '290',   T191 as '291',   T192 as '292',   T193 as '293',   T194 as '294',   T195 as '295',   T196 as '296',   T197 as '297',   T198 as '298',   T199 as '299'
                                )
                          )
                     )
                 order by identifier, starttime'''.format(table_name=table_name, where_clause=where_clause, identifier_fields=identifier_fields, date_field=date_field)

            tablechart_data = pd.read_sql(tablechart_query, con=db)
            print(tablechart_data)
            full_chart = tablechart_data.pivot(index='STARTTIME', columns='IDENTIFIER', values='QUANTITY')

            # create traces, layout, and figure
            browse_traces = []

            for i in full_chart.columns:
                trace = go.Scattergl(
                    x=full_chart.index,
                    y=full_chart[i],
                    yaxis='y',
                    mode='lines',
                    name=i,
                    connectgaps=False,
                    )
                browse_traces.append(trace)

            browse_layout = go.Layout(
                title=table_name,
                yaxis={'side': 'left'},
                legend=dict(orientation='v'),
                # xaxis = dict(nticks = 12,), #tickangle = -45),
                hovermode='closest',
                hoverlabel=dict(namelength=-1),
                paper_bgcolor=paperbackgroundcolor,
                plot_bgcolor=plotbackgroundcolor,
                legend_bgcolor=legendbackgroundcolor,
                )

            browse_fig = go.Figure(
                data=browse_traces,
                layout=browse_layout
                )

            return html.Div([
                dcc.Loading(id="loading-records", children=[
                    html.Div([
                        dcc.Graph(
                            id='browse-data-graph1',
                            figure=browse_fig
                            )
                        ], style={'display': 'inline-block', 'width': '100%', 'height': 450}),

                    html.Div([
                        html.Button('Add Row', id='add-row-button', n_clicks=0),
                        html.Button('Update Selection', id='update-rows-button', n_clicks=0),
                        html.Button('Delete Selection', id='delete-rows-button', n_clicks=0),
                        ], style={'width': '60%',
                                  'display': 'inline-block',
                                  'verticalAlign': 'bottom',
                                  'margin': '10px 10px 10px 5px',
                                  }),

                    dash_table.DataTable(
                        id='database-table-data',
                        columns=[{"name": i, "id": i} for i in tabledata.columns],
                        data=tabledata.to_dict("rows"),
                        # data=tabledata,
                        editable=True,
                        filter_action='native',
                        sort_action='native',
                        sort_mode="multi",
                        row_selectable="multi",
                        row_deletable=True,
                        selected_rows=[],
                        style_table={'overflowX': 'scroll', 'overflowY': 'scroll', 'max_height': 400},
                        # style_as_list_view=True,
                        style_header={
                            'backgroundColor': darkcolor,
                            'fontWeight': 'bold',
                            'color': 'white'
                            },
                        style_cell={
                            # all three widths are needed
                            'minWidth': '120px',
                            'whiteSpace': 'no-wrap',
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                        },
                        style_data_conditional=[
                            {'if': {'row_index': 'odd'},
                             'backgroundColor': lightcolor,
                             }
                            ],
                        css=[{
                            'selector': '.dash-cell div.dash-cell-value',
                            'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                            }],
                        export_format='xlsx',
                        export_headers='display',
                        merge_duplicate_headers=True
                    ),
                ])
            ])

        else:
            # no graph needed
            return html.Div([
                html.Div([
                    html.Button('Add Row', id='add-row-button',  n_clicks=0),
                    html.Button('Update Selection', id='update-rows-button', n_clicks=0),
                    html.Button('Delete Selection', id='delete-rows-button', n_clicks=0),
                ], style={'width': '60%',
                          'display': 'inline-block',
                          'verticalAlign': 'bottom',
                          'margin': '10px 10px 10px 5px',
                          }),

                dcc.Loading(id="loading-records", children=[
                    dash_table.DataTable(
                        id='database-table-data',
                        columns=[{"name": i, "id": i} for i in tabledata.columns],
                        data=tabledata.to_dict("rows"),
                        # data=tabledata,
                        editable=True,
                        filter_action='native',
                        sort_action='native',
                        sort_mode="multi",
                        row_selectable="multi",
                        row_deletable=True,
                        selected_rows=[],
                        style_table={'overflowX': 'scroll', 'overflowY': 'scroll', 'max_height': 600, },
                        # style_as_list_view=True,
                        style_header={
                            'backgroundColor': darkcolor,
                            'fontWeight': 'bold',
                            'color': 'white'
                            },
                        style_cell={
                            # all three widths are needed
                            'minWidth': '120px',
                            'whiteSpace': 'no-wrap',
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                            'backgroundColor': lightcolor
                        },
                        css=[{
                            'selector': '.dash-cell div.dash-cell-value',
                            'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                            }],
                        export_format='xlsx',
                        export_headers='display',
                        merge_duplicate_headers=True
                    ),


                ])

            ])


# ┌┐ ┬─┐┌─┐┬ ┬┌─┐┌─┐  ┌┬┐┌─┐┌┬┐┌─┐      ┌─┐┌┬┐┌┬┐    ┬─┐┌─┐┬ ┬
# ├┴┐├┬┘│ ││││└─┐├┤    ││├─┤ │ ├─┤──────├─┤ ││ ││    ├┬┘│ ││││
# └─┘┴└─└─┘└┴┘└─┘└─┘  ─┴┘┴ ┴ ┴ ┴ ┴      ┴ ┴─┴┘─┴┘────┴└─└─┘└┴┘

@app.callback(
    Output('database-table-data', 'data'),
    [Input('add-row-button', 'n_clicks')],
    [State('database-table-data', 'data'),
     State('database-table-data', 'columns'),
     ])
def add_row(n_clicks, rows, columns):
    if n_clicks > 0:
        rows.append({c['id']: '' for c in columns})
    return rows


# ┌┐ ┬─┐┌─┐┬ ┬┌─┐┌─┐  ┌┬┐┌─┐┌┬┐┌─┐      ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐    ┬─┐┌─┐┬ ┬┌─┐
# ├┴┐├┬┘│ ││││└─┐├┤    ││├─┤ │ ├─┤──────│ │├─┘ ││├─┤ │ ├┤     ├┬┘│ ││││└─┐
# └─┘┴└─└─┘└┴┘└─┘└─┘  ─┴┘┴ ┴ ┴ ┴ ┴      └─┘┴  ─┴┘┴ ┴ ┴ └─┘────┴└─└─┘└┴┘└─┘

@app.callback(
    Output('database-table-data', 'rows'),
    [Input('update-rows-button', 'n_clicks')],
    [State('database-table-data', 'data'),
     State('tables-dropdown', 'value'),
     State('database-table-data', 'selected_rows'),
     ])
def update_rows(n_clicks, rows, tablename, selected_rows):
    if len(selected_rows) != 0:
        # make dataframe of selected rows
        df_rows = pd.DataFrame(rows)
        df_update = df_rows.iloc[selected_rows]

        # get pk columns query
        pk_query = '''
        SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
          FROM user_constraints cons,
               user_cons_columns cols
         WHERE cols.table_name = '{tablename}'
               AND cons.constraint_type = 'P'
               AND cons.constraint_name = cols.constraint_name
         ORDER BY cols.table_name, cols.position'''.format(tablename=tablename)
        pk = pd.read_sql(pk_query, con=db)

        # get table column types
        update_columns_query = '''
        select column_name, data_type, data_length, nullable
          from user_tab_cols
         where table_name = '{tablename}'
               and virtual_column = 'NO'
               and column_name not in ('INSERTUSER', 'INSERTTIMESTAMP', 'UPDATEUSER', 'UPDATETIMESTAMP')
         order by column_id'''.format(tablename=tablename)

        update_columns = pd.read_sql(update_columns_query, con=db)
        # columns_list = update_columns['COLUMN_NAME'].tolist()
        update_columns.set_index(['COLUMN_NAME'], inplace=True)

#        print(pk)

        # insert_df = pd.DataFrame(columns=columns_list)
        # update_df = pd.DataFrame()

        for x in selected_rows:
            insert_row = []
            update_row = []
            update_set_text = ""
            insert_values_text = ""
            insert_column_text = ""
#            bulk_text = ""
            column_def = ""
            values = ""
            counter = 0
            for c in df_update:
                counter = counter + 1
                if c in update_columns.index:
                    data_type = update_columns.loc[c, 'DATA_TYPE']
#                    print(c + "  " + data_type)
#                    print(description.loc[x, 'DATA_TYPE'])
                    if pd.isna(df_update[c][x]) is True:
                        if ~pk['COLUMN_NAME'].str.contains(c).any():
                            update_set_text = update_set_text + c + '=null, '
                        append_string = None
                        insert_column_text = 'null'
                    elif df_update[c][x] == "":
                        if ~pk['COLUMN_NAME'].str.contains(c).any():
                            update_set_text = update_set_text + c + '=null, '
                        append_string = None
                        insert_column_text = 'null'
                    elif data_type == 'DATE':
                        if ~pk['COLUMN_NAME'].str.contains(c).any():
                            update_set_text = update_set_text + c + '=to_date(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss\'), '
                        append_string = 'to_date(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss\')'
                        insert_column_text = append_string
                    elif data_type == 'TIMESTAMP(6)':
                        if ~pk['COLUMN_NAME'].str.contains(c).any():
                            update_set_text = update_set_text + c + '=to_timestamp(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss.ff\'), '
                        append_string = 'to_timestamp(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss.ff\')'
                        insert_column_text = append_string
                    elif data_type in ('NUMBER', 'INTEGER', 'FLOAT'):
                        if ~pk['COLUMN_NAME'].str.contains(c).any():
                            update_set_text = update_set_text + c + '=' + str(df_update[c][x]) + ', '
                        append_string = str(df_update[c][x])
                        insert_column_text = append_string
                    else:
                        if ~pk['COLUMN_NAME'].str.contains(c).any():
                            update_set_text = update_set_text + c + '=\'' + str(df_update[c][x]) + '\', '
                        append_string = str(df_update[c][x])
                        insert_column_text = '\'' + append_string + '\''

                    insert_row.append(append_string)
                    column_def = column_def + c + ', '
                    values = values + ':' + str(counter) + ', '
                    insert_values_text = insert_values_text + insert_column_text + ', '

            update_set_text = update_set_text[:-2]
            insert_values_text = insert_values_text[:-2]
            column_def = column_def[:-2]
            values = values[:-2]

            primary_key = ''
            update_row = copy.deepcopy(insert_row)
            for c in pk['COLUMN_NAME']:
                data_type = update_columns.loc[c, 'DATA_TYPE']
                if df_update[c][x] is None:
                    primary_key = primary_key + c + ' is null and '
                    append_string = None
                elif data_type == 'DATE':
                    primary_key = primary_key + c + '=to_date(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss\') and '
                    append_string = 'to_date(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss\')'
                elif data_type == 'TIMESTAMP(6)':
                    primary_key = primary_key + c + '=to_timestamp(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss.ff\') and '
                    append_string = 'to_timestamp(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss.ff\')'
                elif data_type in ('NUMBER', 'INTEGER', 'FLOAT'):
                    primary_key = primary_key + c + '=' + str(df_update[c][x]) + ' and '
                    append_string = str(df_update[c][x])
                else:
                    primary_key = primary_key + c + '=\'' + str(df_update[c][x]) + '\' and '
                    append_string = str(df_update[c][x])

                update_row.append(append_string)

            primary_key = primary_key[:-4]

            # update_query=f"""update {tablename} \n   set {update_set_text} \n where {primary_key} """

            # insert_query=f"""insert into {tablename} ({column_def}) \n      values ( {insert_values_text} )"""

            # query_block="begin \n   " + insert_query + "\n EXCEPTION\n   when DUP_VAL_ON_INDEX then \n      " + update_query + ";\nend;"
            merge_text = f""" merge into {tablename} using dual on ({primary_key})
                             when matched then update set {update_set_text}
                             when not matched then insert ({column_def}) values ( {insert_values_text} )"""
            print()
            print(merge_text)
            print()
            # query_block="begin \n   " + insert_query + ";\n EXCEPTION\n   when DUP_VAL_ON_INDEX then \n      " + update_query + ";\nend;"

            # insert_df = insert_df.append(pd.DataFrame([insert_row], columns=columns_list), ignore_index=True)
            # update_df.append(update_row)

            # cur.execute(query_block) #query_block, df_update[x] )
            cur.execute(merge_text)
            db.commit()

    return rows


# ┌┐ ┬─┐┌─┐┬ ┬┌─┐┌─┐  ┌┬┐┌─┐┌┬┐┌─┐      ┬ ┬┌─┐┌┬┐┌─┐┌┬┐┌─┐    ┬─┐┌─┐┬ ┬┌─┐
# ├┴┐├┬┘│ ││││└─┐├┤    ││├─┤ │ ├─┤──────│ │├─┘ ││├─┤ │ ├┤     ├┬┘│ ││││└─┐
# └─┘┴└─└─┘└┴┘└─┘└─┘  ─┴┘┴ ┴ ┴ ┴ ┴      └─┘┴  ─┴┘┴ ┴ ┴ └─┘────┴└─└─┘└┴┘└─┘

# @app.callback(
#     Output('database-table-data', 'rows'),
#     [Input('delete-rows-button', 'n_clicks'),
#     ],
#     [State('database-table-data', 'data'),
#      State('tables-dropdown', 'value'),
#      State('database-table-data', 'selected_rows'),
#     ])
# def delete_rows(n_clicks, rows, tablename, selected_rows):
#     if len(selected_rows) != 0:
#         #make dataframe of selected rows
#         df_rows = pd.DataFrame(rows)
#         df_update = df_rows.iloc[selected_rows]

#         # get pk columns query
#         pk_query = '''
#         SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
#           FROM user_constraints cons,
#                user_cons_columns cols
#          WHERE cols.table_name = '{tablename}'
#                AND cons.constraint_type = 'P'
#                AND cons.constraint_name = cols.constraint_name
#          ORDER BY cols.table_name, cols.position'''.format(tablename=tablename)
#         pk = pd.read_sql(pk_query, con=db)

#         for x in selected_rows:
#             insert_row = []
#             update_row = []
#             delete_where_text = ""
#             column_def = ""
#             values = ""
#             counter = 0

#             column_def = column_def[:-2]
#             values = values[:-2]

#             primary_key = ''
#             update_row = copy.deepcopy(insert_row)
#             for c in pk['COLUMN_NAME']:
#                 data_type = update_columns.loc[c, 'DATA_TYPE']
#                 if df_update[c][x] is None:
#                     primary_key = primary_key + c + ' is null and '
#                     append_string = None
#                 elif data_type == 'DATE':
#                     primary_key = primary_key + c + '=to_date(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss\') and '
#                     append_string = 'to_date(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss\')'
#                 elif data_type == 'TIMESTAMP(6)':
#                     primary_key = primary_key + c + '=to_timestamp(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss.ff\') and '
#                     append_string = 'to_timestamp(\'' + str(df_update[c][x])[:10] + ' ' + str(df_update[c][x])[11:] + '\', \'yyyy-mm-dd hh24:mi:ss.ff\')'
#                 elif data_type in ('NUMBER', 'INTEGER', 'FLOAT'):
#                     primary_key = primary_key + c + '=' + str(df_update[c][x]) + ' and '
#                     append_string = str(df_update[c][x])
#                 else:
#                     primary_key = primary_key + c + '=\'' + str(df_update[c][x]) + '\' and '
#                     append_string = str(df_update[c][x])

#                 update_row.append(append_string)

#             primary_key = primary_key[:-4]

#             # update_query=f"""update {tablename} \n   set {update_set_text} \n where {primary_key} """

#             # insert_query=f"""insert into {tablename} ({column_def}) \n      values ( {insert_values_text} )"""

#             # query_block="begin \n   " + insert_query + "\n EXCEPTION\n   when DUP_VAL_ON_INDEX then \n      " + update_query + ";\nend;"
#             merge_text =f""" merge into {tablename} using dual on ({primary_key})
#                              when matched then update set {update_set_text}
#                              when not matched then insert values ( {insert_values_text} )"""
#             print(merge_text)
#             # query_block="begin \n   " + insert_query + ";\n EXCEPTION\n   when DUP_VAL_ON_INDEX then \n      " + update_query + ";\nend;"

#             # insert_df = insert_df.append(pd.DataFrame([insert_row], columns=columns_list), ignore_index=True)
#             # update_df.append(update_row)

#             # cur.execute(query_block) #query_block, df_update[x] )
#             cur.execute(merge_text) #query_block, df_update[x] )
#             db.commit()

#     return rows


# if __name__ == '__main__':
#     app.run_server(debug=False)

# Run flask app
if __name__ == "__main__":
    app.run_server(debug=False, host='0.0.0.0', port=8050)
