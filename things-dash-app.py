"""
Things Database Connector
"""
#%% Import packages

# Data wrangling
import pandas as pd
import numpy as np
import dtale
import sqlite3
import csv
import os
import shutil
import time as time
import datetime
import datetime as dt
from datetime import datetime

# Visualization
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import dash as dash
from dash import html
from dash.dependencies import Input, Output, State
from dash import dcc
from flask import request

# User string '/Users/name'
from credentials import user



#%% Connect to Database
sql_connect = sqlite3.connect(f'{user}/Library/Group Containers/JLMPQHK86H.com.culturedcode.ThingsMac/Things Database.thingsdatabase/main.sqlite')
cursor = sql_connect.cursor()


#%% SQL-Query
query = 'SELECT uuid, title, notes, area, actionGroup, userModificationDate, creationDate, dueDate, startDate, stopDate, status, project, type FROM TMTask;'
query2 = 'SELECT uuid, userModificationDate, creationDate, title, status, stopDate, "index", task FROM TMChecklistItem;'
query3 = 'SELECT uuid, title FROM TMArea;'
results = cursor.execute(query).fetchall()

#%% Executing Query
data = dt
data = pd.read_sql_query(query, sql_connect)
data2 = pd.read_sql_query(query2, sql_connect)
data3 = pd.read_sql_query(query3, sql_connect)

#%% Close connection
sql_connect.close()


#%% Calculating KPIs
tasks_number = data.groupby('project')['title'].agg('count')
print(tasks_number)


#%% Preparing Data

#
data_area = data[['uuid', 'area', 'title']]
data_area = data_area[data_area.area.notnull()]
data_area = data_area.sort_values('uuid')
data_area = data_area.rename(columns={'uuid':'area_id', 'title':'area_title'})

data_projects = data[['uuid', 'project', 'title']]
data_projects = data_projects[data_projects.project.notnull()]
data_projects = data_projects.sort_values('uuid')
data_projects = data_projects.rename(columns={'uuid':'projects_id', 'title':'project_title'})

data_action = data[['uuid', 'actionGroup', 'title']]
data_action = data_action[data_action.actionGroup.notnull()]
data_action = data_action.sort_values('uuid')
data_action = data_action.drop_duplicates("actionGroup")
data_action = data_action.rename(columns={'uuid':'actions_id', 'title':'action_title'})


#%% Joining Data

data_joined0 = pd.merge(data_area, data3, how='left', left_on='area', right_on='uuid')
data_joined0 = data_joined0.rename(columns=({'uuid':'box_id', 'title':'block_title'}))
data_joined0 = data_joined0[['area_id', 'area_title', 'block_title']]
data_joined = pd.merge(data, data_joined0, how='left' , left_on='area', right_on='area_id')
# data_joined = data_joined[data_joined..notnull()]
#%%
data_joined2 = pd.merge(data_joined, data_area, how='left', left_on='project', right_on='area_id')
#data_joined2 = data_joined2[data_joined2..notnull()]

#data_joined2 = data_joined2[['uuid', 'title', 'notes', 'title_x', 'title_y', 'actionGroup', 'userModificationDate', 'creationDate', 'dueDate', 'startDate', 'stopDate', 'status', 'type']]
#%%
data_joined3 = pd.merge(data_joined2, data_action, how='left', left_on='actionGroup', right_on='actionGroup')
#data_joined3 = data_joined3[data_joined3.uuid.notnull()]

#%%
data_arranged = data_joined3[['uuid',
                              'title',
                              'notes',
                              'action_title',
                              'area_title_y',
                              'status',
                              'type',
                              'userModificationDate',
                              'creationDate',
                              'dueDate',
                              'startDate',
                              'stopDate']]
data_arranged = data_arranged.rename(columns={'uuid':'task_id',
                                             'title':'title',
                                             'notes':'notes',
                                             'action_title':'action',
                                             'area_title_y':'area',
                                             'title_y':'project',
                                             'status':'status',
                                             'type':'type'})



#%% Converting Types
convert_dict = {'task_id': str,
                 'title': str,
                 'notes': str,
                 'action': str,
                 'area': str,
                 'userModificationDate': float,
                 'creationDate': float,
                 'dueDate': float,
                 'startDate': float,
                 'stopDate': float,
                 'status': int,
                 'type': int}
data_labelled = data_arranged.astype(convert_dict)

#%%
data_labelled['creationDate'] = pd.to_datetime(round(data_labelled['creationDate']), format=None, unit='s', errors='coerce')
data_labelled['startDate'] = pd.to_datetime(round(data_labelled['startDate']), format=None, unit='s', errors='coerce')
data_labelled['userModificationDate'] = pd.to_datetime(round(data_labelled['userModificationDate']), format=None, unit='s', errors='coerce')
data_labelled['dueDate'] = pd.to_datetime(round(data_labelled['dueDate']), format=None, unit='s', errors='coerce')
data_labelled['stopDate'] = pd.to_datetime(round(data_labelled['stopDate']), format=None, unit='s', errors='coerce')


print(type(data_labelled['startDate']))
print(data_labelled['startDate'])

count = data_labelled['title'].value_counts()
print(count)

#%%
data_labelled['status'] = data_labelled['status'].map({3:'gelöscht', 2:'unkown', 1:'unknown2'})
data_labelled['type'] = data_labelled['type'].map({2:'area', 1:'block', 0:'task'})

#%% Calculating Random Column

data_labelled['randCol'] = np.random.randint(1, 5, data_labelled.shape[0])
data_labelled['randCol2'] = np.random.randint(1, 1000, data_labelled.shape[0])
data_labelled

data_labelled['area'] = data_labelled['area'].fillna(0)
data_labelled['action'] = data_labelled['action'].fillna(0)

#%% Plotting
data_labelled= data_labelled[(data_labelled.type == 'task') & (data_labelled.status != 'gelöscht')]
TaskScatter = px.scatter(data_labelled,
                         x="creationDate",
                         y="randCol2",
                         color='area',
                         size='randCol2',
                         symbol='area',
                         text="title",
                         opacity=0.5,
                         hover_data=["area", 'action'],
                         width=1600,
                         height=800)


# Use date string to set xaxis range
TaskScatter.update_layout(xaxis_range=['2022-01-01', '2022-07-31'],
                          title_text="Manually Set Date Range", uniformtext_minsize=8, uniformtext_mode='hide')

TaskScatter.update_traces(textposition='top right')

# Rangeslider
TaskScatter.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")
        ])
    )
)

# Showing the plot
#TaskScatter.show()

#%% Dash App
# Create the Dash app
app = dash.Dash()

# Set up the layout with a single graph
app.layout = html.Div(children=[
    html.H1(children="AUFGABENSTEUERUNG"),
    dcc.Dropdown(id='track-dropdown',
                    options=[{'label': i, 'value': i} for i in data_labelled['area'].unique()],
                    multi=True),
    dcc.Graph(id='TaskScatter')])

  # Set up callback function
@app.callback(
    Output(component_id='TaskScatter', component_property='figure'),
    Input(component_id='track-dropdown', component_property='value')
)
def update_figure(track):
    filtered_track = data_labelled[data_labelled['area'] == track]
    TaskScatter = px.scatter(filtered_track,
                             title=f'Aufgabensteuerung: {track}',
                             x="creationDate",
                             y="randCol2",
                             color='area',
                             size='randCol2',
                             symbol='area',
                             text="title",
                             opacity=0.5,
                             hover_data=["area", 'action'],
                             width=1600,
                             height=800)
    return TaskScatter

# Run local server
if __name__ == '__main__':
    app.run_server(debug=True)