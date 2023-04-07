"""
Things Database Connector
"""
#%% Import packages
import pandas as pd
import sqlite3
import datetime as dt
import plotly.express as px
import numpy as np
import dtale
from dash import html
import csv

import os
import shutil
import seaborn as sns
import time as time
import datetime
import matplotlib.pyplot as plt
import plotly.graph_objects as go

import dash as dash
from dash import dcc
from flask import request
from dash.dependencies import Input, Output, State
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

data_projects = data[['uuid', 'title', 'area']]
data_projects = data_projects[data_projects.area.notnull()]
data_projects = data_projects.sort_values('uuid')
data_projects = data_projects.rename(columns={'uuid':'project_id', 'title':'project_title', 'area':'block_id'})

data_area = data[['uuid', 'title', 'project']]
data_area = data_area[data_area.project.notnull()]
data_area = data_area.sort_values('uuid')
data_area = data_area.rename(columns={'uuid': 'area_id', 'title': 'area_title'})

data3 = data3.rename(columns={'uuid': 'block_id', 'title': 'block_title'})

#%% Joining Data areas

data_joined0 = pd.merge(data, data_area, how='left', left_on='actionGroup', right_on='area_id')
data_joined0['project_id'] = np.where(data_joined0['project_y'].isnull(), data_joined0['project_x'], data_joined0['project_y'])
data_joined0 = data_joined0.rename(columns=({'uuid':'task_id', 'title':'task_title', 'actionGroup':'area_id_x'}))
data_joined0 = data_joined0[['task_id', 'task_title', 'notes', 'area_id', 'area_title', 'project_id', 'userModificationDate', 'creationDate', 'dueDate', 'startDate', 'stopDate', 'status', 'type']]

#%% Joining data projects

data_joined = pd.merge(data_joined0, data_projects, how='left', left_on='project_id', right_on='project_id')
# data_joined = data_joined[data_joined..notnull()]

#%%
data_joined2 = pd.merge(data_joined, data3, how='left', left_on='block_id', right_on='block_id')

#%%
data_arranged = data_joined2[['task_id',
                              'task_title',
                              'notes',
                              'area_title',
                              'area_id',
                              'project_title',
                              'project_id',
                              'block_title',
                              'block_id',
                              'type',
                              'status',
                              'userModificationDate',
                              'creationDate',
                              'dueDate',
                              'startDate',
                              'stopDate']]



#%% Dtale Data Exploration
dtale.show(data_joined2)

#%% Converting Types
convert_dict = {'task_id': str,
                 'task_title': str,
                 'notes': str,
                 'userModificationDate': float,
                 'creationDate': float,
                 'dueDate': float,
                 'startDate': float,
                 'stopDate': float,
                 'status': int,
                 'type': int}

data_labelled = data_arranged.astype(convert_dict)

#%% Converting Timestamps
data_labelled['creationDate'] = pd.to_datetime(round(data_labelled['creationDate']), format=None, unit='s', errors='coerce')
data_labelled['startDate'] = pd.to_datetime(round(data_labelled['startDate']), format=None, unit='s', errors='coerce')
data_labelled['userModificationDate'] = pd.to_datetime(round(data_labelled['userModificationDate']), format=None, unit='s', errors='coerce')
data_labelled['dueDate'] = pd.to_datetime(round(data_labelled['dueDate']), format=None, unit='s', errors='coerce')
data_labelled['stopDate'] = pd.to_datetime(round(data_labelled['stopDate']), format=None, unit='s', errors='coerce')

#%% Mapping Values
data_labelled['status'] = data_labelled['status'].map({3:'gelöscht', 2:'unkown', 1:'unknown2'})
data_labelled['type'] = data_labelled['type'].map({2:'area', 1:'block', 0:'task'})

#%% Calculating Random Column

data_labelled['randCol'] = np.random.randint(1, 5, data_labelled.shape[0])
data_labelled['randCol2'] = np.random.randint(1, 1000, data_labelled.shape[0])

data_labelled['project_title'] = data_labelled['project_title'].fillna(0)
data_labelled['area_title'] = data_labelled['area_title'].fillna(0)
data_labelled['block_title'] = data_labelled['block_title'].fillna(0)
data_labelled['status'] = data_labelled['status'].fillna(0)


#%% Subsetting Data
subset_tasks = data_labelled[data_labelled['type'] == 'task']
subset_tasks = subset_tasks[['task_id', 'task_title', 'notes', 'area_title', 'project_title', 'project_id', 'block_title', 'status', 'type', 'userModificationDate', 'creationDate', 'dueDate', 'startDate', 'stopDate']]

subset_area = data_labelled[data_labelled['type'] == 'area']
subset_area = subset_area[['task_id', 'task_title',  'area_id', 'project_id', 'area_title', 'status', 'type', 'userModificationDate', 'creationDate', 'dueDate', 'startDate', 'stopDate']]

subset_project = data_labelled[data_labelled['type'] == 'block']
subset_project = subset_project[['task_id', 'task_title', 'notes', 'status', 'type', 'userModificationDate', 'creationDate', 'dueDate', 'startDate', 'stopDate']]

#%% Plotting
plotting_data = data_labelled[(data_labelled.status != 'gelöscht')]
TaskScatter = px.scatter(plotting_data,
                         x="creationDate",
                         y="randCol2",
                         color="block_title",
                         size="randCol",
                         hover_name="task_title",
                         hover_data=["project_title", "area_title", "block_title",
                                     "status", "type", "userModificationDate", "creationDate",
                                     "dueDate", "startDate", "stopDate"])


TaskScatter.for_each_trace(
    lambda trace: trace.update(marker_symbol="square") if trace.name == "ARBEIT" else (),
)

# Use date string to set xaxis range
TaskScatter.update_layout(xaxis_range=['2022-01-01', '2022-07-31'], title_text="Manually Set Date Range", uniformtext_minsize=8, uniformtext_mode='hide')

TaskScatter.update_traces(textposition='top right')

# Rangeslider
TaskScatter.update_xaxes(
    rangeslider_visible=True,
    showgrid=False,
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
TaskScatter.show()

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
    TaskScatter = px.scatter(data_labelled,
                             title=f'Task Scatter {track}',
                             x="creationDate",
                             y="randCol",
                             color='task_title',
                             size='randCol2',
                             symbol='',
                             text="title",
                             opacity=0.5,
                             hover_data=["block_title", 'project_title'],
                             width=1600,
                             height=800)
    return TaskScatter

# Run local server
if __name__ == '__main__':
    app.run_server(debug=True)
