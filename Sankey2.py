import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import plotly.graph_objects as go
import mysql.connector
import plotly.express as px
from textwrap import wrap
from dash import Dash, html, dcc, Input, Output, callback
from dash.dependencies import Input, Output
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt
import numpy as np
from dash.exceptions import PreventUpdate
import seaborn as sns 


# Create a MySQL connection
host = '10.0.0.81'
user = 'crs'
password = 'Omnibis.1234'
database = 'claro'
table_name = 'sankey_bycdr'
conn = mysql.connector.connect(host=host, user=user, password=password, database=database)


# Create a pandas DataFrame by reading the MySQL table
query = f"SELECT * FROM {table_name}"
file = pd.read_sql(query, conn)
conn.close()

unique_source_target = list(set(file['src'].tolist() + file['dest'].tolist()))
mapping_dict = {k: v for v, k in enumerate(unique_source_target)}
file.replace(mapping_dict, inplace=True)
links_dict = file.to_dict(orient="list")

node_palette = sns.color_palette("Set3", n_colors=len(unique_source_target))
node_colors = [f'rgb({int(color[0]*255)}, {int(color[1]*255)}, {int(color[2]*255)})' for color in node_palette]



# Initialize Dash app
app = Dash(__name__)

# Define layout of the app
app.layout = html.Div([
    dcc.Graph(id='sankey-chart', style={'height': '110vh'}),  # Set height to 100% of the viewport height
    dcc.Interval(
        id='interval-component',    
        interval=10 * 1000,  # in milliseconds
        n_intervals=0
    )
], 
style={'height': '110vh', 'margin': '0', 'overflow': 'hidden'})  # Set height to 100% of the viewport height

# Define callback to update the Sankey chart
@app.callback(
    Output('sankey-chart', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_sankey_chart(n):
    # Create a MySQL connection
    conn = mysql.connector.connect(host=host, user=user, password=password, database=database)

    # Create a pandas DataFrame by reading the MySQL table
    query = f"SELECT * FROM {table_name}"
    file = pd.read_sql(query, conn)
    conn.close()

    unique_source_target = list(set(file['src'].tolist() + file['dest'].tolist()))
    mapping_dict = {k: v for v, k in enumerate(unique_source_target)}
    file.replace(mapping_dict, inplace=True)
    links_dict = file.to_dict(orient="list")

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            label=unique_source_target,
            color=node_colors,
        ),
        link=dict(
            source=links_dict["src"],
            target=links_dict["dest"],
            value=links_dict["count"],
            color=['#045F5F' if value == 1 else 'rgba(30,30,30,0.2)' for value in links_dict["repetitive"]]
        )
    )])

    fig.update_layout(title_text="A",
                      font_size=10,
                      )

    return fig

# Run the app on port 8050
if __name__ == '__main__':
    app.run_server(port=8055, debug=True)
