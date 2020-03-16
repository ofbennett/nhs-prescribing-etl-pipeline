import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import numpy as np
import configparser

data_path = './data/0datafile.csv'
colors = {
    'background': '#111111',
    'text': 'lightgrey'
}

app = dash.Dash(__name__)

df = pd.read_csv(data_path)

config = configparser.ConfigParser()
config.read('../config.cfg')
px.set_mapbox_access_token(config['MAPBOX']['MAPBOX_TOKEN'])

fig = px.scatter_mapbox(df, 
                        lat="latitude", 
                        lon="longitude", 
                        color="total_cost", 
                        size="total_cost", 
                        hover_name="name", 
                        hover_data=["total_cost"], 
                        color_continuous_scale=px.colors.sequential.Pinkyl, 
                        size_max=15, 
                        zoom=5.5,
                        center={'lat': 52.58,'lon': -1.117},
                        mapbox_style='dark')

fig.update_layout(autosize=True,
                height=640,
                paper_bgcolor=colors['background'],
                plot_bgcolor=colors['background'],
                margin=dict(l=100, r=100, t=35, b=30))

dropdown = dcc.Dropdown(
    id = 'dropdown',
    value = 'Antibacterial Drugs',
    options = [
        {'label': 'Antibiotics', 'value': 'Antibacterial Drugs'},
        {'label': 'Antiprotozoal Drugs', 'value': 'Antiprotozoal Drugs'},
        {'label': 'Diuretics', 'value': 'Diuretics'},
        {'label': 'Beta Blockers', 'value': 'Beta-Adrenoceptor Blocking Drugs'},
        {'label': 'Bronchodilators', 'value': 'Bronchodilators'},
    ],
    style = {'backgroundColor':'darkgrey', 'color':'black', 'width': '100%'}
)

graph = html.Div(children=[
                html.H1(children='Visualization of Prescribing Patterns across NHS GP Practices', 
                    style={'textAlign':'center', 
                        'color':colors['text'], 
                        'margin':0, 
                        'padding':10}),
                html.Div(children="Built with Plotly Dash, Flask, and Mapbox", 
                    style={'textAlign':'center', 
                        'color':colors['text']}),
                html.Div([dropdown],
                style = {'backgroundColor':colors['background'],'width': '25%', 'margin-left': 20}),
                dcc.Graph(figure=fig, id = 'map')], 
                style={'backgroundColor':colors['background']})

app.layout = html.Div([graph], style={'backgroundColor':colors['background']})

@app.callback(
    Output('map','figure'),
    [Input('dropdown', 'value')]
)
def update_map(selected_med):

    med_dict = {'Antibacterial Drugs':0,
                'Antiprotozoal Drugs':1,
                'Diuretics':2, 
                'Beta-Adrenoceptor Blocking Drugs':3,
                'Bronchodilators':4}

    med_num = med_dict[selected_med]
    data_path = './data/{}datafile.csv'.format(med_num)
    df = pd.read_csv(data_path)
    max_val = df['total_cost'].max()

    fig = px.scatter_mapbox(df, 
                        lat="latitude", 
                        lon="longitude", 
                        color="total_cost", 
                        size="total_cost", 
                        hover_name="name", 
                        hover_data=["total_cost"], 
                        color_continuous_scale=px.colors.sequential.Pinkyl,
                        range_color=[0,max_val], 
                        size_max=15, 
                        zoom=5.5,
                        mapbox_style='dark')

    fig.update_layout(autosize=True,
                    height=640,
                    paper_bgcolor=colors['background'],
                    plot_bgcolor=colors['background'],
                    margin=dict(l=100, r=100, t=35, b=30),
                    )
    return fig

if __name__ == '__main__':
    app.run_server(debug=False)