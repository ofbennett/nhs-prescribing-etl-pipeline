import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import numpy as np
import configparser
import json
from markdown.markdown import md1, md2

config = configparser.ConfigParser()
config.read('./config.cfg')

mode = config['MODE']['MODE']

if mode == 'local':
    data_dir = './data_local/'
elif mode == 'cloud':
    data_dir = './data_cloud/'

data_path = data_dir+'2019/12/0datafile.csv'

num_of_bars = 30

colors = {
    'background': 'WhiteSmoke',
    'text': 'black'
}

# Use a Bootstrap external style sheet like this:
# external_stylesheets = [
# "https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css",
# "assets/my_style.css"
# ]
# app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app = dash.Dash(__name__)

df = pd.read_csv(data_path)
df['name'] = df['name'].map(lambda x: x.title())
max_val = np.sort(df['total_cost'].values)[-3] # picks the 3rd highest for top of range

px.set_mapbox_access_token(config['MAPBOX']['MAPBOX_TOKEN'])

dropdown_med = dcc.Dropdown(
    id = 'dropdown-med',
    value = 'All',
    options = [
        {'label': 'All', 'value': 'All'},
        {'label': 'Antibiotics', 'value': 'Antibacterial Drugs'},
        {'label': 'Antiprotozoal Drugs', 'value': 'Antiprotozoal Drugs'},
        {'label': 'Diuretics', 'value': 'Diuretics'},
        {'label': 'Beta Blockers', 'value': 'Beta-Adrenoceptor Blocking Drugs'},
        {'label': 'Bronchodilators', 'value': 'Bronchodilators'},
    ],
    style = {'backgroundColor':'LightGray', 'color':'black', 'width': '100%', 'margin-top':5, 'margin-bottom':5}
)

if mode == 'local':
    dates = [{'label': 'December 2019', 'value': '2019_12'}]
elif mode == 'cloud':
    dates = [
            {'label': 'December 2019', 'value': '2019_12'},
            {'label': 'November 2019', 'value': '2019_11'},
            {'label': 'October 2019', 'value': '2019_10'},
            {'label': 'September 2019', 'value': '2019_09'},
            {'label': 'August 2019', 'value': '2019_08'},
            {'label': 'July 2019', 'value': '2019_07'},
            {'label': 'June 2019', 'value': '2019_06'},
            {'label': 'May 2019', 'value': '2019_05'},
            {'label': 'April 2019', 'value': '2019_04'},
            {'label': 'March 2019', 'value': '2019_03'},
            {'label': 'February 2019', 'value': '2019_02'},
            {'label': 'January 2019', 'value': '2019_01'},
            {'label': 'Whole of 2019', 'value': '2019_All'},
        ]


dropdown_date = dcc.Dropdown(
    id = 'dropdown-date',
    value = '2019_12',
    options = dates,
    style = {'backgroundColor':'LightGray', 'color':'black', 'width': '100%', 'margin-top':5}
)

graph = html.Div(children=[
                html.H1(children='Medical Prescribing Patterns Across England NHS GP Practices', 
                    style={'textAlign':'center', 
                        'color':colors['text'], 
                        'margin':0, 
                        'padding':20,
                        'text-decoration': 'underline'}),
                html.Div(children="A visualisation of publicly available GP prescription statistics from NHS digital", 
                    style={'textAlign':'center', 
                        'color':colors['text'],
                        'padding':5}),
                html.Div(children="Oscar Bennett, March 2020", 
                    style={'textAlign':'right', 
                        'color':colors['text'],
                        'padding':0,
                        'margin-right':40}),
                html.Div(["Select the type of medication to display:",dropdown_med,"Select the time period to display:",dropdown_date],
                style = {'backgroundColor':colors['background'],'width': '25%', 'margin-left': 30}),
                html.Div(children="Display of Total Spent per Practice on All Prescribing in December 2019", id = 'map-title', 
                    style={'textAlign':'center', 
                        'color':colors['text'],
                        'margin-top': 10,
                        'font-size': '20px'}),
                html.Div(style={'backgroundColor':colors['background']}, id = 'map')])

df_sorted = df.sort_values(by=['total_cost'], ascending=False)
barchart = dcc.Graph(
        id='bar-chart',
        figure={}
    )

ts_data_path = './data_cloud/timeseries.json'
with open(ts_data_path) as f:
    ts_data_json = json.load(f)

ts_data = ts_data_json['All']['2019']
months = ["Jan", "Feb", "Mar", "Apr", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"]

ts_graph = dcc.Graph(
    figure = {},
    style={'height': 300},
    id='ts-graph'
)


mdtext1 = dcc.Markdown(md1, style={'color':colors['text'], 'backgroundColor':colors['background'], 'textAlign':'center', 'margin-left':100, 'margin-right':100, 'margin-top':40, 'padding':10})

diagram = html.Img(src="assets/diagram.png",style = {"width": "65%", "display": "block" , "margin-left": "auto", "margin-right": "auto"})

mdtext2 = dcc.Markdown(md2, style={'color':colors['text'], 'backgroundColor':colors['background'], 'textAlign':'center', 'margin-left':100, 'margin-right':100, 'padding':10})

app.layout = html.Div([graph, barchart, ts_graph, mdtext1, diagram, mdtext2], style={'backgroundColor':colors['background']})

@app.callback(
    [Output('map','children'), Output('bar-chart','figure'), Output('map-title','children'), Output('ts-graph','figure')],
    [Input('dropdown-med', 'value'), Input('dropdown-date', 'value'), Input('dropdown-date', 'options')]
)
def update_charts(selected_med,selected_date,dropdown_date_options):

    med_dict = {'All':0,
                'Antibacterial Drugs':1,
                'Antiprotozoal Drugs':2,
                'Diuretics':3,
                'Beta-Adrenoceptor Blocking Drugs':4,
                'Bronchodilators':5}
    label_list = ['All', 'Antibiotics', 'Antiprotozoal Drugs', 'Diuretics', 'Beta Blockers', 'Bronchodilators']
    med_num = med_dict[selected_med]
    selected_med_label = label_list[med_num]

    for i in range(len(dropdown_date_options)):
        if dropdown_date_options[i]['value'] == selected_date:
            date_label = dropdown_date_options[i]['label']
            break
    
    data_path = data_dir + '{year}/{month}/{med_num}datafile.csv'.format(year=selected_date[:4],month=selected_date[5:],med_num=med_num)
    df = pd.read_csv(data_path)
    df['name'] = df['name'].map(lambda x: x.title())

    if selected_date[5:] == 'All':
        max_val = np.sort(df['total_cost'].values)[-3] # picks the 3rd highest for top of range
    else:
        data_path_ref = data_dir + "2019/12/{med_num}datafile.csv".format(med_num=med_num)
        df_ref = pd.read_csv(data_path_ref)
        max_val = np.sort(df_ref['total_cost'].values)[-3] # picks the 3rd highest for top of range

    fig_map = px.scatter_mapbox(df,
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

    fig_map.update_layout(autosize=True,
                    height=640,
                    paper_bgcolor=colors['background'],
                    plot_bgcolor=colors['background'],
                    margin=dict(l=150, r=150, t=25, b=10),
                    )
    map_graph = dcc.Graph(figure=fig_map),
    df_sorted = df.sort_values(by=['total_cost'], ascending=False)
    fig_barchart={
            'data': [
                {'x': df_sorted['name'][:num_of_bars], 'y': df_sorted['total_cost'][:num_of_bars], 'type': 'bar', 'name': 'SF'},
            ],
            'layout': {
                'title': 'Top {} practices when looking at {} prescribing in {}'.format(num_of_bars,selected_med_label.lower(),date_label),
                'plot_bgcolor': colors['background'],
                'paper_bgcolor': colors['background'],
                'margin': dict(l=150, r=150, t=50, b=150),
                'font': {
                    'color':colors['text']
                }
            }
        }
    map_title = "Display of Total Spent per Practice on {} Prescribing in {}".format(selected_med_label,date_label)

    ts_data_path = './data_cloud/timeseries.json'
    with open(ts_data_path) as f:
        ts_data_json = json.load(f)
    ts_data = ts_data_json[selected_med]['2019']
    months = ["Jan", "Feb", "Mar", "Apr", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"]

    fig_ts_graph = {
                    'data': [
                                {
                                    'x': months,
                                    'y': ts_data,
                                    'name': 'Medication',
                                    'marker': {'color': 'rgb(55, 83, 109)'}
                                }
                    ],
                    'layout': {
                        'title': 'Change in amount of {} prescribing across all practices through 2019'.format(selected_med_label),
                        'plot_bgcolor': colors['background'],
                        'paper_bgcolor': colors['background'],
                        'margin': dict(l=150, r=150, t=40, b=30),
                        'font': {'color': colors['text']}
                    }
    }

    
    return map_graph, fig_barchart, map_title, fig_ts_graph

server = app.server # for gunicorn to import 
if __name__ == '__main__':
    app.run_server(debug=False)