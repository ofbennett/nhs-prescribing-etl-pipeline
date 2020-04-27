import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import numpy as np
import configparser
import json

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

app = dash.Dash(__name__)

df = pd.read_csv(data_path)
df['name'] = df['name'].map(lambda x: x.title())
max_val = np.sort(df['total_cost'].values)[-3] # picks the 3rd highest for top of range

px.set_mapbox_access_token(config['MAPBOX']['MAPBOX_TOKEN'])

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
                        center={'lat': 52.58,'lon': -1.117},
                        mapbox_style='dark')

fig.update_layout(autosize=True,
                height=640,
                paper_bgcolor=colors['background'],
                plot_bgcolor=colors['background'],
                margin=dict(l=150, r=150, t=25, b=0))

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
    style = {'backgroundColor':'LightGray', 'color':'black', 'width': '100%', 'margin-top':5}
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
                html.Div(children="A visualisation of publicly available GP prescription records from NHS digital", 
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
                dcc.Graph(figure=fig, id = 'map')], 
                style={'backgroundColor':colors['background']})

df_sorted = df.sort_values(by=['total_cost'], ascending=False)
barchart = dcc.Graph(
        id='bar-chart',
        figure={
            'data': [
                {'x': df_sorted['name'][:num_of_bars], 'y': df_sorted['total_cost'][:num_of_bars], 'type': 'bar', 'name': 'SF'},
            ],
            'layout': {
                'plot_bgcolor': colors['background'],
                'paper_bgcolor': colors['background'],
                'margin': dict(l=150, r=150, t=0, b=0),
                'font': {
                    'color':colors['text']
                }
            }
        }
    )

ts_data_path = './data_cloud/timeseries.json'
with open(ts_data_path) as f:
    ts_data_json = json.load(f)

ts_data = ts_data_json['All']['2019']
months = ["Jan", "Feb", "Mar", "Apr", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"]

ts_graph = dcc.Graph(
    figure = {
            'data': [
                        {
                            'x': months,
                            'y': ts_data,
                            'name': 'Medication',
                            'marker': {'color': 'rgb(55, 83, 109)'}
                        }
            ],
            'layout': {
                'title': 'Change in amount of All prescribing across all practices through 2019',
                'plot_bgcolor': colors['background'],
                'paper_bgcolor': colors['background'],
                'margin': dict(l=150, r=150, t=40, b=30),
                'font': {'color': colors['text']}
            }
    },
    style={'height': 300},
    id='ts-graph'
)


mdtext1 = dcc.Markdown(
"""
# What is this?

This is a visualisation of patterns of GP prescribing across England. Different types of medication can be displayed - you can select the type of medication using the dropdown menu in the top left. The "total cost" to the NHS (in GBP) of medication prescribed by a practice within a medication category was used as a summary statistic of the "amount" prescribed.

At the moment this *only* displays the pattern from prescriptions in 2019. The plan is to extend it to display any pattern from the past 10 years in due course. 

&nbsp;

# Where did the data come from?

The majority of the data comes from a large volume (~100GB) of anonymised GP prescription records which have been released publicly by [NHS digital](https://digital.nhs.uk). The data can be downloaded from their website [here](https://digital.nhs.uk/data-and-information/publications/statistical/practice-level-prescribing-data) and a detailed description of what the data contains can be found [here](https://digital.nhs.uk/data-and-information/areas-of-interest/prescribing/practice-level-prescribing-in-england-a-summary/practice-level-prescribing-data-more-information). This public sector information is published and made available under the [Open Government Licence v3.0](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

The prescribed medication in this dataset is referenced with a code used by the British National Formulary (BNF). An extra dataset (~19MB) providing more information and categorisation of each of these medications from the BNF was downloaded from the NHS Business Services Authority website ([here](https://apps.nhsbsa.nhs.uk/infosystems/data/showDataSelector.do?reportId=126)).

Finally, a free and open source API called [Postcodes.io](https://postcodes.io) was used to obtain latitude and longitude coordinates (as well as other location metadata) of the GP practices in the dataset to make plotting them easier.

&nbsp;

# How was it made?

 This web app was built using **Flask**, **Plotly Dash** and **Mapbox** and is currently hosted on DigitalOcean. There is a very large volume of data to process in order to generate these visualisations. The data pipeline therefore presented an interesting engineering problem. The partially cloud-based architecture I settled on is outlined in the schematic below. The ETL data pipeline was built using **Postgres**, **Apache Airflow**, **AWS Redshift**, and **S3**.
 """,
 style={'color':colors['text'], 'backgroundColor':colors['background'], 'textAlign':'center', 'margin-left':80, 'margin-right':80, 'padding':10})

diagram = html.Img(src="assets/diagram.png",style = {"width": "65%", "display": "block" , "margin-left": "auto", "margin-right": "auto"})

mdtext2 = dcc.Markdown("""
Essentially the data is transformed into a useful schema and loaded into an AWS Redshift data warehouse. Once this has been done it is simple to run any SQL query you like against the tables in Redshift. The visualisations being demonstrated above were created by running and caching queries related to the amount of medication within a certain category being prescribed in GP practices across England. The various ETL steps are joined together in a DAG and orchestrated with Apache Airflow.

&nbsp;

# Can I see the code?

Of course! It's kept in my GitHub repo [here](https://github.com/ofbennett/NHS_Prescribing_ETL_Pipeline). The ETL pipeline can be either run locally with a small sample dataset or run using AWS resources with a large or full dataset. **NB: Running the pipeline on your AWS account will cost money!** Feel free to raise issues or contribute.

&nbsp;

# Who am I?

I'm Oscar. I like learning things from data. I'm a data scientist and software engineer with a particular focus on biomedical and healthcare applications. If you're curious you can checkout my [Github](https://github.com/ofbennett) or my [LinkedIn](https://www.linkedin.com/in/oscar-bennett/).

""",
style={'color':colors['text'], 'backgroundColor':colors['background'], 'textAlign':'center', 'margin-left':80, 'margin-right':80, 'padding':10}
)

app.layout = html.Div([graph, barchart, ts_graph, mdtext1, diagram, mdtext2], style={'backgroundColor':colors['background']})

@app.callback(
    [Output('map','figure'), Output('bar-chart','figure'), Output('map-title','children'), Output('ts-graph','figure')],
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

    
    return fig_map, fig_barchart, map_title, fig_ts_graph

server = app.server # for gunicorn to import 
if __name__ == '__main__':
    app.run_server(debug=True)