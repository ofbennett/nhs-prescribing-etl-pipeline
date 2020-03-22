import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import configparser

data_dir = './data_cloud/'
# data_dir = './data_local/'
data_path = data_dir+'0datafile.csv'

colors = {
    'background': 'WhiteSmoke',
    'text': 'black'
}

app = dash.Dash(__name__)

df = pd.read_csv(data_path)

config = configparser.ConfigParser()
config.read('./config.cfg')
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
                html.Div(["Select the type of medication to display:",dropdown],
                style = {'backgroundColor':colors['background'],'width': '25%', 'margin-left': 30}),
                dcc.Graph(figure=fig, id = 'map')], 
                style={'backgroundColor':colors['background']})

mdtext1 = dcc.Markdown(
"""
# What is this?

This is a visualisation of patterns of GP prescribing across England. Different types of medication can be displayed - you can select the type of medication using the dropdown menu in the top left. The "total cost" to the NHS of medication prescribed by a practice within a medication category was used as a summary statistic of the "amount" prescribed.

At the moment this *only* displays the pattern from prescriptions in the month of November 2019. The plan is to extend it to display any pattern from the past 10 years in due course. 

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
Essentially the data is trasformed into a useful schema and loaded into an AWS Redshift data warehouse. Once this has been done it is simple to run any SQL query you like against the tables in Redshift. The visualisation being demonstrated above was created by running a query related to the amount of medication within a certain category being prescribed in all the GP practices across England. The various ETL steps are joined together in a DAG and orchestrated with Apache Airflow.

&nbsp;

# Can I see the code?

Of course! It's kept in my GitHub repo [here](https://github.com/ofbennett/NHS_Prescribing_ETL_Pipeline). The ETL pipeline can be either run locally with a small sample dataset or run using AWS resources with a large or full dataset. **NB: Running the pipeline on your AWS account will cost money!** Feel free to raise issues or contribute.

&nbsp;

# Who am I?

I'm Oscar. I like learning things from data. I'm a data scientist and software engineer with a particular focus on biomedical and healthcare applications. If you're curious you can checkout my [Github](https://github.com/ofbennett) or my [LinkedIn](https://www.linkedin.com/in/oscar-bennett/).

""",
style={'color':colors['text'], 'backgroundColor':colors['background'], 'textAlign':'center', 'margin-left':80, 'margin-right':80, 'padding':10}
)

app.layout = html.Div([graph, mdtext1, diagram, mdtext2], style={'backgroundColor':colors['background']})

@app.callback(
    Output('map','figure'),
    [Input('dropdown', 'value')]
)
def update_map(selected_med):

    med_dict = {'All':0,
                'Antibacterial Drugs':1,
                'Antiprotozoal Drugs':2,
                'Diuretics':3,
                'Beta-Adrenoceptor Blocking Drugs':4,
                'Bronchodilators':5}

    med_num = med_dict[selected_med]
    data_path = data_dir + '{}datafile.csv'.format(med_num)
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

server = app.server # for gunicorn to import 
if __name__ == '__main__':
    app.run_server(debug=True)