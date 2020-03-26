import requests
import pandas as pd
import json
import time
from tqdm import tqdm
import numpy as np

"""
This script takes a GP practices info CSV file, extracts the postcodes of all the practices and then obtains metadata about each postcode via an API. Metadata includes lat and long coordinates. 
"""

api_url = "https://api.postcodes.io/postcodes"
data_path = "../../data/2019_11_Nov/T201911ADDR BNFT.csv"
out_json_file_path = './postcode_info.json'
out_csv_file_path = './postcode_info.csv'

col_names = ['time_period','gp_prac_id','addr1','addr2','addr3','addr4','addr5','postcode']
df = pd.read_csv(data_path, names=col_names)
postcode_np = df['postcode'].dropna().unique()

postcode_info = []

step_size = 100 # API only allows 100 postcode queries in a single POST request
for i in tqdm(range(0, len(postcode_np), step_size)):
    front = i
    if i + step_size > len(postcode_np):
        back = len(postcode_np)
    else:
        back = i + step_size
    postcodes = postcode_np[front:back].tolist()
    data = {"postcodes" : postcodes}
    r = requests.post(api_url, data = data)
    if r.status_code != 200:
        print('api responded with code {}'.format(r.status_code),'... Skipping')
        continue
    postcode_info += r.json()['result']
    time.sleep(1) # Don't overload API

postcode_ls = postcode_np.tolist()

# Remove None responses
postcode_info_cleaned = []
n=0
for i in range(len(postcode_info)):
    if postcode_info[i]['result'] != None:
        postcode_info_cleaned.append(postcode_info[i])
    else:
        n+=1
        continue

print("Removed {} None responses".format(n))

json_data = {'all_postcodes_attempted': postcode_ls, 'postcode_info': postcode_info_cleaned}

print("Final postcode info list has {} items in it".format(len(postcode_info_cleaned)))
print("Saving postcode info to json file {}".format(out_json_file_path))

# Save json file
with open(out_json_file_path, 'w') as f:
    json.dump(json_data, f)

# Create pandas dataframe from desired postcode info and save to csv file
df_clean_csv = pd.DataFrame(index=np.arange(0, len(postcode_info_cleaned)), columns=('postcode', 'county', 'region', 'longitude', 'latitude'))
for i in np.arange(0, len(postcode_info_cleaned)):
    jrow = postcode_info_cleaned[i]['result']
    df_clean_csv.loc[i] = [jrow['postcode'], jrow['admin_county'], jrow['region'], jrow['longitude'], jrow['latitude']]

print("Saving postcode info to csv file {}".format(out_csv_file_path))
df_clean_csv.to_csv(out_csv_file_path,index=False)