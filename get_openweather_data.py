#
# Author: Team Mojo
# Course: DWL03-Dat Lakes and Data Warehouses
# Data: Autumn 2021
#
# Description: AWS Lambda function to GET Open weather map API data and POST to S3 bucket


import json
import urllib3
from datetime import datetime
from ast import literal_eval

url = "https://api.openweathermap.org/data/2.5/onecall?lat=47.405812&lon=9.437534&exclude={part}&appid=beffc764a5c033472881cb4b86b79d4b"
http = urllib3.PoolManager()
r = http.request('GET', url)

data = literal_eval(r.data.decode('utf8'))

bucket='dwl03-bucket'

date_time = datetime.now().strftime("%Y%m%d_%H:%M:%S")

filename = 'openweathermap_data/' + date_time + '.json'

uploadByteStream = bytes(json.dumps(data).encode('UTF-8'))