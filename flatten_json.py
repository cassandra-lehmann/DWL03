import boto3
import json
import datetime
import os
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')

session = boto3.Session()
s3 = session.resource('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)

my_bucket = s3.Bucket('dwl03-bucket')

data_timestamp = {}


def lambda_handler(event, context):
    for objects in my_bucket.objects.filter(Prefix="openweathermap_data/"):

        temp = objects.key.split("/")[1]
        if temp != '':
            date = temp.split(".json")[0]
            a_string = date[:4] + "-"
            a_string = date[:4] + "-" + date[4:6] + "-" + date[6:8]

            time = date.split("_")[1]
            time = time.split(":")
            try:
                x = datetime.datetime(int(date[:4]), int(date[4:6]), int(date[6:8]), int(time[0]), int(time[1]),
                                      int(time[2]))
                temp_dict = {temp: x}
                data_timestamp.update(temp_dict)
            except:
                None

    key_max = max(data_timestamp.keys(), key=(lambda k: data_timestamp[k]))
    key = 'openweathermap_data/' + key_max

    s3 = boto3.client('s3')

    obj = s3.get_object(Bucket='dwl03-bucket', Key=key)
    a = json.loads(obj['Body'].read())

    clean = {"lat": a["lat"],
             "lon": a["lon"],
             'timezone': a['timezone'],
             'timezone_offset': a['timezone_offset'],
             'current_dt': a['current']['dt'],
             'current_sunrise': a['current']['sunrise'],
             'current_sunset': a['current']['sunset'],
             'current_temp': a['current']['temp'],
             'current_feels_like': a['current']['feels_like'],
             'current_pressure': a['current']['pressure'],
             'current_humidity': a['current']['humidity'],
             'current_dew_point': a['current']['dew_point'],
             'current_uvi': a['current']['uvi'],
             'current_clouds': a['current']['clouds'],
             'current_visibility': a['current']['visibility'],
             'current_wind_speed': a['current']['wind_speed'],
             'current_wind_deg': a['current']['wind_deg'],
             'current_weather_0_id': a['current']['weather'][0]['id'],
             'current_weather_0_main': a['current']['weather'][0]['description'],
             'current_weather_0_description': a['current']['weather'][0]['main'],
             'current_weather_0_icon': a['current']['weather'][0]['icon']
             }

    filename = 'flattened_json/' + key_max.split(".json")[0] + '.json'
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    uploadByteStream = bytes(json.dumps(clean).encode('UTF-8'))
    s3_client.put_object(Bucket='dwl03-bucket', Key=filename, Body=uploadByteStream)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "Filename ": filename
        })
    }