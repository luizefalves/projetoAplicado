import requests
import json
import pandas as pd
import boto3
import os
import numpy as np
from datetime import datetime
from io import StringIO
import time

def port_find(port_name):
    params = {
      'api-key': 'a599c1bb-61dc-4a9d-bc58-daf57d6ce8a5',
      'name': str(port_name)
    }
    method = 'port_find'
    api_base = 'https://api.datalastic.com/api/v0/'
    api_result = requests.get(api_base+method, params)
    api_response = api_result.json()
    porto = api_response['data'][0]['uuid']
    return  porto


def vessel_inradius(port_name):
    params = {
      'api-key': 'a599c1bb-61dc-4a9d-bc58-daf57d6ce8a5',
      'port_uuid': port_find(port_name),
        'radius' : '30'
    }
    method = 'vessel_inradius'
    api_base = 'https://api.datalastic.com/api/v0/'
    api_result = requests.get(api_base+method, params)
    api_response = api_result.json()
    vessels = api_response['data']["vessels"]
    df = pd.json_normalize(vessels)
    df = df[(df['type'] == 'Cargo') \
            & (df['destination'] != port_name) \
            & (df['imo'].notnull())
           ]
    t = datetime.now()
    df['request_time'] = t.strftime('%y-%m-%d %Hh:%Mm')
    df['port_name'] = port_name
    return df






def s3_upload(df, source):
    t = datetime.now()
    date = t.strftime('%y-%m-%d %Hh:%Mm')
    destination_s3_bucket = 'datalake-proaplicado'
    upload_file_key = 'raw-zone/' + '{}-{}'.format(date,source)
    filepath =  upload_file_key + ".csv"

    s3_client = boto3.client('s3',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_access_key,
                             region_name='sa-east-1')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    csv_buffer.seek(0)
    response = s3_client.put_object(
        Bucket=destination_s3_bucket,
        Key=filepath,
        Body=csv_buffer.getvalue()
    )

access_key='AKIAYVR3CNZ6SSYYHWMW'
secret_access_key='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'


def main():
  port_names = ["Rotterdam","Antwerp","Hamburg","Valencia","Barcelona","London","Mersin","Izmit",\
                "Piraeus","Alexandria", "Jebel Ali","Cape Town"]
  appended = []
  for port_name in port_names:
      total = vessel_inradius(port_name)
      appended.append(total)

  appended_data = pd.concat(appended)
  s3_upload(appended_data, "vessels")


  return print("Data was Extracted")

if __name__ == "__main__":
    main()