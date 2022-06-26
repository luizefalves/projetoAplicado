import requests
import json
import pandas as pd
import boto3
import os
import numpy as np
from datetime import datetime
from io import StringIO
import time
import glob


access_key='AKIAYVR3CNZ6SSYYHWMW'
secret_access_key='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'


def s3_upload(path, source):
    print("enviando para a nuvem...")
    t = datetime.now()
    date = t.strftime('%y-%m-%d %Hh:%Mm:%Ss')
    destination_s3_bucket = 'datalake-proaplicado'
    upload_file_key = 'raw-zone/' + '{}-{}'.format(date,source)
    filepath =  upload_file_key + ".csv"

    s3_client = boto3.client('s3',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_access_key,
                             region_name='sa-east-1')


    s3 = boto3.resource('s3',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_access_key,
                             region_name='sa-east-1')
    s3.Bucket(destination_s3_bucket).upload_file(path,upload_file_key)



def vesselCompanies():
    lista = []
    print("Obtendo tabelas...")
    for number in range(100,13900,100):
        passo = pd.read_html('https://nrcwaplan.usecology.com/VesselList/VesselList?index={}'.format(number), attrs = {'class': 'table'}, header=0)
        lista.append(passo)
        time.sleep(1)
    print("Gravando tabelas...")
    for i in range(0,137):
        lista[i][0].to_csv(f'./operadoras/operadoras{i}.csv', sep=';', header=True)

    return
    

def main():
    #vesselCompanies()
    path = "/home/airflow/airflow/dags/operadoras"
    for file in os.listdir(path):
        if file.endswith(".csv"):
            s3_upload(os.path.join(path, file), "operators")
    return print("Data was uploaded")

if __name__ == "__main__":
    main()