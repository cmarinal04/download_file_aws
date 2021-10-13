import pandas as pd
from connection import sql_server_connection as con_sql
from boto3.session import Session
import boto3
from _class.config import config_session
from datetime import date
from datetime import datetime
from datetime import timedelta
from os import remove
from os import path
import locale # Idioma "es-CO" (código para el español de Colombia) 
locale.setlocale(locale.LC_ALL, 'es-CO')

def download_file():
    
    ACCESS_KEY = config_session()[0]
    SECRET_KEY = config_session()[1]

    session = Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    s3 = session.resource('s3')
    today = date.today()
    yesterday = today - timedelta(days=1)
    date_1 = date.strftime(yesterday, '%Y%m%d')
    year = date.strftime(yesterday, '%Y')
    month = date.strftime(yesterday, '%m')
    file_name = (f'MTT_Pagos{date_1}.csv') #Acá, agregar ruta donde descargar los archivos.
    your_bucket = s3.Bucket(f'paymentgateway-billingsystem-prod')
    file_local = f'./Soporte/{file_name}'
    your_bucket.download_file(f'CO/outbox/{year}/{month}/{file_name}', file_local)

    load(file_local)
    delete_file(file_local)


    
    # /CO/outbox/{year}/{month}/
    # for s3_file in your_bucket.objects.all():
    #     print(s3_file.key) # prints the contents of bucket
    # for your_bucket in s3.buckets.all():
    #         print(your_bucket.name)
    # s3 = boto3.client ('s3')
    # s3.download_file('your_bucket','k.png','/Users/username/Desktop/k.png')



def read(filename):
     df = pd.read_csv(f'{filename}', dtype=str)
     dict_columns = {
         'Application/Billing System Name': 'Application Billing System Name',
         'Nombre Columna en Pandas': 'Nombre en la tabla sql'
     }
     df.rename(columns={'Application/Billing System Name': 'Application Billing System Name'}, inplace=True)
     df.fillna('', inplace=True)
     # breakpoint()
     return df #.iloc[:10]


def load(filename):
     df_read = read(filename)
     records = df_read.values.tolist()
     columns = [f'[{col}]' for col in df_read.columns.tolist()]
     sql_insert = f'''INSERT INTO [dbo].[MTT_Pagos_Prueba_Test] ({', '.join(columns)})
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
     '''

   
     connection = con_sql.connection()
     cursor = connection.cursor()
     cursor.executemany(sql_insert,records)
     connection.commit()

def delete_file(filename):
    if path.exists(filename):
         remove(filename)
    print(f'Archivo eliminado')

def main():
    download_file()


if __name__ == "__main__":
    main()

