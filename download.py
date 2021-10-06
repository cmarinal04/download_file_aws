
from boto3.session import Session
import boto3
from _class.config import config_session
from datetime import date
from datetime import datetime
from datetime import timedelta
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
    file_name = f'MTT_Pagos{date_1}.csv'

    your_bucket = s3.Bucket(f'paymentgateway-billingsystem-prod')
    your_bucket.download_file(f'CO/outbox/{year}/{month}/{file_name}',file_name)
    # /CO/outbox/{year}/{month}/

    # for s3_file in your_bucket.objects.all():
    #     print(s3_file.key) # prints the contents of bucket

    for your_bucket in s3.buckets.all():
            print(your_bucket.name)


    # s3 = boto3.client ('s3')

    # s3.download_file('your_bucket','k.png','/Users/username/Desktop/k.png')



def main():
    download_file()


if __name__ == "__main__":
    main()

