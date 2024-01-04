import airplane
import os
import pandas as pd
import paramiko
import json
from datetime import date, datetime
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from zipfile import ZipFile, ZIP_DEFLATED
import io

SERVICE_ACCOUNT = os.getenv('SERVICE_ACCOUNT')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')

@airplane.task(
    slug="sftp_transmit",
    name="SFTP Transmit",
    resources=[
        airplane.Resource("data_airplane"),
     ],
    env_vars=[
        airplane.EnvVar(
            name="SERVICE_ACCOUNT",
            config_var_name="SERVICE_ACCOUNT",
        ),                
        airplane.EnvVar(
            name="USER",
            config_var_name="USER",
        ),
        airplane.EnvVar(
            name="PASSWORD",
            config_var_name="PASSWORD",
        ),
    ],
)

def sftp_transmit(file_names: str):
    date_time_created = str(datetime.now().strftime("%Y.%m.%d.%I.%M"))
    month_year_created = str(datetime.now().strftime("%Y-%m"))
    
    # Setup Google Cloud Storage Credentials 
    credentials_dict = json.loads(SERVICE_ACCOUNT)
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    client = storage.Client(credentials=credentials)
    bucket = client.bucket('bucket')

    # run queries and upload to GCS bucket
    file_name_list = json.loads(file_names)
    for filename in file_name_list:
        run = airplane.sql.query(
            sql_resource="data_airplane",
            query=
                """
                EXPORT DATA OPTIONS(
                    uri='gs://xxx/sftp/{}_{}_*.csv',
                    format='CSV',
                    overwrite=true,
                    header=true,
                    field_delimiter='|'
                ) AS 
                SELECT * FROM {}
                """.format(filename['file_name'], date_time_created, filename['file_name'])
        )
        blobs_to_merge = list(bucket.list_blobs(prefix='sftp/{}_{}'.format(filename['file_name'], date_time_created)))
        destination_blob = bucket.blob('sftp/sftp_reporting_{}_cache_{}.csv'.format(filename['file'], date_time_created))      
        with destination_blob.open("w") as dest_file:
            for blob in blobs_to_merge:
                blob_content = blob.download_as_text()
                dest_file.write(blob_content + '\n')  
                blob.delete()
        airplane.execute("{}_append".format(filename['file_name']))
    airplane.execute("airplane_reconcile")
    
    # Zip files in GCS Verisk folder
    blobs = list(bucket.list_blobs(prefix='sftp/sftp'))
    with io.BytesIO() as zip_buffer:
        with ZipFile(zip_buffer, 'w', compression=ZIP_DEFLATED) as zip:
            for blob in blobs:
                data = blob.download_as_string()
                zip.writestr(blob.name.split('/', 1)[1], data)
                blob.delete()
        bucket.blob('sftp/sftp_{}.zip'.format(date_time_created)).upload_from_string(zip_buffer.getvalue())

    # Connect to Files.com (formerly ExaVault) SFTP and transfer zip file
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname="xxx", port=22, username=USER, password=PASSWORD)
        ftp_client = ssh_client.open_sftp()

        try:
            ftp_client.chdir('/xxx/'+month_year_created+'/')
        except:
            ftp_client.mkdir('/xxx/'+month_year_created+'/')
            ftp_client.chdir('/xxx/'+month_year_created+'/')  
    
        zip_blob = bucket.blob('sftp/sftp_{}.zip'.format(date_time_created))
        file_contents = zip_blob.download_as_string()
        file_name = 'verisk_{}.zip'.format(date_time_created)
        with ftp_client.open(file_name, 'wb') as f:
            f.write(file_contents)

    except paramiko.SSHException as ssh_e:
        print(f"SSH error occurred: {str(ssh_e)}")
        
    except OSError as os_e:
        print(f"OS error occurred: {str(os_e)}")

    finally:
        ftp_client.close()
        ssh_client.close()

    delete_blobs = bucket.list_blobs(prefix='sftp/sftp')
    for blob in delete_blobs:
        blob.delete()
