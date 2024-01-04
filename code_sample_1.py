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

DATA_AIRPLANE_SERVICE_ACCOUNT = os.getenv('DATA_AIRPLANE_SERVICE_ACCOUNT')
SFTP_GDAY_USER = os.getenv('SFTP_GDAY_USER')
SFTP_GDAY_PASSWORD = os.getenv('SFTP_GDAY_PASSWORD')
KEY_PATH = os.getenv('KEY_PATH')
SFTP_VERISK_USER = os.getenv('SFTP_VERISK_USER')
SFTP_VERISK_PASSWORD = os.getenv('SFTP_VERISK_PASSWORD')

@airplane.task(
    slug="verisk_sftp_transmit",
    name="Verisk SFTP Transmit",
    resources=[
        airplane.Resource("data_airplane"),
     ],
    env_vars=[
        airplane.EnvVar(
            name="DATA_AIRPLANE_SERVICE_ACCOUNT",
            config_var_name="DATA_AIRPLANE_SERVICE_ACCOUNT",
        ),                
        airplane.EnvVar(
            name="SFTP_GDAY_USER",
            config_var_name="SFTP_GDAY_USER",
        ),
        airplane.EnvVar(
            name="SFTP_GDAY_PASSWORD",
            config_var_name="SFTP_GDAY_PASSWORD",
        ),
        airplane.EnvVar(
            name="KEY_PATH",
            config_var_name="KEY_PATH",
        ),
        airplane.EnvVar(
            name="SFTP_VERISK_USER",
            config_var_name="SFTP_VERISK_USER",
        ),
        airplane.EnvVar(
            name="SFTP_VERISK_PASSWORD",
            config_var_name="SFTP_VERISK_PASSWORD",
        ),
    ],
)

def verisk_sftp_transmit(file_names: str):
    date_time_created = str(datetime.now().strftime("%Y.%m.%d.%I.%M"))
    month_year_created = str(datetime.now().strftime("%Y-%m"))
    
    # Setup Google Cloud Storage Credentials 
    credentials_dict = json.loads(DATA_AIRPLANE_SERVICE_ACCOUNT)
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    client = storage.Client(credentials=credentials)
    bucket = client.bucket('branch-data-office')

    # run queries and upload to GCS bucket
    file_name_list = json.loads(file_names)
    for filename in file_name_list:
        run = airplane.sql.query(
            sql_resource="data_airplane",
            query=
                """
                EXPORT DATA OPTIONS(
                    uri='gs://branch-data-office/Verisk/{}_{}_*.csv',
                    format='CSV',
                    overwrite=true,
                    header=true,
                    field_delimiter='|'
                ) AS 
                SELECT * FROM {}
                """.format(filename['file_name'], date_time_created, filename['file_name'])
        )
        blobs_to_merge = list(bucket.list_blobs(prefix='Verisk/{}_{}'.format(filename['file_name'], date_time_created)))
        destination_blob = bucket.blob('Verisk/verisk_reporting_{}_cache_{}.csv'.format(filename['file'], date_time_created))      
        with destination_blob.open("w") as dest_file:
            for blob in blobs_to_merge:
                blob_content = blob.download_as_text()
                dest_file.write(blob_content + '\n')  
                blob.delete()
        airplane.execute("{}_append".format(filename['file_name']))
    airplane.execute("verisk_reporting_master_bordereaux_airplane_append")
    
    # Zip files in GCS Verisk folder
    blobs = list(bucket.list_blobs(prefix='Verisk/verisk'))
    with io.BytesIO() as zip_buffer:
        with ZipFile(zip_buffer, 'w', compression=ZIP_DEFLATED) as zip:
            for blob in blobs:
                data = blob.download_as_string()
                zip.writestr(blob.name.split('/', 1)[1], data)
                blob.delete()
        bucket.blob('Verisk/verisk_{}.zip'.format(date_time_created)).upload_from_string(zip_buffer.getvalue())

    # Connect to Files.com (formerly ExaVault) SFTP and transfer zip file
    try:
        exa_vault_ssh_client = paramiko.SSHClient()
        exa_vault_ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        exa_vault_ssh_client.connect(hostname="ourbranch.exavault.com", port=22, username=SFTP_GDAY_USER, password=SFTP_GDAY_PASSWORD)
        exa_vault_ftp_client = exa_vault_ssh_client.open_sftp()

        try:
            exa_vault_ftp_client.chdir('/verisk-PDP/'+month_year_created+'/')
        except:
            exa_vault_ftp_client.mkdir('/verisk-PDP/'+month_year_created+'/')
            exa_vault_ftp_client.chdir('/verisk-PDP/'+month_year_created+'/')  
    
        zip_blob = bucket.blob('Verisk/verisk_{}.zip'.format(date_time_created))
        file_contents = zip_blob.download_as_string()
        exa_vault_file_name = 'verisk_{}.zip'.format(date_time_created)
        with exa_vault_ftp_client.open(exa_vault_file_name, 'wb') as f:
            f.write(file_contents)

    except paramiko.SSHException as ssh_e:
        print(f"SSH error occurred: {str(ssh_e)}")
        
    except OSError as os_e:
        print(f"OS error occurred: {str(os_e)}")

    finally:
        exa_vault_ftp_client.close()
        exa_vault_ssh_client.close()

    delete_blobs = bucket.list_blobs(prefix='Verisk/verisk')
    for blob in delete_blobs:
        blob.delete()
