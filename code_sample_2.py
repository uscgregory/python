# Import Libraries
from google.cloud import bigquery 
from google.oauth2 import service_account
import os
import dotenv
import pandas as pd
import json
from datetime import date, datetime
import time
import xmltodict
from dicttoxml import dicttoxml
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor

start_time = time.time()
# Establish Environment Variables
dotenv.load_dotenv('C:\\Users\\xxx\\xxx\\.env')
API_KEY = os.environ['API_KEY']
API_ID  = os.environ['API_ID']
KEY_PATH = os.environ['KEY_PATH']

# Establish credentials for Big Query
credentials = service_account.Credentials.from_service_account_file(KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"])

# SQL:  Curate query in BQ
sql =   """
        SELECT DISTINCT
            *
        FROM `project.dataset.table`
        """

# Run SQL
query_start_time = time.time()

client = bigquery.Client(credentials=credentials, project=credentials.project_id)
query_res = client.query(sql)
df = query_res.to_dataframe()

query_end_time = time.time()
print("Query Finished: ",'{:.2f}'.format(query_end_time - query_start_time),"seconds.")

#   API URL
url = 'https://xxx/xxx/xxx/{}?parameter={}&endpoint'


# Semaphore is a synchronization primative that can limit access to shared resources.  
# In this case, the Semaphore will limit us to 10 concurrent API calls, per xxx's Directions.
api_call_start_time = time.time()

semaphore = asyncio.Semaphore(10)
results = []

# This function is used to create a list of tasks to be executed concurrently. It takes the argument `session`, 
# which is an instance of an `aiohttp.ClientSession`.   A task is created for each iteration of the loop.
def get_tasks(session):
    tasks = []
    iteration_counter = 0
    for xx in df['xxx']:
        tasks.append(asyncio.create_task(session.get(url.format(xxx, str(df.loc[iteration_counter, 'yyy'])), auth=aiohttp.BasicAuth(API_ID, API_KEY), ssl=False)))
        iteration_counter = iteration_counter + 1
    return tasks

# This is an Asynchronous function that retrieves data from the API, using `aiohttp` library.
# Loops through each task and gathers responses.  In this case, concurrent requests are limited by the semaphore wrapper.
# Responses are appended to a results list.
async def get_xx():
    async with aiohttp.ClientSession() as session:
        tasks = get_tasks(session)
        responses = []
        for task in tasks:
            async with semaphore:
                response = await task
                print(f"Response code: {response.status}")
                responses.append(response)
                time.sleep(1)
        for response in responses:
            results.append(await response.text())
   
# Execute API Calls through 2 functions defined above.
asyncio.run(get_xx())
api_call_end_time = time.time()
print("API Calls Finished: ", '{:.2f}'.format(api_call_end_time - api_call_start_time), "seconds.")

# Create a process results function to clean up the xml and matrix.
cleaning_start_time = time.time()

def process_result(result, df, row_counter):
    decoded_response = result.replace("\n", "")
    decoded_response = decoded_response.replace(',', '')
    response_json = json.loads(json.dumps(xmltodict.parse(decoded_response)))
    request_xml = response_json['Response']['Result'][0]['XXX']
    response_xml = response_json['Response']['Result'][1]['XXX']

    response_matrix = response_json['Response']['Result'][2]['Matrix']
    lob = response_xml['XXX']['LOB']
    state = response_xml['XXX]['XXX']['XXX']['XXX']['STATE']    
    
    # This part here loops through the cleaned up rating matrix and only returns dictionary items that have a value (all empty cells removed)
    matrix = {}
    for row_num in range(0, len(response_matrix['Row'])-1):
        for column_num in range(0, len(response_matrix['Row'][row_num]['Col'])-1):
            try:
                matrix[response_matrix['Row'][row_num]['Col'][column_num]['@id']] = response_matrix['Row'][row_num]['Col'][column_num]['#text']
            except:
                pass
    
    # Converts matrix to JSON for easy reading/accessing
    matrix_json = str(json.dumps(matrix))
    matrix_json.replace('\n', '')
    
    #  Formats the request xml to a useable xml format
    request_xml = str(dicttoxml(request_xml, custom_root='XXX', attr_type=False))
    request_xml = request_xml.replace("b'", "", 1)
    request_xml = request_xml[::-1].replace("'", "", 1)[::-1]

    # Appends clean request xml and rating matrix to original request file
    df.loc[row_counter, 'lob'] = lob
    df.loc[row_counter, 'rating_state'] = rating_state    
    df.loc[row_counter, 'request_xml'] = request_xml
    df.loc[row_counter, 'rating_matrix'] = matrix_json

# ThreadPoolExectur() allows us to execute the process_result function asynchronously using a pool of worker threads.  
row_counter = 0
with ThreadPoolExecutor() as executor:
    for result in results:
        executor.submit(process_result, result, df, row_counter)
        row_counter = row_counter + 1

cleaning_end_time = time.time()
print("Data Cleaning Finished: ", '{:.2f}'.format(cleaning_end_time - cleaning_start_time), "seconds.")

df.to_csv('C:\\Users\\XXX\\XXX\\rating.csv', index=False)

end_time = time.time()
print("Total Execution Time: ", '{:.2f}'.format(end_time - start_time), " seconds.")
print("For", '{:,d}'.format(len(df)), "requests, that is an average of", '{:.2f}'.format((end_time - start_time)/len(df)), "seconds per request.")
