# Python Code Samples

## Sample 1
I had to create a script to make over 1 million api calls to one of our business partners, download the information, and prepare the data for ingestion into our warehouse.  Due to api constraints, we are limited to only 10 asynchronous calls at a time. In this code I use asyncio and aiohttp to make our synchronous calls. Those requests are limited to 10 by a semaphore wrapper.  I also use a ThreadPoolExecutor to expedite the processing/formatting of the raw data.

## Sample 2

