from fastapi import FastAPI, Query
import pandas as pd
import boto3
from io import StringIO
import uvicorn


app = FastAPI()

def fetch_data(year: int = None, country: str = None, market: str = None):
    try:
        # Read the CSV file into a DataFrame
        # Initialize S3 client
        # s3 = boto3.client('s3',aws_access_key_id='',aws_secret_access_key='')

        # Specify the bucket name and file key
        # bucket_name = 'truck-eta-classification-logs'
        # file_key = 'raw_food_data/total_data.csv'

        # Download the file from S3
        # response = s3.get_object(Bucket=bucket_name, Key=file_key)
        # csv_content = response['Body'].read().decode('utf-8')

        # Load CSV content into a pandas DataFrame
        df = pd.read_csv("https://food-data-test.s3.us-east-2.amazonaws.com/total_data.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEBgaCmFwLXNvdXRoLTEiSDBGAiEAqn5ZKj506Xu%2FPJZa0NLYfNWCGi8W3x52imJSUv4W0%2B4CIQDHV%2F6LrHE7V0cx1MuyUbsryNM7xJRTcMipoxARp6Zoryr9AghBEAIaDDE0MzE3NjIxOTU1MSIMqiQejxyDyox4rlrMKtoCphkoVLhDfTe4mgcdMrLFCV%2BHK%2B4hy1qa67bn3MtPoWsPf7vsrEqizHRoOk3hjw%2FpIHE4nM4KM9t8YGPrkBDsR8glGJ1Qx2X9qeeM4dMYJVse9lGfdzPvQX2PEfOxJZNHNPVgSM8TJ3zwFjuZLsEhr6g9dVa%2BtZoHtnXhP1l0IktAqigsWWw2M%2FZYWjjF12Kpr36x36zB%2BBvJQnR9wjvY2cT5%2BWoWxzMVe6WjxjxqQMwzBvO%2F0sd2kgQ5JgmEmep1jUmezwE7%2FCooygbe4rgKP0F30JkWY0if5Uaj78exVUFsxWQ93B60Pm3yKaBFBFt6Jdp0KFg2eEkjAE04VvHcHzb9xUNxxCj1u7pbsmwy6rFRMJxHir85DPTrqqYi3IEtvyK%2FpPHYTmCY4h6iiz8rIbndw%2FevIwGu0Mz9ZFTQ4W%2FfRPDhK4Y7Nv8KwdYjsMkPIAvVY2%2B%2BtO10TzCB0KmwBjqyAmjk%2B6IAOQNcDj6K%2Bv0GJXVltxnnK%2F0t6JoyXXXUvLJya80pVp6VMPeRkMTp6x1zNRVoohCaH8T3suv%2Bitw8y3aUXtp05bVOq%2BvVmIhAKJHtz%2FgUIhbhOBHj9yrVVGUvkY0L8HDw6HxHnS6D5atJSFknzY2bfDATmUTl4fHsMKQe%2Ff0SYpw%2FYBAQDS5j%2Ba8jZ2V11B9jMrGQfOybbGcrlI8CFIdYIcKTxbG7IH5aKUcbtmIb9NJRhj3WkpSuocqxElV1eGU9LkVngiN3jFegBE8eX%2BmIB4SEwAjZx7KLIw8tT0YvHmnyB3olH%2Fc8STBbIuDsB7Y3VRa%2F085ZKvocfEPiYLPvgttR5M45e9Z48kPo%2FpSfroC6Ib3dVEUM%2F%2FWEhVUstgPj3QxHu7cJHvz1C3hsuA%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20240401T075920Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIASCVPXXOPVMSZRUHF%2F20240401%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Signature=d171c067bb1b9aff14394c2068803c5850e3e76432db35b6a3681bbb48891df6")
        # df = pd.read_csv(StringIO(csv_content))
        print(df.shape[0])
        # Apply filters based on provided parameters
        if year is not None:
            print("1")
            df = df[df['year'] == year]
        if country is not None:
            print("2")
            df = df[df['country'] == country]
        if market is not None:
            print("3")
            df = df[df['mkt_name'] == market]

        # Fill NaN values with empty strings
        df_filter = df.fillna('')

        print(df_filter.shape[0])
        
        # Convert filtered DataFrame to JSON
        if df_filter is None or df_filter.empty:
            raise ValueError('No data found for the specified filters.')
        else:
            filtered_json = df_filter.to_json(orient='records')
            return filtered_json

    except Exception as e:
        return {'error': str(e)}

@app.get('/fetch_data')
async def fetch_data_api(year: int = Query(None), country: str = Query(None), market: str = Query(None)):
    try:
        # Call fetch_data function with provided parameters
        filtered_data = fetch_data(year, country, market)

        if filtered_data is None:
            raise ValueError('No data found for the specified filters.')
        else:
            # Return filtered data as API response
            return filtered_data
    except Exception as e:
        return {'error': str(e)}, 400

if __name__ == '__main__':
    uvicorn.run(app, port=8080, host='0.0.0.0')
