from fastapi import FastAPI, Query
import pandas as pd

app = FastAPI()

def fetch_data(year: int = None, country: str = None, market: str = None):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv("C:\\Users\\rohan\\Downloads\\total_data.csv")
        # Apply filters based on provided parameters
        if year is not None:
            df = df[df['year'] == year]
        if country is not None:
            df = df[df['country'] == country]
        if market is not None:
            df = df[df['mkt_name'] == market]

        # Fill NaN values with empty strings
        df_filter = df.fillna('')

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
