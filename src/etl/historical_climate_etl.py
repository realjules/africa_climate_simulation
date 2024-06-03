import requests
import pandas as pd

def extract_historical_climate_data():
    api_url = "https://api.example.com/historical-climate"
    response = requests.get(api_url)
    data = response.json()
    return data

def transform_historical_climate_data(data):
    df = pd.DataFrame(data)
    # Perform necessary transformations
    return df

def load_historical_climate_data(df):
    output_file = "../data/processed/historical_climate_data.csv"
    df.to_csv(output_file, index=False)

def run_historical_climate_etl():
    data = extract_historical_climate_data()
    transformed_data = transform_historical_climate_data(data)
    load_historical_climate_data(transformed_data)

if __name__ == "__main__":
    run_historical_climate_etl()