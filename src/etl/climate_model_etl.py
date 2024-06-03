import requests
import pandas as pd

def extract_climate_model_data():
    api_url = "https://api.example.com/climate-model"
    response = requests.get(api_url)
    data = response.json()
    return data

def transform_climate_model_data(data):
    df = pd.DataFrame(data)
    # Perform necessary transformations
    return df

def load_climate_model_data(df):
    output_file = "../data/processed/climate_model_data.csv"
    df.to_csv(output_file, index=False)

def run_climate_model_etl():
    data = extract_climate_model_data()
    transformed_data = transform_climate_model_data(data)
    load_climate_model_data(transformed_data)

if __name__ == "__main__":
    run_climate_model_etl()