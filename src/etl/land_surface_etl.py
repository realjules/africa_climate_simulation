import requests
import pandas as pd

def extract_land_surface_data():
    api_url = "https://api.example.com/land-surface"
    response = requests.get(api_url)
    data = response.json()
    return data

def transform_land_surface_data(data):
    df = pd.DataFrame(data)
    # Perform necessary transformations
    return df

def load_land_surface_data(df):
    output_file = "../data/processed/land_surface_data.csv"
    df.to_csv(output_file, index=False)

def run_land_surface_etl():
    data = extract_land_surface_data()
    transformed_data = transform_land_surface_data(data)
    load_land_surface_data(transformed_data)

if __name__ == "__main__":
    run_land_surface_etl()