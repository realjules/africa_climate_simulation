import requests
import pandas as pd

def extract_socioeconomic_data():
    api_url = "https://api.example.com/socioeconomic"
    response = requests.get(api_url)
    data = response.json()
    return data

def transform_socioeconomic_data(data):
    df = pd.DataFrame(data)
    # Perform necessary transformations
    return df

def load_socioeconomic_data(df):
    output_file = "../data/processed/socioeconomic_data.csv"
    df.to_csv(output_file, index=False)

def run_socioeconomic_etl():
    data = extract_socioeconomic_data()
    transformed_data = transform_socioeconomic_data(data)
    load_socioeconomic_data(transformed_data)

if __name__ == "__main__":
    run_socioeconomic_etl()