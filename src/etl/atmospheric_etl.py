import cdsapi
import xarray as xr
import pandas as pd

def extract_atmospheric_data():
    c = cdsapi.Client()

    data = c.retrieve(
        "reanalysis-era5-single-levels",
        {
            "variable": "2m_temperature",
            "year": "2022",
            "month": "01",
            "day": "01",
            "time": "12:00",
            "format": "netcdf",
            "area": [31, -3, 28.75, -1],  # Rwanda bounding box (N/W/S/E)
        },
        'temperature_data.nc'
    )

    return 'temperature_data.nc'

def transform_atmospheric_data(data_file):
    # Load the NetCDF file using xarray
    ds = xr.open_dataset(data_file)

    # Extract the relevant data (temperature at 2 meters)
    temperature_data = ds["t2m"].to_dataframe().reset_index()

    return temperature_data

def load_atmospheric_data(df):
    output_file = "../data/processed/atmospheric_data.csv"
    df.to_csv(output_file, index=False)

def run_atmospheric_etl():
    data_file = extract_atmospheric_data()
    transformed_data = transform_atmospheric_data(data_file)
    load_atmospheric_data(transformed_data)

if __name__ == "__main__":
    run_atmospheric_etl()
