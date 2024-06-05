import cdsapi
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta

def extract_atmospheric_data():
    """
    Extracts atmospheric temperature data from the CDS API for the last 100 years.
    Returns a pandas DataFrame with combined data for all available years.
    """
    c = cdsapi.Client()

    current_year = datetime.now().year
    start_year = current_year - 100

    temperature_data = []

    for year in range(start_year, current_year + 1):
        print(f"Retrieving data for year {year}")

        try:
            # Retrieve temperature data for the specified year from the CDS API
            data = c.retrieve(
                "reanalysis-era5-single-levels",
                {
                    "variable": "2m_temperature",
                    "year": str(year),
                    "month": "01",
                    "day": "01",
                    "time": "12:00",
                    "format": "netcdf",
                    "area": [31, -3, 28.75, -1],  # Rwanda bounding box (N/W/S/E)
                    "grid": [0.25, 0.25],  # Grid resolution (latitude, longitude)
                },
            )

            # Load the NetCDF data using xarray
            ds = xr.open_dataset(data)

            # Extract the relevant data (temperature at 2 meters)
            temp_data = ds["t2m"].to_dataframe().reset_index()

            # Add the year column to the DataFrame
            temp_data["year"] = year

            # Append the DataFrame to the temperature_data list
            temperature_data.append(temp_data)

        except Exception as e:
            # Print an error message if data retrieval fails for a specific year
            print(f"Error retrieving data for year {year}: {str(e)}")
            continue

    if len(temperature_data) > 0:
        # Concatenate DataFrames from all years into a single DataFrame
        combined_data = pd.concat(temperature_data, ignore_index=True)
        return combined_data
    else:
        # Return None if no data is available for any year
        return None

def transform_atmospheric_data(data):
    """
    Transforms the atmospheric temperature data by printing it.
    """
    if data is not None:
        print("Temperature data:")
        print(data)
    else:
        print("No temperature data available.")

def load_atmospheric_data(data):
    """
    Loads the atmospheric temperature data into a CSV file.
    """
    if data is not None:
        output_file = "data/processed/atmospheric_data.csv"
        data.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")

def run_atmospheric_etl():
    """
    Runs the ETL process for atmospheric temperature data.
    """
    # Extract data from the CDS API
    data = extract_atmospheric_data()

    # Transform the data
    transform_atmospheric_data(data)

    # Load the data into a CSV file
    load_atmospheric_data(data)

if __name__ == "__main__":
    # Run the ETL process when the script is executed
    run_atmospheric_etl()