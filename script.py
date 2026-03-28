import os
import h5py
import numpy as np
import pandas as pd

folder_path = "."

for file_name in os.listdir(folder_path):
    if file_name.endswith(".he5"):

        file_path = os.path.join(folder_path, file_name)

        print(f"\nProcessing {file_name}...")

        try:
            with h5py.File(file_path, 'r') as f:
                no2 = f["HDFEOS/GRIDS/ColumnAmountNO2/Data Fields/ColumnAmountNO2TropCloudScreened"][:]
                lat = np.linspace(-90, 90, no2.shape[0])
                lon = np.linspace(-180, 180, no2.shape[1])

                lon2d, lat2d = np.meshgrid(lon, lat)
                lat_min, lat_max = -11, 20
                lon_min, lon_max = 90, 150

                mask = (
                    (lat2d >= lat_min) & (lat2d <= lat_max) &
                    (lon2d >= lon_min) & (lon2d <= lon_max)
                )

                no2_filtered = np.where(mask, no2, np.nan)

                df = pd.DataFrame({
                    'latitude': lat2d.flatten(),
                    'longitude': lon2d.flatten(),
                    'NO2': no2_filtered.flatten()
                })

                df = df.dropna()

                year = file_name[20:24]

                output_file = f"no2_{year}.csv"
                df.to_csv(output_file, index=False)

                print(f"Berhasil simpan {output_file}")

        except Exception as e:
            print(f"Error di {file_name}: {e}")