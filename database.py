import os
import h5py
import numpy as np
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="", 
    database="bigdata_no2"
)
cursor = conn.cursor()
folder_path = r"D:\data nasa\OMNO2d_004-20260322_061626"

for file_name in os.listdir(folder_path):
    if file_name.endswith(".he5"):

        print(f"Processing {file_name}...")

        file_path = os.path.join(folder_path, file_name)
        year = int(file_name.split("_")[2][:4])

        try:
            with h5py.File(file_path, 'r') as f:
                no2 = f["HDFEOS/GRIDS/ColumnAmountNO2/Data Fields/ColumnAmountNO2TropCloudScreened"][:]

                lat = np.linspace(-90, 90, no2.shape[0])
                lon = np.linspace(-180, 180, no2.shape[1])
                lon2d, lat2d = np.meshgrid(lon, lat)

                mask = (
                    (lat2d >= -11) & (lat2d <= 20) &
                    (lon2d >= 90) & (lon2d <= 150)
                )

                no2_filtered = np.where(mask, no2, np.nan)

                lat_flat = lat2d.flatten()
                lon_flat = lon2d.flatten()
                no2_flat = no2_filtered.flatten()

                batch_size = 1000
                batch = []

                for i in range(len(no2_flat)):
                    if not np.isnan(no2_flat[i]):
                        batch.append((
                            year,
                            float(lat_flat[i]),
                            float(lon_flat[i]),
                            float(no2_flat[i])
                        ))

                        if len(batch) == batch_size:
                            cursor.executemany("""
                                INSERT INTO data_no2 (year, latitude, longitude, no2)
                                VALUES (%s, %s, %s, %s)
                            """, batch)
                            conn.commit()
                            batch = []

                if batch:
                    cursor.executemany("""
                        INSERT INTO data_no2 (year, latitude, longitude, no2)
                        VALUES (%s, %s, %s, %s)
                    """, batch)
                    conn.commit()

        except Exception as e:
            print(f"Error di {file_name}: {e}")

print("SEMUA DATA BERHASIL MASUK!")
conn.close()