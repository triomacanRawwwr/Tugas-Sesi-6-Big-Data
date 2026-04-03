import pandas as pd
from sqlalchemy import create_engine

# 1. KONEKSI KE MYSQL
engine = create_engine("mysql+pymysql://root:@localhost/bigdata_no2")

print("Terhubung ke database")

# 2. SETTING
chunk_size = 10000
query = "SELECT year, latitude, longitude, no2 FROM data_no2"

print("Mulai proses cleaning, transformasi, dan filtering outlier...")

total_processed = 0
chunk_number = 0

# 3. PROSES PER CHUNK
for chunk in pd.read_sql(query, engine, chunksize=chunk_size):

    chunk_number += 1
    print(f"\nMemproses chunk ke-{chunk_number} ({len(chunk)} baris)")

    # CLEANING DASAR
    chunk = chunk.drop_duplicates()
    chunk = chunk.dropna()

    # TRANSFORMASI TIPE DATA
    chunk["latitude"] = chunk["latitude"].astype(float)
    chunk["longitude"] = chunk["longitude"].astype(float)
    chunk["no2"] = chunk["no2"].astype(float)

    # buang nilai tidak masuk akal
    chunk = chunk[(chunk["no2"] > 0) & (chunk["no2"] < 1e16)]

    # FILTER OUTLIER (STATISTIK)
    if len(chunk) > 0:
        q_low = chunk["no2"].quantile(0.01)
        q_high = chunk["no2"].quantile(0.99)
        chunk = chunk[(chunk["no2"] >= q_low) & (chunk["no2"] <= q_high)]

    # NORMALISASI (MIN-MAX)
    if len(chunk) > 0:
        min_val = chunk["no2"].min()
        max_val = chunk["no2"].max()

        if max_val - min_val != 0:
            chunk["no2_norm"] = (chunk["no2"] - min_val) / (max_val - min_val)
        else:
            chunk["no2_norm"] = 0

    # SIMPAN KE MYSQL
    if len(chunk) > 0:
        chunk.to_sql(
            name="data_no2_cleaned",
            con=engine,
            if_exists="append",   
            index=False,
            method="multi"
        )

    total_processed += len(chunk)
    print(f"Chunk ke-{chunk_number} selesai. Total tersimpan: {total_processed}")

print("\nSemua data selesai diproses dan disimpan")