import pandas as pd
from sqlalchemy import create_engine,text
import os
import time

DATABASE_URL="postgresql://postgres:rahul150@localhost:5432/nyc_taxi"

def get_engine():
    return create_engine(DATABASE_URL)

def create_bronze_schema(engine):
    """Create Bronze Schema if it doesn't exist"""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        conn.commit()
    print("✓ Bronze schema ready")

def create_bronze_table(engine):
    """Create the raw trips table in bronze layer"""
    ddl="""
    CREATE TABLE IF NOT EXISTS bronze.raw_trips(
        vendor_id           INTEGER,
        tpep_pickup_datetime    TIMESTAMP,
        tpep_dropoff_datetime   TIMESTAMP,
        passenger_count     FLOAT,
        trip_distance       FLOAT,
        ratecode_id         FLOAT,
        store_and_fwd_flag  TEXT,
        pu_location_id      INTEGER,
        do_location_id      INTEGER,
        payment_type        FLOAT,
        fare_amount         FLOAT,
        extra               FLOAT,
        mta_tax             FLOAT,
        tip_amount          FLOAT,
        tolls_amount        FLOAT,
        improvement_surcharge FLOAT,
        total_amount        FLOAT,
        congestion_surcharge FLOAT,
        airport_fee         FLOAT,
        ingested_at         TIMESTAMP DEFAULT NOW(),
        source_file         TEXT
    );
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    print("✓ Bronze table ready")

def ingest_parquet(filepath,engine):
    """Load parquet file into bronze layer in chunks"""
    print(f"\n→ Reading {filepath}...")
    df=pd.read_parquet(filepath)

    # Standardise column names to lowercase with underscores
    df.columns=[c.lower().replace(' ','_') for c in df.columns]

     # Rename columns to match our schema
    col_map={
         'vendorid':'vendor_id',
         'ratecodeid':'ratecode_id',
         'pulocationid':'pu_location_id',
         'dolocationid':'do_location_id',

    }
    df.rename(columns=col_map,inplace=True)

    # Add metadata columns
    df['ingested_at']=pd.Timestamp.now()
    df['source_file']=os.path.basename(filepath)

    # Keep only columns that exist in our schema
    schema_cols=[
        'vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'passenger_count', 'trip_distance', 'ratecode_id', 'store_and_fwd_flag',
        'pu_location_id', 'do_location_id', 'payment_type', 'fare_amount',
        'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount', 'congestion_surcharge',
        'airport_fee', 'ingested_at', 'source_file'
    ]
    existing_cols=[c for c in schema_cols if c in df.columns]
    df=df[existing_cols]

    print(f"→ {len(df):,} rows loaded into memory")
    print(f"→ Writing to PostgreSQL in chunks...")

    # Write in chunks of 100k rows — important for large files
    chunk_size=100_000
    total_chunks=(len(df)//chunk_size)+1

    start=time.time()
    for i,chunk_start in enumerate(range(0,len(df),chunk_size)):
        chunk=df.iloc[chunk_start:chunk_start+chunk_size]
        chunk.to_sql(
            name='raw_trips',
            schema='bronze',
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        print(f" Chunk{i+1}/{total_chunks} written ({chunk_start + len(chunk):,} rows)")
    elapsed=time.time() - start
    print(f"\n✓ Ingestion complete - {len(df):,} rows in {elapsed:.1f}s")
    
def verify_ingestion(engine):
    """Quick check on what was loaded"""
    with engine.connect() as conn:
        result=conn.execute(text("""
             SELECT
                COUNT(*) as total_rows,
                MIN(tpep_pickup_datetime) as earliest_trip,
                MAX(tpep_pickup_datetime) as latest_trip,
                ROUND(AVG(total_amount)::numeric, 2) as avg_fare,
                source_file
            FROM bronze.raw_trips
            GROUP BY source_file
         """))
        print("\n=== INGESTION SUMMARY ===")
        for row in result:
            print(f"File:            {row.source_file}")
            print(f"Total Rows:      {row.total_rows:,}")
            print(f"Date Range:      {row.earliest_trip} → {row.latest_trip}")
            print(f"Avg Fare:        ${row.avg_fare}")

if __name__ == "__main__":
    print("=== NYC Taxi Bronze Ingestion ===\n")
    engine=get_engine()
    create_bronze_schema(engine)
    create_bronze_table(engine)

    # Path to your downloaded parquet file
    data_folder=os.path.join(os.path.dirname(__file__),'..','data')
    parquet_files=[f for f in os.listdir(data_folder) if f.endswith('.parquet')]

    if not parquet_files:
        print("No parquet files found in data/ folder")
        print("Download from: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")
    else:
        for f in parquet_files:
            filepath=os.path.join(data_folder,f)
            ingest_parquet(filepath,engine)
        verify_ingestion(engine)
