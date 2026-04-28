# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 16:11:34 2026

@author: Hemani Vandra
"""

import os
import io
import sys
import zipfile
import requests
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")
sys.path.append(os.path.join(PROJECT_ROOT, 'config'))

import config

class Loader(object) :
    """
    Download and load cricsheet data
    """

    def __init__(self) :
        self.config_obj = config.Config()

        self.s3_bucket = os.getenv("S3_BUCKET")
        self.aws_region = os.getenv("AWS_REGION")
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id = self.aws_access_key,
            aws_secret_access_key = self.aws_secret_access_key,
            region_name = self.aws_region
        )

    def download_cricsheet(self, dest_dir) :
        dest_dir.mkdir(parents = True, exist_ok = True)
        zip_path = dest_dir / "ipl_csv2.zip"

        print("Downloading Cricsheet IPL data...")
        response = requests.get(self.config_obj.cricsheet_url, stream = True)
        response.raise_for_status()

        with open(zip_path, 'wb') as f :
            for chunk in response.iter_content(chunk_size = 8192) :
                f.write(chunk)

        print(f"Downloaded to {zip_path}")
        return zip_path

    def extract_cricsheet(self, zip_path, dest_dir) :
        extract_dir = dest_dir / "extracted"
        extract_dir.mkdir(parents = True, exist_ok = True)

        with zipfile.ZipFile(zip_path, 'r') as z :
            z.extractall(extract_dir)

        print(f"Extracted Cricsheet data to {extract_dir}")
        print(f"Extracted {len(list(extract_dir.glob('*.csv')))} CSV files")
        return extract_dir

    def load_deliveries(self, extract_dir) :
        """
        It's a utility function to load all ball-by-ball delivery CSVs into a single DataFrame.
        We are not calling this functions in our pipeline since we are uploading raw CSVs to S3 for Databricks Auto Loader to ingest directly.
        However, this function can be useful for local processing or debugging.
        Cricsheet CSV2 format has 2 file types per match:
        - <match_id>.csv -> ball-by-ball deliveries
        - <match_id>_info.csv -> match metadata
        """
        delivery_files = [f for f in extract_dir.glob("*.csv") if not f.name.endswith("_info.csv")]
        dfs = []

        for f in delivery_files :
            df = pd.read_csv(f)
            df["_source_file"] = f.name
            df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            dfs.append(df)

        deliveries = pd.concat(dfs, ignore_index = True)

        if 'season' in deliveries.columns :
            deliveries['season'] = deliveries['season'].astype(str).str[:4]

        print(f"Loaded {len(deliveries):,} delivery records from {len(delivery_files)} matches")
        return deliveries
    
    def load_match_info(self, extract_dir) :
        info_files = list(extract_dir.glob("*_info.csv"))

        rows = []
        for f in info_files:
            match_id = f.name.replace("_info.csv", "")
            with open(f, "r", encoding="utf-8") as file:
                for line in file:
                    # Strip newline and split on comma
                    parts = [p.strip() for p in line.strip().split(",")]
                    
                    if len(parts) < 3:
                        continue  # skip malformed lines
                    
                    # parts[0] is always "info", parts[1] is the key
                    key = parts[1]

                    # Everything from parts[2] onwards is the value
                    # Join back with comma for multi-value fields
                    value = ",".join(parts[2:])

                    rows.append({
                        "match_id": match_id,
                        "key": key,
                        "value": value,
                        "_ingested_at": datetime.now(timezone.utc).isoformat()
                    })

        match_info = pd.DataFrame(rows)
        print(f"Loaded metadata for {len(info_files)} matches — {len(match_info)} rows")
        return match_info

    # S3 Upload and Parquet saving methods
    def upload_file_to_s3(self, local_path, s3_prefix) :
        """Upload a single local file to s3.
        """
        s3_key = f"{s3_prefix}/{local_path.name}"
        self.s3_client.upload_file(str(local_path), self.s3_bucket, s3_key)
        print(f"Uploaded: s3://{self.s3_bucket}/{s3_key}")

    def upload_dataframes_to_s3(self, df, s3_key) :
        """Upload a match_info pandas dataframe to S3 as Parquet
            Uses in-memory buffer - no local file creation
        """
        buffer = io.BytesIO()
        df.to_parquet(buffer, index = False)
        buffer.seek(0)

        self.s3_client.put_object(
            Bucket = self.s3_bucket,
            Key = s3_key,
            Body = buffer.getvalue(),
            ContentType = "application/octet-stream"
        )
        print(f"Uploaded DataFrame to s3://{self.s3_bucket}/{s3_key} ({len(df):,} rows)")

    def upload_cricsheet_csvs_to_s3(self, extract_dir) :
        """Upload all extracted ball-by-ball CSV files to S3 under a prefix like 'cricsheet/extracted/'.
            Auto Loader on Databricks reads from this prefix.
        """
        delivery_csv_files = [
            f for f in extract_dir.glob("*.csv")
            if not f.name.endswith("_info.csv")
        ]

        if not delivery_csv_files :
            raise FileNotFoundError(f"No CSV files found in {extract_dir} to upload to S3")
        
        print(f"Uploading {len(delivery_csv_files)} CSV files to S3 bucket...")

        for i, f in enumerate(delivery_csv_files) :
            self.upload_file_to_s3(f, s3_prefix = "cricsheet/extracted")
            if i % 100 == 0 and i > 0 :
                print(f"Progress: {i}/{len(delivery_csv_files)} files uploaded...")
        
        print(f"Upload complete: {len(delivery_csv_files)} delivery files uploaded to S3")

    def save_to_parquet(self, df, name, dest_dir) :
        print(df.columns)
        out_path = dest_dir / f"{name}.parquet"
        df.to_parquet(out_path, index = False)
        print(f"Saved {name} -> {out_path} ({len(df):,} rows)")

    def main(self) :
        # step 1: download and extract cricsheet data
        zip_path = self.download_cricsheet(self.config_obj.raw_data_dir)
        extract_dir = self.extract_cricsheet(zip_path, self.config_obj.raw_data_dir)

        # step 2: load deliveries and match info into dataframes
        deliveries = self.load_deliveries(extract_dir)
        match_info = self.load_match_info(extract_dir)

        # # Step 3: Load raw csvs to s3 for Databricks Auto Loader ingestion
        # self.upload_cricsheet_csvs_to_s3(extract_dir)

        # # step 4: Upload match info as parquet to S3 - Databricks bronze reads this parquet
        # self.upload_dataframes_to_s3(
        #     match_info,
        #     s3_key = "cricsheet/raw_match_info.parquet"
        # )
        self.save_to_parquet(deliveries, "raw_deliveries", self.config_obj.raw_data_dir)
        self.save_to_parquet(match_info, "raw_match_info", self.config_obj.raw_data_dir)

        print("\nSample delivery columns:", deliveries.columns.tolist())
        print(deliveries.head(3))
        print(f"\n{'='*50}")
        print("Cricsheet pipeline complete")
        print(f"  Delivery rows : {len(deliveries):,}")
        print(f"  Match info rows: {len(match_info):,}")
        print(f"{'='*50}")

if __name__ == "__main__":
    obj = Loader()
    obj.main()
else :
    print("not ran")
