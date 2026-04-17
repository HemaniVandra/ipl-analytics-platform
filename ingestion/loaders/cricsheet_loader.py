# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 16:11:34 2026

@author: Hemani Vandra
"""

import os
import sys
import zipfile
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(os.path.join(PROJECT_ROOT, 'config'))

import config

class Loader(object) :
    """
    Download and load cricsheet data
    """

    def __init__(self) :
        self.config_obj = config.Config()
        
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

        print(f"Extracted {len(list(extract_dir.glob('*.csv')))} CSV files")
        return extract_dir

    def load_deliveries(self, extract_dir) :
        """
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

    def save_to_parquet(self, df, name, dest_dir) :
        print(df.columns)
        out_path = dest_dir / f"{name}.parquet"
        df.to_parquet(out_path, index = False)
        print(f"Saved {name} -> {out_path} ({len(df):,} rows)")

    def main(self) :
        zip_path = self.download_cricsheet(self.config_obj.raw_data_dir)
        extract_dir = self.extract_cricsheet(zip_path, self.config_obj.raw_data_dir)

        deliveries = self.load_deliveries(extract_dir)
        match_info = self.load_match_info(extract_dir)

        self.save_to_parquet(deliveries, "raw_deliveries", self.config_obj.raw_data_dir)
        self.save_to_parquet(match_info, "raw_match_info", self.config_obj.raw_data_dir)

        print("\nSample delivery columns:", deliveries.columns.tolist())
        print(deliveries.head(3))

if __name__ == "__main__":
    obj = Loader()
    obj.main()
else :
    print("not ran")
