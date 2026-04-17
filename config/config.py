# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 13:48:46 2026

@author: Hemani Vandra
"""

import yaml
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

class Config(object) :

    def __init__(self) :
        config_path = PROJECT_ROOT / "config.yaml"
        with open(config_path, 'r') as f :
            config = yaml.safe_load(f)

        self.cricsheet_url = config['CRICSHEET']['URL']
        self.raw_data_dir = Path(config['CRICSHEET']['EXTRACT_DIR'])

        self.raw_data = Path(config['PATHS']['RAW_DATA'])
        self.scraped_data = Path(config['PATHS']['SCRAPED_DATA'])

        self.ipl_squads_url = config['SCRAPER']['IPL_SQUADS_URL']
        self.poll_interval_seconds = config['SCRAPER']['POLL INTERVAL SECONDS']

        self.bronze_db = config['DATABRICKS']['BRONZE_DB']
        self.silver_db = config['DATABRICKS']['SILVER_DB']
        self.gold_db = config['DATABRICKS']['GOLD_DB']
