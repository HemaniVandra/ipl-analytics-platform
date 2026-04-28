# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 14:05:12 2026

@author: Hemani Vandra
"""
import os
import io
import boto3
import sys
import time
import traceback
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")
sys.path.append(os.path.join(PROJECT_ROOT, 'config'))
import config

class Scraper(object) :
    """
    Scrape match metadata from Cricinfo
    """

    def __init__(self) :
        self.config_obj = config.Config()
        self.scraped_data = self.config_obj.scraped_data
        self.ipl_squads_url = self.config_obj.ipl_squads_url
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

    def get_driver(self) :
        """
        Get a Selenium WebDriver instance
        """
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        driver = webdriver.Chrome(options = options)

        driver.delete_all_cookies()
        return driver

    def scrape_team_squads(self, url) :
        """Scrape IPL team squads from the given URL
        """
        driver = self.get_driver()
        players = []

        try :
            driver.get(url)
            print(driver.title, "get the title to confirm page load")
            time.sleep(5)  # Initial wait for page to load
            wait = WebDriverWait(driver, 15)

            # Wait for the squads section to load
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.ds-p-4")
            ))
            time.sleep(5)  # Additional wait to ensure all content is loaded

            # Clicking on first team
            first_team = wait.until(EC.element_to_be_clickable((By.XPATH, '//a[@title = "Chennai Super Kings Squad"]')))
            driver.execute_script("arguments[0].click();", first_team)
            time.sleep(5)  # Wait for the squad details to load

            team_sections = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//a[contains(@class, "ds-group ds-block ds-px-4 ds-py-2")]')))
            
            for section in team_sections :
                try :
                    time.sleep(2)  # Wait before clicking to avoid rapid interactions
                    actions = ActionChains(driver).move_to_element(section).click().perform() # perform click using actionchains to mimic real user interaction
                    time.sleep(3)  # Wait for the squad details to load

                    team_name = section.find_element(By.XPATH, ".//span").text

                    players_sections = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class, "ds-flex ds-space-x-2")]')))
                    for player in players_sections :
                        player_name = player.find_element(By.XPATH, ".//div/a").get_attribute("title")
                        player_link = player.find_element(By.XPATH, ".//div/a").get_attribute("href")

                        players.append({
                            "team": team_name,
                            "player_name": player_name,
                            "cricinfo_url": player_link,
                            "season" : "2024",
                            "_scraped_at": datetime.now(timezone.utc).isoformat(),
                            "_source" : "cricinfo_squads"
                        })

                except Exception as e :
                    traceback.print_exc()
                    print(f"Skipping a team section due to error: {e}")
                    continue
            print(f"Scraped {len(players)} players across {len(team_sections)} teams")

        finally :
            driver.quit()
        
        return players

    def save_players_to_s3(self, players) :
        """
        Save scraped player data to S3 as Parquet
        Uses in-memory buffer - no local file creation
        Returns the S3 path of the uploaded file
        """
        df = pd.DataFrame(players)

        # write to in-memory buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3_key = f"scraped/raw_players.parquet"
        self.s3_client.put_object(
            Bucket = self.s3_bucket,
            Key = s3_key,
            Body = buffer.getvalue(),
            ContentType = "application/octet-stream"
        )

        s3_path = f"s3://{self.s3_bucket}/{s3_key}"
        print(f"Saved {len(df)} players to {s3_path}")
        return s3_path

    def save_players(self, players) :
        """
        Save scraped player data to Parquet
        """
        self.scraped_data.mkdir(parents = True, exist_ok = True)
        df = pd.DataFrame(players)
        out = self.scraped_data / "raw_players.parquet"
        df.to_parquet(out, index=False)
        print(f"Saved {len(df)} players -> {out}")
        return df

    def main(self) :
        """
        Main method to orchestrate the scraping process
        """
        players = self.scrape_team_squads(self.ipl_squads_url)
        self.save_players(players)
        print(f"\n{'='*50}")
        print("Scraper pipeline complete")
        print(f"  Players scraped : {len(players)}")
        print(f"  Teams scraped   : {len(set(p['team'] for p in players))}")
        print(f"{'='*50}")

if __name__ == "__main__" :
    scraper = Scraper()
    scraper.main()