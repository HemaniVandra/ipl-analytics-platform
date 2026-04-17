import time
import traceback
import random
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

RAW_DATA_DIR = Path("data/raw/scraped")

# IPL teams Cricinfo series page (update series ID per season)
IPL_SQUADS_URL = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/squads"

def get_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=options)


def scrape_team_squads(url: str = IPL_SQUADS_URL) -> list[dict]:
    driver = get_driver()
    players = []

    try:
        driver.get(url)
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

        # team_sections = driver.find_elements(By.CSS_SELECTOR, "ds-group ds-block ds-px-4 ds-py-2")
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

    finally:
        driver.quit()

    return players


def save_players(players: list[dict]):
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(players)
    out = RAW_DATA_DIR / "raw_players.parquet"
    df.to_parquet(out, index=False)
    print(f"Saved {len(df)} players → {out}")
    return df


if __name__ == "__main__":
    players = scrape_team_squads()
    df = save_players(players)
    print(df[["team", "player_name"]].to_string())