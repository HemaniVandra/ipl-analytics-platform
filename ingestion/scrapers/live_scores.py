# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 16:57:59 2026

@author: Hemani Vandra
"""

import os
import sys
import json
import boto3
import requests
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")
sys.path.append(os.path.join(PROJECT_ROOT, 'config'))
import config

class Score(object) :
    """
    Scrape live scores from a cricket API
    """
    def __init__(self) :
        self.config_obj = config.Config()
        self.s3_bucket = os.getenv("S3_BUCKET")
        self.aws_region = os.getenv("AWS_REGION")
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.cricapi_key = os.getenv("CRICAPI_KEY")

        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id = self.aws_access_key,
            aws_secret_access_key = self.aws_secret_access_key,
            region_name = self.aws_region
        )

    def get_all_matches(self) :
        """Fetch all current matches from the cricket API
        Returns a raw list of match dictionaries.
        """
        url = f"{self.config_obj.cricapi_base_url}/matches"
        params = {
            "apikey": self.cricapi_key,
            "offset": 0,
            "limit": 100
        }
        try :
            response = requests.get(url, params = params, timeout = 10)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "success" :
                raise ValueError(f"API returned non-success status: {data.get('status')}")

            matches = data.get("data", [])
            print(f"Total matches from API: {len(matches)}")
            return matches

        except requests.exceptions.Timeout :
            raise RuntimeError("cricapi.com request timed out after 10 seconds")
        except requests.exceptions.RequestException as e :
            raise RuntimeError(f"cricapi.com request failed: {str(e)}")

    def filter_ipl_matches(self, matches) :
        """Filter only IPL matches from all matches returned by API.
        """
        ipl_matches = [
            m for m in matches
            if self.config_obj.cricapi_ipl_keyword in m.get("series", "")
            or self.config_obj.cricapi_ipl_keyword in m.get("name", "")
        ]
        print(f"IPL matches found: {len(ipl_matches)}")
        return ipl_matches

    def filter_live_matches(self, ipl_matches) :
        """From IPL macthes, return only currently live ones.
        A match is live if it has started but not yet ended.
        """
        live = [
            m for m in ipl_matches 
            if m.get("matchStarted") is True
            and m.get("matchEnded") is False
        ]
        print(f"Live IPL matches found: {len(live)}")
        return live

    def get_match_details(self, match_id) :
        """Fetch detailed score card for a specific match.
        Returns a dictionary with match details and score information.
        """
        url = f"{self.config_obj.cricapi_base_url}/match_info"
        params = {
            "apikey": self.cricapi_key,
            "id": match_id
        }

        try :
            response = requests.get(url, params = params, timeout = 10)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "success" :
                raise ValueError(f"Match detail API failed: {data.get("status")}")

            return data.get("data", {})

        except requests.exceptions.RequestException as e :
            raise RuntimeError(f"Match detail request failed for {match_id}: {str(e)}")

    def parse_live_match(self, raw_match) :
        """Extract and flatten the fields we care about from a raw
        API match dictionary into a clean structured record.
        """
        # parse score list - API returns list of innings scores
        scores = raw_match.get("score", [])

        innings_1 = scores[0] if len(scores) > 0 else {}
        innings_2 = scores[1] if len(scores) > 1 else {}

        return {
            # match identifiers
            "match_id" : raw_match.get("id"),
            "match_name" : raw_match.get("name"),
            "series" : raw_match.get("series"),
            "match_type" : raw_match.get("matchType"),

            # teams
            "team1" : raw_match.get("teams", [None, None])[0],
            "team2" : raw_match.get("teams", [None, None])[1],

            # match state
            "status" : raw_match.get("status"),
            "match_started" : raw_match.get("matchStarted"),
            "match_ended" : raw_match.get("matchEnded"),

            # venue
            "venue" : raw_match.get("venue"),

            # date
            "match_date" : raw_match.get("date"),

            # Inning1 score
            "innings1_team" : innings_1.get("inning"),
            "innings1_runs" : innings_1.get("r"),
            "innings1_wickets" : innings_1.get("w"),
            "innings1_overs" : innings_1.get("o"),

            # Inning2 score
            "innings2_team" : innings_2.get("inning"),
            "innings2_runs" : innings_2.get("r"),
            "innings2_wickets" : innings_2.get("w"),
            "innings2_overs" : innings_2.get("o"),

            # metadata
            "_fetched_at" : datetime.now(timezone.utc).isoformat(),
            "_source" : "cricapi"
        }

    # S3 upload
    def upload_live_scores_to_s3(self, live_scores) :
        """Upload live scores as a JSON file to S3.
        File is timestamped so each fetch is stored separately.
        Auto Loader on Databricks will pick up new files automatically.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        # s3_key = f"live_scores/{timestamp}_live_scores.json"

        # # convert to JSON bytes
        # content = json.dumps(live_scores, indent = 2).encode("utf-8")
        # self.s3_client.put_object(
        #     Bucket = self.s3_bucket,
        #     Key = s3_key,
        #     Body = content,
        #     ContentType = "application/json"
        # )

        # s3_path = f"s3://{self.s3_bucket}/{s3_key}"
        # print(f"Uploaded live scores to {s3_path}")

        self.live_scores_data.mkdir(parents = True, exist_ok = True)
        # convert to JSON bytes
        # content = json.dumps(live_scores, indent = 2).encode("utf-8")
        out = self.live_scores_data / f"{timestamp}_live_scores.json"
        # Save the file
        with open(out, "w", encoding="utf-8") as f:
            json.dump(live_scores, f, indent = 2)
        print(f"Uploaded live scores to {out}")

        return out
        
    def upload_latest_pointer(self, live_scores) :
        """Also write a latest.json that always has the most recent scores.
        Useful for the live dashboard which always needs the current state.
        """
        # content = json.dumps(live_scores, indent = 2).encode("utf-8")

        # self.s3_client.put_object(
        #     Bucket = self.s3_bucket,
        #     Key = "live_scores/latest.json",
        #     Body = content,
        #     ContentType = "application/json"
        # )

        out = self.live_scores_data / "latest.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(live_scores, f, indent = 2)
        print("Updated live_scores/latest.json with the latest scores")

    # Status check helpers
    def is_ipl_match_live(self) :
        """Simple boolean check - used by Airflow ShortCircuitOperator
        to decide whether to run the live pipeline or skip it.
        Returns True if at least 1 live IPL match is found, False otherwise.
        """
        try :
            matches = self.get_all_matches()
            ipl_matches = self.filter_ipl_matches(matches)
            live = self.filter_live_matches(ipl_matches)

            return len(live) > 0
        except Exception as e :
            print(f"Live check failed: {e}")
            return False

    def get_live_match_summary(self) :
        """Returns a summary dict for logging and notifications.
        Shows current score at a glance.
        """
        matches = self.get_all_matches()
        ipl_matches = self.filter_ipl_matches(matches)
        live = self.filter_live_matches(ipl_matches)

        if not live :
            return {
                "live" : False,
                "message" : "No live IPL matches at the moment."
            }

        summaries = []
        for match in live :
            parsed = self.parse_live_match(match)
            summaries.append({
                "match" : parsed["match_name"],
                "status" : parsed["status"],
                "score_1" : f"{parsed['innings1_team']}:"
                            f"{parsed['innings1_runs']} / {parsed['innings1_wickets']} "
                            f"({parsed['innings1_overs']} ov)",
                "score_2" : f"{parsed['innings2_team']}:"
                            f"{parsed['innings2_runs']} / {parsed['innings2_wickets']} "
                            f"({parsed['innings2_overs']} ov)"
                            if parsed['innings2_runs'] else "Yet to bat"
            })
        return {
            "live" : True,
            "matches" : summaries
        }
    
    # main pipeline method
    def fetch_and_store_live_scores(self) :
        """Master function called by Airflow live match DAG.

        Flow:
        1. Fetch all matches from API
        2. Filter to IPL only
        3. Filter to live only
        4. Parse and flatten the data into clean structure
        5. Upload to S3 (timestamped + latest)
        6. Return parsed scores for logging and notifications
        """
        print(f"\n{'='*50}")
        print(f"Live score fetch started: {datetime.now(timezone.utc).isoformat()}")
        print(f"{'='*50}\n")

        all_matches = self.get_all_matches()
        ipl_matches = self.filter_ipl_matches(all_matches)
        live_matches = self.filter_live_matches(ipl_matches)

        if not live_matches :
            print("No live IPL matches found - skipping upload")
            return []

        live_scores = [self.parse_live_match(m) for m in live_matches]
        print(f"Parsed {len(live_scores)} live match records")

        s3_path = self.upload_live_scores_to_s3(live_scores)
        self.upload_latest_pointer(live_scores)

        for score in live_scores :
            print(f"\nMatch: {score['match_name']}")
            print(f"Status: {score['status']}")
            print(f"    {score['innings1_team']}"
                  f" {score['innings1_runs']}/{score['innings1_wickets']} "
                  f"({score['innings1_overs']} ov)")
            if score['innings2_runs'] :
                print(f"    {score['innings2_team']}"
                      f" {score['innings2_runs']}/{score['innings2_wickets']} "
                      f"({score['innings2_overs']} ov)")

        print(f"\nFetch complete - stored at {s3_path}")
        return live_scores

    def main(self) :
        """Main method for testing and local runs.
         In production, Airflow will call fetch_and_store_live_scores() directly.
        """
        # Test 1 - check if any match is live right now
        print("Is IPL match live?", self.is_ipl_match_live())

        # Test 2 - get live match summary without uploading
        summary = self.get_live_match_summary()
        print("\nLive match summary:")
        print(json.dumps(summary, indent = 2))

        # Test 3 - run full pipeline (upload to S3)
        scores = self.fetch_and_store_live_scores()
        print(f"\nTotal live scores fetched: {len(scores)}")

if __name__ == "__main__" :
    score_obj = Score()
    score_obj.main()