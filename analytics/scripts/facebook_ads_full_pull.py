import os
import sys
import json
import requests
import pandas as pd
from typing import Iterator, Dict, Any, List

# --- Configuration ---
# Fetches configuration from environment variables. Exits if required ones are missing.
try:
    TOKEN = os.environ["FB_ACCESS_TOKEN"]
    ACCOUNT = os.environ["FB_AD_ACCOUNT_ID"]
except KeyError as e:
    sys.exit(f"❌ Error: Missing required environment variable: {e}")

# Use getenv for optional variables with sensible defaults.
API_VER = os.getenv("FB_API_VER", "v20.0")
LOOKBACK = int(os.getenv("FB_LOOKBACK_DAYS", "7"))
OUT_DIR = "analytics/dataprocessed"
BASE_URL = "https://graph.facebook.com"

# --- Session Setup ---
# A single, reusable session is more efficient.
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {TOKEN}"})


def paginate(url: str, params: Dict[str, Any] = None) -> Iterator[Dict[str, Any]]:
    """
    Handles pagination for the Facebook Graph API with enhanced error handling.
    """
    current_url = url
    current_params = params or {}
    while True:
        try:
            resp = session.get(current_url, params=current_params, timeout=90)
            resp.raise_for_status()
            payload = resp.json()

            yield from payload.get("data", [])

            next_url = payload.get("paging", {}).get("next")
            if not next_url:
                break
            
            current_url = next_url
            current_params = {}

        except requests.exceptions.HTTPError as e:
            # Enhanced error handling to provide more specific advice.
            error_details = {}
            try:
                error_details = e.response.json().get("error", {})
            except json.JSONDecodeError:
                pass # Response was not JSON

            error_code = error_details.get("code")
            error_message = error_details.get("message")

            print(f"❌ API Error on request to {e.response.url}:\n   Status: {e.response.status_code}", file=sys.stderr)
            
            if error_code == 100:
                print(f"   Message: {error_message}", file=sys.stderr)
                print("   💡 HINT: This is a 'Bad Request' error, often caused by an invalid combination of 'breakdowns' or incompatible 'fields' and 'breakdowns'. Please check the Facebook API documentation for valid combinations.", file=sys.stderr)
            else:
                print(f"   Response: {e.response.text}", file=sys.stderr)
            
            # Raise the exception up to the caller so it can be caught and handled.
            raise e
        except requests.exceptions.RequestException as e:
            print(f"❌ Network or request error: {e}", file=sys.stderr)
            raise e


def pull_metadata() -> pd.DataFrame:
    """Pulls ad-level metadata (ID, name, status, creative, etc.)."""
    print("➡️  Fetching ad metadata...")
    fields = [
        "id", "name", "status", "effective_status",
        "created_time", "updated_time",
        "adset{id,name}", "campaign{id,name}",
        "creative{id,name,thumbnail_url,effective_object_story_id}"
    ]
    url = f"{BASE_URL}/{API_VER}/{ACCOUNT}/ads"
    params = {"fields": ",".join(fields), "limit": 500}
    data = list(paginate(url, params))
    return pd.DataFrame(data) if data else pd.DataFrame()


def pull_insights(level: str, breakdowns: List[str] = None) -> pd.DataFrame:
    """
    Pulls performance insights for a given level, with optional breakdowns.
    """
    action = f"Fetching {level}-level insights"
    if breakdowns:
        action += f" with breakdowns: {breakdowns}"
    print(f"➡️  {action}...")

    fields = [
        "date_start", "date_stop",
        "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name",
        "impressions", "reach", "clicks", "spend", "cpc", "ctr", "cpm"
    ]
    url = f"{BASE_URL}/{API_VER}/{ACCOUNT}/insights"
    
    params = {
        "level": level,
        "date_preset": f"last_{LOOKBACK}d",
        "fields": ",".join(fields),
        "limit": 500
    }

    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)
    
    data = list(paginate(url, params))
    return pd.DataFrame(data) if data else pd.DataFrame()


def save_dataframe(df: pd.DataFrame, filename: str):
    """Saves a DataFrame to a CSV file if it's not empty."""
    if not df.empty:
        path = os.path.join(OUT_DIR, filename)
        df.to_csv(path, index=False)
        print(f"✅ Wrote {len(df)} rows to {path}")
    else:
        print(f"⚠️ No data returned for {filename}; file not created.")


def main():
    """
    Runs the full data extraction process with resilient error handling for each report.
    """
    print("🚀 Starting Facebook Ads data pull...")
    os.makedirs(OUT_DIR, exist_ok=True)

    # --- Report 1: Core Metadata ---
    try:
        print("\n--- Pulling Core Metadata ---")
        meta_df = pull_metadata()
        save_dataframe(meta_df, "facebook_ads_meta.csv")
    except Exception as e:
        print(f"🔴 FAILED to pull core metadata. Error: {e}", file=sys.stderr)

    # --- Report 2: High-Level Insights (No Breakdowns) ---
    try:
        print("\n--- Pulling High-Level Insights (No Breakdowns) ---")
        ad_df = pull_insights("ad")
        save_dataframe(ad_df, "facebook_ads_insights_summary.csv")
    except Exception as e:
        print(f"🔴 FAILED to pull high-level insights. Error: {e}", file=sys.stderr)
    
    # --- Report 3: Single Breakdown Example ---
    try:
        print("\n--- Pulling Insights by a Single Breakdown ---")
        device_breakdown = ["device_platform"]
        insights_by_device_df = pull_insights("ad", breakdowns=device_breakdown)
        save_dataframe(insights_by_device_df, "facebook_ads_insights_by_device.csv")
    except Exception as e:
        print(f"🔴 FAILED to pull insights by device. The 'device_platform' breakdown may be invalid for your account. Error: {e}", file=sys.stderr)

    # --- Report 4: Combined Breakdown Example ---
    try:
        print("\n--- Pulling Insights by a Combined Breakdown ---")
        placement_breakdown = ["publisher_platform", "platform_position"]
        insights_by_placement_df = pull_insights("ad", breakdowns=placement_breakdown)
        save_dataframe(insights_by_placement_df, "facebook_ads_insights_by_placement.csv")
    except Exception as e:
        print(f"🔴 FAILED to pull insights by placement. The combination of 'publisher_platform' and 'platform_position' may be invalid for your account. Error: {e}", file=sys.stderr)

    print("\n✨ Data pull process finished.")


if __name__ == "__main__":
    main()
