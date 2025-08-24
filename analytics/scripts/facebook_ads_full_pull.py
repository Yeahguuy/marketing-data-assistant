import os
import sys
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
    Handles pagination for the Facebook Graph API.

    This generator function yields each item from the 'data' array in an API
    response and automatically follows the 'next' link for subsequent pages.
    It includes robust error handling for network and API issues.
    """
    current_url = url
    current_params = params or {}
    while True:
        try:
            resp = session.get(current_url, params=current_params, timeout=90)
            # Raise an exception for HTTP errors (e.g., 400, 401, 500).
            resp.raise_for_status()
            payload = resp.json()

            # Yield each record from the current page's data.
            yield from payload.get("data", [])

            # Get the full URL for the next page.
            next_url = payload.get("paging", {}).get("next")
            if not next_url:
                break  # No more pages.

            # The 'next_url' contains all necessary parameters, so we use it directly
            # and clear the params dictionary for the next loop.
            current_url = next_url
            current_params = {}

        except requests.exceptions.HTTPError as e:
            # Catches specific API errors (like the 'breakdowns' error) and prints details.
            print(f"❌ API Error on request to {e.response.url}:\n   {e.response.status_code}: {e.response.text}", file=sys.stderr)
            return # Stop iteration on error.
        except requests.exceptions.RequestException as e:
            print(f"❌ Network or request error: {e}", file=sys.stderr)
            return
        except requests.exceptions.JSONDecodeError:
            print(f"❌ Failed to decode JSON from response: {resp.text}", file=sys.stderr)
            return


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
    
    Args:
        level (str): The level to pull data for ('ad', 'adset', or 'campaign').
        breakdowns (List[str], optional): A list of valid breakdown values.
                                          Defaults to None (no breakdowns).
    """
    if breakdowns:
        print(f"➡️  Fetching {level}-level insights with breakdowns: {breakdowns}...")
    else:
        print(f"➡️  Fetching {level}-level insights...")

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

    # IMPORTANT: Only add the 'breakdowns' parameter if it's actually provided.
    # This prevents the API error you were seeing.
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
    """Runs the full data extraction process."""
    print("🚀 Starting Facebook Ads data pull...")
    os.makedirs(OUT_DIR, exist_ok=True)

    # --- Pull Metadata ---
    meta_df = pull_metadata()
    save_dataframe(meta_df, "facebook_ads_meta.csv")

    # --- Pull Insights (No Breakdowns by Default) ---
    # By default, no breakdowns are requested to prevent errors.
    ad_df = pull_insights("ad")
    save_dataframe(ad_df, "facebook_ads_insights.csv")
    
    adset_df = pull_insights("adset")
    save_dataframe(adset_df, "facebook_adset_insights.csv")

    # --- EXAMPLE: Pull Insights WITH Breakdowns ---
    # The error you are seeing is caused by activating this section with an invalid value.
    # To fix it, ensure every value in the 'breakdown_values' list is valid.
    #
    # VALID VALUES INCLUDE: 'age', 'gender', 'country', 'region', 'device_platform', 'publisher_platform'
    # See the error log for the complete list of allowed values.
    #
    # print("\n🚀 Starting pull for data with breakdowns...")
    # breakdown_values = ["device_platform", "publisher_platform"] # This is a valid example
    # ad_breakdown_df = pull_insights("ad", breakdowns=breakdown_values)
    # save_dataframe(ad_breakdown_df, "facebook_ads_insights_by_platform.csv")

    print("\n✨ Data pull complete.")


if __name__ == "__main__":
    main()
