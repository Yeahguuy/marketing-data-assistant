import os
import sys
import requests
import pandas as pd
from typing import Iterator, Dict, Any

# --- Configuration ---
# Fetches configuration from environment variables.
try:
    TOKEN = os.environ["FB_ACCESS_TOKEN"]
    ACCOUNT = os.environ["FB_AD_ACCOUNT_ID"]
except KeyError as e:
    sys.exit(f"❌ Error: Missing required environment variable: {e}")

# Default to a recent, valid API version; v23.0 is too far in the future.
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

        except requests.exceptions.RequestException as e:
            print(f"API request failed. URL: {resp.request.url}\nError: {e}", file=sys.stderr)
            return  # Stop iteration on error.
        except requests.exceptions.JSONDecodeError:
            print(f"Failed to decode JSON. URL: {resp.request.url}\nResponse: {resp.text}", file=sys.stderr)
            return  # Stop iteration on error.


def pull_metadata() -> pd.DataFrame:
    """Pulls ad-level metadata (ID, name, status, creative, etc.)."""
    fields = [
        "id", "name", "status", "effective_status",
        "created_time", "updated_time",
        "adset{id,name}", "campaign{id,name}",
        "creative{id,name,thumbnail_url,effective_object_story_id,object_story_spec}"
    ]
    url = f"{BASE_URL}/{API_VER}/{ACCOUNT}/ads"
    params = {"fields": ",".join(fields), "limit": 500}
    data = list(paginate(url, params))
    # Return a DataFrame, even if it's empty.
    return pd.DataFrame(data) if data else pd.DataFrame()


def pull_insights(level: str) -> pd.DataFrame:
    """Pulls performance insights for a given level ('ad' or 'adset')."""
    fields = [
        "date_start", "date_stop",
        "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name",
        "impressions", "reach", "clicks", "unique_clicks",
        "inline_link_clicks", "spend", "cpc", "ctr", "cpm"
    ]
    url = f"{BASE_URL}/{API_VER}/{ACCOUNT}/insights"
    params = {
        "level": level,
        "date_preset": f"last_{LOOKBACK}d",
        "fields": ",".join(fields),
        "limit": 500  # A limit of 500 is generally safer and more reliable than 5000.
    }
    data = list(paginate(url, params))
    return pd.DataFrame(data) if data else pd.DataFrame()


def main():
    """Runs the full data extraction process."""
    print("🚀 Starting Facebook Ads data pull...")
    os.makedirs(OUT_DIR, exist_ok=True)

    # --- Metadata Pull ---
    print("\nPulling ads metadata...")
    meta_df = pull_metadata()
    if not meta_df.empty:
        path = os.path.join(OUT_DIR, "facebook_ads_meta.csv")
        meta_df.to_csv(path, index=False)
        print(f"✅ Wrote {len(meta_df)} rows to {path}")
    else:
        print("⚠️ No metadata was returned from the API.")

    # --- Ad Insights Pull ---
    print("\nPulling ad-level insights...")
    ad_df = pull_insights("ad")
    if not ad_df.empty:
        path = os.path.join(OUT_DIR, "facebook_ads_insights.csv")
        ad_df.to_csv(path, index=False)
        print(f"✅ Wrote {len(ad_df)} rows to {path}")
    else:
        print("⚠️ No ad-level insights were returned from the API.")

    # --- Adset Insights Pull ---
    print("\nPulling adset-level insights...")
    adset_df = pull_insights("adset")
    if not adset_df.empty:
        path = os.path.join(OUT_DIR, "facebook_adset_insights.csv")
        adset_df.to_csv(path, index=False)
        print(f"✅ Wrote {len(adset_df)} rows to {path}")
    else:
        print("⚠️ No adset-level insights were returned from the API.")

    print("\n✨ Data pull complete.")


if __name__ == "__main__":
    main()
