from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import os
import pandas as pd

# required environment variables:
# FB_APP_ID, FB_APP_SECRET, FB_ACCESS_TOKEN, FB_AD_ACCOUNT_ID
app_id     = os.getenv("FB_APP_ID")
app_secret = os.getenv("FB_APP_SECRET")
access_token = os.getenv("FB_ACCESS_TOKEN")
account_id  = os.getenv("FB_AD_ACCOUNT_ID")  # format "act_1234567890"
api_version  = os.getenv("FB_API_VER", "v23.0")  # default to v23.0
lookback     = int(os.getenv("FB_LOOKBACK_DAYS", "7"))

if not app_id or not app_secret or not access_token or not account_id:
    raise RuntimeError("Missing one or more Facebook credentials.")

# initialise the Facebook API
FacebookAdsApi.init(app_id, app_secret, access_token, api_version)

account = AdAccount(account_id)
OUT_DIR = "analytics/dataprocessed"
os.makedirs(OUT_DIR, exist_ok=True)

def cursor_to_df(cursor):
    """Convert a FB SDK cursor to a pandas DataFrame."""
    return pd.DataFrame(list(cursor))

# 1) Ad creative metadata (campaign, adset, creative details)
meta_fields = [
    "id", "name", "effective_status", "status",
    "created_time", "updated_time",
    "adset_id", "adset_name",
    "campaign_id", "campaign_name",
    "creative",
]
ads_cursor = account.get_ads(fields=meta_fields)
ads_df = cursor_to_df(ads_cursor)
ads_df.to_csv(os.path.join(OUT_DIR, "facebook_ads_meta.csv"), index=False)
print(f"Wrote {len(ads_df)} rows to facebook_ads_meta.csv")

# 2) Aggregate ad‑level insights (last N days)
ad_fields = [
    "date_start", "date_stop",
    "ad_id", "ad_name", "adset_id", "adset_name", "campaign_id", "campaign_name",
    "impressions", "reach", "clicks", "unique_clicks",
    "inline_link_clicks", "spend", "cpc", "ctr", "cpm",
]
ad_params = {
    "level": "ad",
    "date_preset": f"last_{lookback}d",
}
ad_insights = account.get_insights(fields=ad_fields, params=ad_params)
ad_insights_df = cursor_to_df(ad_insights)
ad_insights_df.to_csv(os.path.join(OUT_DIR, "facebook_ads_insights.csv"), index=False)
print(f"Wrote {len(ad_insights_df)} rows to facebook_ads_insights.csv")

# 3) Aggregate ad‑set‑level insights (last N days)
adset_fields = [
    "date_start", "date_stop",
    "adset_id", "adset_name", "campaign_id", "campaign_name",
    "impressions", "reach", "clicks", "unique_clicks",
    "spend", "cpc", "ctr", "cpm",
]
adset_params = {
    "level": "adset",
    "date_preset": f"last_{lookback}d",
}
adset_insights = account.get_insights(fields=adset_fields, params=adset_params)
adset_insights_df = cursor_to_df(adset_insights)
adset_insights_df.to_csv(os.path.join(OUT_DIR, "facebook_adset_insights.csv"), index=False)
print(f"Wrote {len(adset_insights_df)} rows to facebook_adset_insights.csv")
