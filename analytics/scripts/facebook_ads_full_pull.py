import os, sys, requests, pandas as pd

TOKEN   = os.getenv("FB_ACCESS_TOKEN")
ACCOUNT = os.getenv("FB_AD_ACCOUNT_ID")
API_VER = os.getenv("FB_API_VER", "v20.0")
LOOKBACK = int(os.getenv("FB_LOOKBACK_DAYS", "7"))
OUT_DIR  = "analytics/dataprocessed"

if not TOKEN or not ACCOUNT:
    sys.exit("Missing env vars")

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {TOKEN}"})

def paginate(url, params=None):
    params = params or {}
    while True:
        resp = session.get(url, params=params, timeout=90)
        if not resp.ok:
            raise RuntimeError(f"{resp.status_code}: {resp.text}")
        payload = resp.json()
        for row in payload.get("data", []):
            yield row
        next_url = payload.get("paging", {}).get("next")
        if not next_url:
            break
        url, params = next_url, {}

def pull_metadata():
    fields = "id,name,status,effective_status,created_time,updated_time,adset{id,name},campaign{id,name},creative{id,name,thumbnail_url,effective_object_story_id,object_story_spec}"
    url = f"https://graph.facebook.com/{API_VER}/{ACCOUNT}/ads"
    rows = list(paginate(url, {"fields": fields, "limit": 500}))
    return pd.DataFrame(rows)

def pull_insights(level):
    fields = "date_start,date_stop,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,reach,clicks,unique_clicks,inline_link_clicks,spend,cpc,ctr,cpm"
    url = f"https://graph.facebook.com/{API_VER}/{ACCOUNT}/insights"
    params = {
        "level": level,
        "date_preset": f"last_{LOOKBACK}d",
        "fields": fields,
        "limit": 5000,
    }
    rows = list(paginate(url, params))
    return pd.DataFrame(rows)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    meta = pull_metadata()
    meta.to_csv(os.path.join(OUT_DIR, "facebook_ads_meta.csv"), index=False)
    print(f"Wrote {len(meta)} rows to facebook_ads_meta.csv")

    ad_insights = pull_insights("ad")
    ad_insights.to_csv(os.path.join(OUT_DIR, "facebook_ads_insights.csv"), index=False)
    print(f"Wrote {len(ad_insights)} rows to facebook_ads_insights.csv")

    adset_insights = pull_insights("adset")
    adset_insights.to_csv(os.path.join(OUT_DIR, "facebook_adset_insights.csv"), index=False)
    print(f"Wrote {len(adset_insights)} rows to facebook_adset_insights.csv")

if __name__ == "__main__":
    main()
