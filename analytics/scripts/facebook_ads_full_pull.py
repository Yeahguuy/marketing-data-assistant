import os, sys, time, json, requests, pandas as pd

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
        time.sleep(0.2)

def flatten_creative(ad):
    out = {
        "ad_id": ad.get("id"),
        "ad_name": ad.get("name"),
        "status": ad.get("status"),
        "effective_status": ad.get("effective_status"),
        "created_time": ad.get("created_time"),
        "updated_time": ad.get("updated_time"),
        "adset_id": (ad.get("adset") or {}).get("id"),
        "adset_name": (ad.get("adset") or {}).get("name"),
        "campaign_id": (ad.get("campaign") or {}).get("id"),
        "campaign_name": (ad.get("campaign") or {}).get("name"),
    }
    creative = ad.get("creative") or {}
    out["creative_id"]   = creative.get("id")
    out["creative_name"] = creative.get("name")
    out["thumbnail_url"] = creative.get("thumbnail_url")
    out["effective_object_story_id"] = creative.get("effective_object_story_id")
    oss = creative.get("object_story_spec") or {}
    link = oss.get("link_data") or {}
    out["primary_text"]  = oss.get("message") or link.get("message")
    out["headline"]      = link.get("name")
    out["description"]   = link.get("description")
    out["display_link"]  = link.get("link")
    out["caption"]       = link.get("caption")
    out["call_to_action_type"] = (link.get("call_to_action") or {}).get("type")
    if "child_attachments" in link:
        children = link["child_attachments"]
        out["carousel_json"] = json.dumps(children)
        if children:
            out["carousel_headline_first"] = children[0].get("name")
            out["carousel_desc_first"]     = children[0].get("description")
    video_data = oss.get("video_data") or {}
    out["video_description"]         = video_data.get("description")
    out["video_call_to_action_type"] = (video_data.get("call_to_action") or {}).get("type")
    return out

def pull_ads_metadata():
    fields = ",".join([
        "id","name","status","effective_status",
        "created_time","updated_time",
        "adset{id,name}","campaign{id,name}",
        "creative{id,name,thumbnail_url,effective_object_story_id,object_story_spec}"
    ])
    url = f"https://graph.facebook.com/{API_VER}/{ACCOUNT}/ads"
    params = {"fields": fields, "limit": 500}
    rows = [flatten_creative(ad) for ad in paginate(url, params)]
    return pd.DataFrame(rows)

INSIGHT_FIELDS = ",".join([
    "date_start","date_stop",
    "campaign_id","campaign_name",
    "adset_id","adset_name",
    "ad_id","ad_name",
    "impressions","reach",
    "clicks","unique_clicks",
    "inline_link_clicks","spend","cpc","ctr","cpm"
])

def pull_insights(level="ad", breakdowns=None):
    url = f"https://graph.facebook.com/{API_VER}/{ACCOUNT}/insights"
    params = {
        "level": level,
        "date_preset": f"last_{LOOKBACK}d",
        "fields": INSIGHT_FIELDS,
        "limit": 5000
    }
    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)
    return pd.DataFrame(list(paginate(url, params)))

def safe_pull(level, label, breakdowns=None):
    """Call pull_insights and catch any errors so the script doesn't exit."""
    try:
        df = pull_insights(level=level, breakdowns=breakdowns)
        filename = f"facebook_{level}_insights"
        if breakdowns:
            filename += "_" + "_".join(breakdowns)
        filename += ".csv"
        outpath = os.path.join(OUT_DIR, filename)
        df.to_csv(outpath, index=False)
        print(f"Wrote {len(df)} rows to {outpath}")
    except RuntimeError as err:
        print(f"Skipping {level} {breakdowns}: {err}")

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # creative metadata
    meta_df = pull_ads_metadata()
    meta_df.to_csv(os.path.join(OUT_DIR, "facebook_ads_meta.csv"), index=False)
    print(f"Wrote {len(meta_df)} rows to facebook_ads_meta.csv")

    # ad-level metrics (no breakdown)
    safe_pull("ad", "aggregate")

    # ad-level placement breakdowns
    for bd in [["device_platform"], ["platform_position"], ["publisher_platform"]]:
        safe_pull("ad", "_".join(bd), breakdowns=bd)

    # adset-level metrics
    safe_pull("adset", "aggregate")

    # adset-level demographics
    for bd in [["age"], ["gender"], ["country"], ["dma"], ["region"]]:
        safe_pull("adset", "_".join(bd), breakdowns=bd)

if __name__ == "__main__":
    main()
