# analytics/scripts/facebook_ads_full_pull.py
import os, sys, time, json, requests, pandas as pd

TOKEN = os.environ.get("FB_ACCESS_TOKEN")
ACCOUNT = os.environ.get("FB_AD_ACCOUNT_ID")          # act_###########
API_VER = os.environ.get("FB_API_VER", "v20.0")
LOOKBACK = int(os.environ.get("FB_LOOKBACK_DAYS", "7"))
OUT_DIR = "analytics/dataprocessed"

if not TOKEN or not ACCOUNT:
    sys.exit("Missing env vars: FB_ACCESS_TOKEN, FB_AD_ACCOUNT_ID")

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {TOKEN}"})

def paginate(url, params=None):
    params = params or {}
    while True:
        r = session.get(url, params=params, timeout=90)
        if not r.ok:
            sys.exit(f"[ERROR] {url} -> {r.status_code}: {r.text}")
        data = r.json()
        for row in data.get("data", []):
            yield row
        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        url, params = next_url, {}

def flatten_creative(row):
    """Flatten creative headline, primary text, description, CTA, link, media"""
    out = {
        "ad_id": row.get("id"),
        "ad_name": row.get("name"),
        "ad_status": row.get("status"),
        "ad_effective_status": row.get("effective_status"),
        "ad_created_time": row.get("created_time"),
        "ad_updated_time": row.get("updated_time"),
        "adset_id": (row.get("adset") or {}).get("id"),
        "adset_name": (row.get("adset") or {}).get("name"),
        "campaign_id": (row.get("campaign") or {}).get("id"),
        "campaign_name": (row.get("campaign") or {}).get("name"),
    }
    cr = row.get("creative") or {}
    out["creative_id"] = cr.get("id")
    out["creative_name"] = cr.get("name")
    out["thumbnail_url"] = cr.get("thumbnail_url")
    out["effective_object_story_id"] = cr.get("effective_object_story_id")

    # object_story_spec → link_data/message variations
    oss = cr.get("object_story_spec") or {}
    link = (oss.get("link_data") or {})
    out["primary_text"] = oss.get("message") or link.get("message")
    out["headline"] = link.get("name")                    # headline
    out["description"] = link.get("description")
    out["display_link"] = link.get("link")
    out["caption"] = link.get("caption")
    cta = (link.get("call_to_action") or {}).get("type")
    out["call_to_action_type"] = cta

    # image or video
    out["image_hash"] = link.get("image_hash")
    if "child_attachments" in link:
        # carousels… take first as representative, also store JSON
        ca = link["child_attachments"][0] if link["child_attachments"] else {}
        out["carousel_headline_first"] = ca.get("name")
        out["carousel_desc_first"] = ca.get("description")
        out["carousel_json"] = json.dumps(link["child_attachments"])
    video_data = (oss.get("video_data") or {})
    out["video_description"] = video_data.get("description")
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
    rows = []
    for ad in paginate(url, params):
        rows.append(flatten_creative(ad))
    df = pd.DataFrame(rows)
    return df

def pull_insights(level="ad", preset_days=LOOKBACK, breakdowns=None):
    fields = ",".join([
        "date_start","date_stop",
        "campaign_id","campaign_name",
        "adset_id","adset_name",
        "ad_id","ad_name",
        "impressions","reach","clicks","unique_clicks",
        "inline_link_clicks","spend","cpc","ctr","cpm"
    ])
    url = f"https://graph.facebook.com/{API_VER}/{ACCOUNT}/insights"
    params = {
        "level": level,
        "date_preset": f"last_{preset_days}d",
        "fields": fields,
        "limit": 5000
    }
    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)
    rows = list(paginate(url, params))
    return pd.DataFrame(rows)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # 1) Ad metadata + creative
    meta_df = pull_ads_metadata()
    meta_path = os.path.join(OUT_DIR, "facebook_ads_meta.csv")
    meta_df.to_csv(meta_path, index=False)
    print(f"Wrote {len(meta_df)} rows to {meta_path}")

    # 2) Insights (ad level)
    ins_df = pull_insights()
    ins_path = os.path.join(OUT_DIR, "facebook_ads_insights.csv")
    ins_df.to_csv(ins_path, index=False)
    print(f"Wrote {len(ins_df)} rows to {ins_path}")

    # 3) Optional breakdowns… daily and placement
    daily_df = pull_insights(breakdowns=["date"])
    daily_path = os.path.join(OUT_DIR, "facebook_ads_insights_daily.csv")
    daily_df.to_csv(daily_path, index=False)
    print(f"Wrote {len(daily_df)} rows to {daily_path}")

    place_df = pull_insights(
        breakdowns=["publisher_platform","platform_position","device_platform"]
    )
    place_path = os.path.join(OUT_DIR, "facebook_ads_insights_placement.csv")
    place_df.to_csv(place_path, index=False)
    print(f"Wrote {len(place_df)} rows to {place_path}")

if __name__ == "__main__":
    main()
