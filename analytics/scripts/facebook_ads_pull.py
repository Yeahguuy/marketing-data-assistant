import os, sys, time, requests, pandas as pd
from datetime import date, timedelta

API_VER = os.getenv("FB_API_VER", "v20.0")
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")  # e.g. act_9018283094917961
DAYS = int(os.getenv("FB_LOOKBACK_DAYS", "7"))
OUT_FILE = os.getenv("FB_OUT_FILE", "analytics/dataprocessed/facebook_live_ads.csv")

def _fail(msg, code=1):
    print(f"[ERROR] {msg}"); sys.exit(code)

def _check_env():
    miss=[]
    if not ACCESS_TOKEN: miss.append("FB_ACCESS_TOKEN")
    if not AD_ACCOUNT_ID: miss.append("FB_AD_ACCOUNT_ID")
    if miss: _fail("Missing env vars: " + ", ".join(miss))

def _time_range(days):
    since=(date.today()-timedelta(days=days)).isoformat()
    until=date.today().isoformat()
    return since, until

def fetch_ads(days=7):
    since, until = _time_range(days)
    url=f"https://graph.facebook.com/{API_VER}/{AD_ACCOUNT_ID}/ads"
    params={
        "access_token": ACCESS_TOKEN,
        "limit": 200,
        "fields": ",".join([
            "ad_id","name","adset_id","adset_name","campaign_id","campaign_name",
            "creative{title,body,object_story_spec}",
            f"insights.time_range({{since:'{since}',until:'{until}'}})"
            "{{impressions,clicks,unique_clicks,spend,cpc,ctr,cpm,reach}}"
        ])
    }
    rows=[]; next_url=url; tries=0
    while next_url:
        r=requests.get(next_url, params=params if next_url==url else {})
        if r.status_code>=400:
            tries+=1
            if tries<=3: time.sleep(2*tries); continue
            _fail(f"Facebook API error {r.status_code}: {r.text[:400]}")
        payload=r.json()
        for ad in payload.get("data", []):
            insights=(ad.get("insights") or {}).get("data") or [{}]
            creative=ad.get("creative") or {}
            for i in insights:
                rows.append({
                    "date_start": i.get("date_start"),
                    "date_stop":  i.get("date_stop"),
                    "ad_id": ad.get("ad_id"),
                    "ad_name": ad.get("name"),
                    "adset_id": ad.get("adset_id"),
                    "adset_name": ad.get("adset_name"),
                    "campaign_id": ad.get("campaign_id"),
                    "campaign_name": ad.get("campaign_name"),
                    "headline": creative.get("title"),
                    "body": creative.get("body"),
                    "impressions": i.get("impressions"),
                    "reach": i.get("reach"),
                    "clicks": i.get("clicks"),
                    "unique_clicks": i.get("unique_clicks"),
                    "spend": i.get("spend"),
                    "cpc": i.get("cpc"),
                    "ctr": i.get("ctr"),
                    "cpm": i.get("cpm"),
                })
        next_url=(payload.get("paging") or {}).get("next")
    df=pd.DataFrame(rows)
    for c in ["impressions","reach","clicks","unique_clicks"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce")
    for c in ["spend","cpc","ctr","cpm"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce")
    return df

def main():
    _check_env()
    df=fetch_ads(DAYS)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    df.to_csv(OUT_FILE, index=False)
    print(f"[OK] Wrote {len(df)} rows to {OUT_FILE}")

if __name__=="__main__":
    main()
