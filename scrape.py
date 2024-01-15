from goes2go import GOES
import datetime
import pandas as pd
import requests
import json
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


G = GOES(product="ABI-L2-MCMIPC", satellite="noaa-goes16", domain="C")

URL = "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lng}&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,visibility,wind_speed_10m,wind_direction_10m,temperature_80m,soil_temperature_0cm,soil_moisture_0_to_1cm"

df = pd.read_csv("sample_cities.csv").sample(frac=1.0)
print(df)


def snapshot():
    now = datetime.datetime.now()
    dt = now.isoformat()[:16]
    path = f"F:\goes2go\snapshots\\" + dt.replace(":", "-") + ".jsonl"
    for _, row in df.iterrows():
        lat, lng = row["lat"], row["lng"]
        url = URL.format(lat=lat, lng=lng)
        print(url)
        r = requests.get(url)
        r_json = r.json()
        r_json["_dt"] = dt
        r_json["_ts"] = datetime.datetime.now().timestamp()
        r_json["_city_ascii"] = row["city_ascii"]
        with open(path, "a") as f:
            f.write(json.dumps(r_json))
            f.write("\n")

    ds = G.nearesttime(attime=now)
    print(dt, ds.path)


# snapshot()

scheduler = BlockingScheduler()
scheduler.add_job(snapshot, CronTrigger(minute=0), hours=1)
scheduler.start()
