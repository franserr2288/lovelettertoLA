import datetime as dt

def get_today_str():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

def get_time_stamp():
    return dt.datetime.now(dt.timezone.utc).timestamp()
    