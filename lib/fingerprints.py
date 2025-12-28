import hashlib

def deal_fingerprint(origin_city, destination_city, outbound_date, return_date, airline, stops):
    raw = f"{origin_city}|{destination_city}|{outbound_date}|{return_date}|{airline}|{stops}".lower()
    return hashlib.md5(raw.encode("utf-8")).hexdigest()
