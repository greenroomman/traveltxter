import hashlib


def deal_fingerprint(
    origin_city: str,
    destination_city: str,
    outbound_date: str,
    return_date: str,
    airline: str,
    stops: str,
) -> str:
    raw = f"{origin_city}|{destination_city}|{outbound_date}|{return_date}|{airline}|{stops}".lower().strip()
    return hashlib.md5(raw.encode("utf-8")).hexdigest()
