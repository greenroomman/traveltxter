import re

def _digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or ""))

def norm_date_ddmmyy(value: str) -> str:
    d = _digits(value)
    if len(d) == 6:
        return d
    if len(d) == 8:
        if d.startswith("20") or d.startswith("19"):
            yyyy, mm, dd = d[0:4], d[4:6], d[6:8]
            return f"{dd}{mm}{yyyy[2:4]}"
        dd, mm, yyyy = d[0:2], d[2:4], d[4:8]
        return f"{dd}{mm}{yyyy[2:4]}"
    return d[:6].ljust(6, "-")

def norm_price_3digits(value: str) -> str:
    d = _digits(value)
    return d[:3] if d else "---"
def norm_price_3digits(value: str) -> str:
    """
    Converts price to whole pounds, max 3 digits.
    Examples:
      £54.99  -> 54
      129.00  -> 129
      £1,249  -> 124
    """
    s = str(value).strip()

    # If there's a decimal point, take pounds only
    if "." in s:
        s = s.split(".")[0]

    # Remove anything that's not a digit
    digits = re.sub(r"\D", "", s)

    return digits[:3] if digits else "---"

def looks_like_airport_code(s: str) -> bool:
    s = str(s or "").strip().upper()
    return bool(re.fullmatch(r"[A-Z]{3}", s))

def norm_city_full(value: str, field_name: str) -> str:
    s = str(value or "").strip()
    if not s:
        raise ValueError(f"{field_name} missing")
    if looks_like_airport_code(s):
        raise ValueError(f"{field_name} looks like an airport code ({s}); must be full city name")
    return s.upper()

def normalise_deal_for_render(deal: dict) -> dict:
    out = dict(deal)

    out["origin_city"] = norm_city_full(out.get("origin_city"), "origin_city")
    out["destination_city"] = norm_city_full(out.get("destination_city"), "destination_city")
    out["outbound_date"] = norm_date_ddmmyy(out.get("outbound_date"))
    out["return_date"] = norm_date_ddmmyy(out.get("return_date"))
    out["price_gbp"] = norm_price_3digits(out.get("price_gbp"))

    return out
