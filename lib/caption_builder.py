def build_caption(deal: dict) -> str:
    # Matches your board aesthetic: factual + calm + values-led, no hype.
    origin = deal.get("origin_city", "").strip()
    dest = deal.get("destination_city", "").strip()
    out_date = deal.get("outbound_date", "").strip()
    ret_date = deal.get("return_date", "").strip()
    price = deal.get("price_gbp", "").strip()

    lines = [
        f"FROM {origin} → TO {dest}",
        f"IN {out_date} · RETURN {ret_date} · PRICE £{price}",
        "",
        "Adventure for less — travel thoughtfully.",
        "Tip: fewer trips, longer stays."
    ]
    return "\n".join(lines)
