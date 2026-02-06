#!/usr/bin/env bash
set -euo pipefail

BASE="https://greenroomman.pythonanywhere.com"
ENDPOINT="$BASE/api/render"   # <-- if your PA mount is /api
# ENDPOINT="$BASE/render"     # <-- if NOT mounted under /api

echo "Testing health..."
curl -sS "$BASE/api/health" || curl -sS "$BASE/health"
echo
echo "----------------------------------------"

test_render () {
  local layout="$1"
  local theme="$2"
  local from_city="$3"
  local to_city="$4"
  local out_date="$5"   # ddmmyy
  local in_date="$6"    # ddmmyy
  local price="$7"      # e.g. £159

  echo "Rendering layout=$layout theme=$theme $from_city -> $to_city $out_date/$in_date $price"

  curl -sS -X POST "$ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "{
      \"TO\":\"$to_city\",
      \"FROM\":\"$from_city\",
      \"OUT\":\"$out_date\",
      \"IN\":\"$in_date\",
      \"PRICE\":\"$price\",
      \"layout\":\"$layout\",
test_render "PM" "northern_lights" "London" "Keflavik" "120326" "180326" "£159"
Testing health...
{"ok":true}

----------------------------------------
Rendering layout=AM theme=northern_lights London -> Keflavik 120326/180326 £159
{
    "graphic_url": "https://greenroomman.pythonanywhere.com/static/renders/deal_london_keflavik_120326_180326_260206_082946_8dcb123c.png",
    "layout": "AM",
    "ok": true,
    "theme": "northern_lights"
}
----------------------------------------
Rendering layout=PM theme=northern_lights London -> Keflavik 120326/180326 £159
{
    "graphic_url": "https://greenroomman.pythonanywhere.com/static/renders/deal_london_keflavik_120326_180326_260206_082947_bda590a0.png",
    "layout": "PM",
    "ok": true,
    "theme": "northern_lights"
}
-------------
