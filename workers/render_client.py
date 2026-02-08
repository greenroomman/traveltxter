#!/usr/bin/env bash
set -euo pipefail

BASE="https://greenroomman.pythonanywhere.com"
ENDPOINT="$BASE/api/render"

echo "Testing health..."
curl -sS "$BASE/api/health" || curl -sS "$BASE/health"
echo
echo "----------------------------------------"

test_render () {
  local layout="$1"
  local theme="$2"
  local from_city="$3"
  local to_city="$4"
  local out_date="$5"
  local in_date="$6"
  local price="$7"
  
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
      \"theme\":\"$theme\"
    }"
  
  echo
  echo "----------------------------------------"
}

test_render "AM" "northern_lights" "London" "Keflavik" "120326" "180326" "£159"
test_render "PM" "northern_lights" "London" "Keflavik" "120326" "180326" "£159"
