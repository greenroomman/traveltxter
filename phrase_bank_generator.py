#!/usr/bin/env python3
"""
TravelTxter V5 - Phrase Bank Generator
Purpose: Generate editorial phrase candidates for destinations lacking PHRASE_BANK coverage
Contract: Reads CONFIG + PHRASE_BANK, writes to PHRASE_BANK_CANDIDATES only
Human loop: Richard reviews candidates → manually approves → moves to PHRASE_BANK
"""

import os
import sys
from datetime import datetime, timezone
from openai import OpenAI
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================================
# CONFIGURATION
# ============================================================================

REQUIRED_SECRETS = [
    'OPENAI_API_KEY',
    'OPENAI_MODEL',
    'GCP_SA_JSON',
    'SPREADSHEET_ID'
]

SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

# Sheet tab names
CONFIG_TAB = 'CONFIG'
PHRASE_BANK_TAB = 'PHRASE_BANK'
CANDIDATES_TAB = 'PHRASE_BANK_CANDIDATES'
IATA_MASTER_TAB = 'IATA_MASTER'

# Voice guidance (locked)
VOICE_GUIDE = """
TravelTxter editorial voice (Huckberry tone):
- Conversational, knowledgeable, anti-tourist
- Avoid hype language: "hidden gem", "bucket list", "must-see", "paradise"
- Reference specific seasonal, cultural, or geographical hooks
- Length: 8-15 words maximum
- Factual observations, not marketing claims
- Well-travelled friend sharing an insight, not a guidebook selling a destination

Good examples:
- "Kyoto's maple tunnels turn red in November, and tourists vanish by December"
- "Iceland's winter geothermal pools while everyone else is in Bali"
- "Porto's port lodges are empty on weekday mornings in February"

Bad examples:
- "Discover the hidden gems of magical Santorini!" (hype, generic)
- "A bucket list destination you absolutely must visit" (cliché, pushy)
- "Paradise awaits in this stunning location" (marketing nonsense)
"""

# ============================================================================
# HELPERS
# ============================================================================

def check_secrets():
    """Verify all required secrets are present"""
    missing = [s for s in REQUIRED_SECRETS if not os.getenv(s)]
    if missing:
        print(f"❌ Missing secrets: {', '.join(missing)}")
        sys.exit(1)
    print("✅ All secrets present")

def init_sheets():
    """Initialize Google Sheets client"""
    try:
        creds_json = os.getenv('GCP_SA_JSON')
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            eval(creds_json), SCOPE
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_key(os.getenv('SPREADSHEET_ID'))
        print("✅ Google Sheets authorized")
        return sheet
    except Exception as e:
        print(f"❌ Sheets auth failed: {e}")
        sys.exit(1)

def init_openai():
    """Initialize OpenAI client"""
    try:
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        print("✅ OpenAI client initialized")
        return client
    except Exception as e:
        print(f"❌ OpenAI init failed: {e}")
        sys.exit(1)

def get_iata_mapping(sheet):
    """Build IATA → city/country lookup"""
    print("\n📖 Reading IATA_MASTER...")
    try:
        tab = sheet.worksheet(IATA_MASTER_TAB)
        rows = tab.get_all_values()
        
        if not rows:
            print("❌ IATA_MASTER is empty")
            sys.exit(1)
        
        headers = rows[0]
        iata_idx = headers.index('iata')
        city_idx = headers.index('city')
        country_idx = headers.index('country')
        
        mapping = {}
        for row in rows[1:]:
            if len(row) > max(iata_idx, city_idx, country_idx):
                iata = row[iata_idx].strip().upper()
                city = row[city_idx].strip()
                country = row[country_idx].strip()
                if iata and city and country:
                    mapping[iata] = {'city': city, 'country': country}
        
        print(f"✅ Loaded {len(mapping)} IATA mappings")
        return mapping
    except Exception as e:
        print(f"❌ Failed to read IATA_MASTER: {e}")
        sys.exit(1)

def get_covered_destinations(sheet):
    """Get destinations that already have phrases"""
    print("\n📖 Reading PHRASE_BANK...")
    try:
        tab = sheet.worksheet(PHRASE_BANK_TAB)
        rows = tab.get_all_values()
        
        if not rows:
            print("⚠️  PHRASE_BANK is empty")
            return set()
        
        headers = rows[0]
        iata_idx = headers.index('destination_iata')
        
        covered = set()
        for row in rows[1:]:
            if len(row) > iata_idx:
                iata = row[iata_idx].strip().upper()
                if iata:
                    covered.add(iata)
        
        print(f"✅ {len(covered)} destinations already covered")
        return covered
    except Exception as e:
        print(f"❌ Failed to read PHRASE_BANK: {e}")
        sys.exit(1)

def get_all_destinations(sheet):
    """Get all unique destinations from CONFIG"""
    print("\n📖 Reading CONFIG...")
    try:
        tab = sheet.worksheet(CONFIG_TAB)
        rows = tab.get_all_values()
        
        if not rows:
            print("❌ CONFIG is empty")
            sys.exit(1)
        
        headers = rows[0]
        dest_idx = headers.index('destination_iata')
        
        destinations = set()
        for row in rows[1:]:
            if len(row) > dest_idx:
                iata = row[dest_idx].strip().upper()
                if iata and len(iata) == 3:
                    destinations.add(iata)
        
        print(f"✅ {len(destinations)} total destinations in CONFIG")
        return destinations
    except Exception as e:
        print(f"❌ Failed to read CONFIG: {e}")
        sys.exit(1)

def get_sample_phrases(sheet, limit=10):
    """Get sample approved phrases for voice calibration"""
    print("\n📖 Fetching sample phrases for voice calibration...")
    try:
        tab = sheet.worksheet(PHRASE_BANK_TAB)
        rows = tab.get_all_values()
        
        if len(rows) <= 1:
            print("⚠️  No sample phrases available")
            return []
        
        headers = rows[0]
        phrase_idx = headers.index('phrase')
        
        samples = []
        for row in rows[1:limit+1]:
            if len(row) > phrase_idx:
                phrase = row[phrase_idx].strip()
                if phrase:
                    samples.append(phrase)
        
        print(f"✅ Loaded {len(samples)} sample phrases")
        return samples
    except Exception as e:
        print(f"⚠️  Could not load samples: {e}")
        return []

def generate_phrase_candidates(openai_client, destination_city, destination_country, sample_phrases):
    """Generate 3 phrase candidates using OpenAI"""
    
    samples_text = "\n".join([f"- {p}" for p in sample_phrases]) if sample_phrases else "No samples available - use voice guide."
    
    prompt = f"""Generate 3 editorial travel phrases for {destination_city}, {destination_country}.

{VOICE_GUIDE}

Sample approved phrases (learn the style):
{samples_text}

Requirements:
1. Return EXACTLY 3 phrases, one per line
2. Each phrase must be 8-15 words
3. Focus on seasonal timing, cultural observations, or geographical facts
4. Avoid tourist clichés and hype language
5. Sound like a well-travelled friend sharing an insight

Format your response as:
1. [phrase one]
2. [phrase two]
3. [phrase three]

Destination: {destination_city}, {destination_country}
Generate phrases:"""

    try:
        response = openai_client.chat.completions.create(
            model=os.getenv('OPENAI_MODEL'),
            messages=[
                {"role": "system", "content": "You are an expert travel writer creating concise, anti-tourist editorial phrases in the style of Huckberry or Monocle magazine."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.8,
            max_tokens=200
        )
        
        content = response.choices[0].message.content.strip()
        
        # Parse numbered lines
        lines = [l.strip() for l in content.split('\n') if l.strip()]
        phrases = []
        
        for line in lines:
            # Remove numbering (1., 2., etc)
            cleaned = line
            if line[0].isdigit() and '. ' in line:
                cleaned = line.split('. ', 1)[1]
            phrases.append(cleaned)
        
        # Ensure exactly 3
        if len(phrases) < 3:
            print(f"⚠️  Only got {len(phrases)} phrases for {destination_city}, padding with empty")
            phrases.extend([''] * (3 - len(phrases)))
        
        return phrases[:3]
    
    except Exception as e:
        print(f"❌ OpenAI call failed for {destination_city}: {e}")
        return ['', '', '']

def write_candidates(sheet, candidates_data):
    """Write candidates to PHRASE_BANK_CANDIDATES tab"""
    print(f"\n📝 Writing {len(candidates_data)} candidates to {CANDIDATES_TAB}...")
    
    try:
        tab = sheet.worksheet(CANDIDATES_TAB)
        
        # Verify headers
        existing = tab.get_all_values()
        expected_headers = [
            'destination_iata', 'destination_city', 'destination_country',
            'candidate_1', 'candidate_2', 'candidate_3',
            'status', 'generated_at', 'notes'
        ]
        
        if not existing or existing[0] != expected_headers:
            print("⚠️  Setting headers in PHRASE_BANK_CANDIDATES")
            tab.update('A1:I1', [expected_headers])
        
        # Prepare rows
        rows = []
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        for item in candidates_data:
            rows.append([
                item['iata'],
                item['city'],
                item['country'],
                item['phrases'][0],
                item['phrases'][1],
                item['phrases'][2],
                'PENDING_REVIEW',
                timestamp,
                ''
            ])
        
        # Append all rows
        if rows:
            tab.append_rows(rows, value_input_option='RAW')
            print(f"✅ Wrote {len(rows)} candidate sets")
        else:
            print("⚠️  No candidates to write")
    
    except Exception as e:
        print(f"❌ Failed to write candidates: {e}")
        sys.exit(1)

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("🚀 TravelTxter V5 - Phrase Bank Generator")
    print("=" * 60)
    
    # Validate environment
    check_secrets()
    
    # Initialize clients
    sheet = init_sheets()
    openai_client = init_openai()
    
    # Load data
    iata_map = get_iata_mapping(sheet)
    all_destinations = get_all_destinations(sheet)
    covered_destinations = get_covered_destinations(sheet)
    sample_phrases = get_sample_phrases(sheet, limit=10)
    
    # Calculate gaps
    uncovered = all_destinations - covered_destinations
    print(f"\n📊 Coverage Analysis:")
    print(f"   Total destinations: {len(all_destinations)}")
    print(f"   Covered: {len(covered_destinations)}")
    print(f"   Uncovered: {len(uncovered)}")
    print(f"   Coverage: {len(covered_destinations)/len(all_destinations)*100:.1f}%")
    
    if not uncovered:
        print("\n✅ 100% coverage - nothing to generate")
        print("=" * 60)
        sys.exit(0)
    
    # Filter uncovered destinations to those with IATA mapping
    uncovered_with_data = []
    for iata in sorted(uncovered):
        if iata in iata_map:
            uncovered_with_data.append({
                'iata': iata,
                'city': iata_map[iata]['city'],
                'country': iata_map[iata]['country']
            })
        else:
            print(f"⚠️  {iata} not in IATA_MASTER, skipping")
    
    print(f"\n🎯 Generating phrases for {len(uncovered_with_data)} destinations...")
    
    # Generate candidates
    candidates_data = []
    for i, dest in enumerate(uncovered_with_data, 1):
        print(f"\n[{i}/{len(uncovered_with_data)}] {dest['city']}, {dest['country']} ({dest['iata']})")
        
        phrases = generate_phrase_candidates(
            openai_client,
            dest['city'],
            dest['country'],
            sample_phrases
        )
        
        candidates_data.append({
            'iata': dest['iata'],
            'city': dest['city'],
            'country': dest['country'],
            'phrases': phrases
        })
        
        print(f"   1. {phrases[0]}")
        print(f"   2. {phrases[1]}")
        print(f"   3. {phrases[2]}")
    
    # Write to candidates tab
    write_candidates(sheet, candidates_data)
    
    print("\n" + "=" * 60)
    print("✅ PHRASE_BANK_GENERATOR COMPLETE")
    print(f"✅ {len(candidates_data)} candidate sets written to {CANDIDATES_TAB}")
    print(f"✅ Next step: Review tab → approve phrases → manually copy to PHRASE_BANK")
    print("=" * 60)

if __name__ == "__main__':
    main()
