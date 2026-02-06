# Phrase Bank Generator - V5

**Purpose:** Generate editorial phrase candidates for destinations lacking PHRASE_BANK coverage

**Contract:**
- Reads: CONFIG, PHRASE_BANK, IATA_MASTER
- Writes: PHRASE_BANK_CANDIDATES (new tab, human review required)
- Never writes to canonical PHRASE_BANK

---

## Setup (One-Time)

### 1. Create PHRASE_BANK_CANDIDATES tab

In your Google Sheet, create a new tab named exactly:

```
PHRASE_BANK_CANDIDATES
```

**Headers (row 1):**
```
destination_iata | destination_city | destination_country | candidate_1 | candidate_2 | candidate_3 | status | generated_at | notes
```

Leave rows below empty. Worker will populate.

---

### 2. Verify GitHub Secrets

Ensure these exist (already configured):
- `OPENAI_API_KEY` ✅
- `OPENAI_MODEL` ✅
- `GCP_SA_JSON` ✅
- `SPREADSHEET_ID` ✅

---

## Usage

### Run the generator

1. Go to **Actions** tab in GitHub
2. Select **"Phrase Bank Generator"** workflow
3. Click **"Run workflow"** → **"Run workflow"**
4. Wait 5-10 minutes (depends on gap size)

### Review candidates

1. Open Google Sheet → `PHRASE_BANK_CANDIDATES` tab
2. Review all `candidate_1`, `candidate_2`, `candidate_3` columns
3. For each destination:
   - Pick best phrase (or edit it)
   - Add notes if needed

### Approve and deploy

**Manual copy to PHRASE_BANK:**

For each approved phrase:
1. Copy row data: `destination_iata`, chosen phrase
2. Paste into `PHRASE_BANK` with proper columns:
   - `destination_iata`: (from candidate)
   - `theme`: (leave blank or assign)
   - `category`: (assign based on phrase type)
   - `phrase`: (chosen candidate)
   - `approved`: TRUE
   - `channel_hint`: (optional)
   - `max_per_month`: (optional)
   - `notes`: (optional)
   - `context_hint`: (optional)

3. Delete row from `PHRASE_BANK_CANDIDATES` after approval

---

## What It Does

1. **Identifies gaps**: Compares CONFIG vs PHRASE_BANK
2. **Generates candidates**: 3 phrases per uncovered destination
3. **Learns voice**: Uses existing PHRASE_BANK as examples
4. **Writes to review queue**: Never touches canonical PHRASE_BANK

**Example output:**

| destination_iata | destination_city | candidate_1 | candidate_2 | candidate_3 |
|------------------|------------------|-------------|-------------|-------------|
| KTM | Kathmandu | Nepal's spring treks open in March before monsoon crowds arrive | Kathmandu's valley temples at dawn, before the motorbikes wake up | October's clear Himalayan skies after monsoon, before winter closes passes |

---

## Voice Guide (Locked)

The generator follows TravelTxter editorial voice:
- ✅ Conversational, knowledgeable, anti-tourist
- ✅ Seasonal/cultural/geographical hooks
- ✅ 8-15 words
- ✅ Factual observations
- ❌ No hype: "hidden gem", "bucket list", "must-see"
- ❌ No marketing: "paradise", "stunning", "discover"

---

## Frequency

**Recommended:** Run monthly or when coverage drops below 95%

**Check coverage:**
```
Coverage % = (PHRASE_BANK destinations / CONFIG destinations) × 100
```

Current target: **100% coverage**

---

## Troubleshooting

**"PHRASE_BANK_CANDIDATES not found"**
→ Create the tab with exact headers

**"OpenAI API error"**
→ Check `OPENAI_API_KEY` secret is valid
→ Verify `OPENAI_MODEL` is set (e.g., `gpt-4o-mini`)

**"No gaps found"**
→ Already 100% covered, nothing to generate

**Phrases sound wrong**
→ Add more approved examples to PHRASE_BANK
→ Generator learns from first 10 approved phrases

---

## Anti-Patterns (Do Not)

❌ Auto-approve candidates  
❌ Write directly to PHRASE_BANK  
❌ Generate theme-specific phrases (V1 is destination-level only)  
❌ Run on schedule (manual trigger only)  
❌ Skip human review  

---

## Metrics

**Target:** Close gap from 204/267 (89.9%) → 267/267 (100%)

**Cost:** ~$0.01-0.05 per full run (depending on model + gap size)

**Time:** 5-10 minutes for 60 destinations

---

## Future Enhancements (Not V1)

- Theme-specific phrase variants
- Seasonal phrase rotation
- Multi-language support
- Automated A/B testing integration

---

**Status:** Locked and ready to ship  
**Last updated:** 2026-02-06
