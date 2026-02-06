# Reddit Authority Bridge Content Generator - V5

**Purpose:** Generate credibility-building Reddit content across 12-week Authority Bridge campaign

**Contract:**
- Reads: REDDIT_CAMPAIGN_CALENDAR, THEMES, RAW_DEALS (context)
- Writes: REDDIT_CONTENT_QUEUE (review-only tab, never auto-posts)
- Human loop: Richard reviews → edits → posts manually → tracks engagement

**Target:** Break-even on Reddit subscribers by Week 10-12

---

## Setup (One-Time)

### 1. Create two new Google Sheets tabs

#### Tab A: REDDIT_CAMPAIGN_CALENDAR

**Tab name:** `REDDIT_CAMPAIGN_CALENDAR`

**Headers (row 1):**
```
week_number | start_date | strategy_phase | content_type | post_frequency | brand_mention_allowed | focus_topic | tone_guidance | success_metric
```

**Note:** Worker will auto-populate this tab with 12-week plan on first run

---

#### Tab B: REDDIT_CONTENT_QUEUE

**Tab name:** `REDDIT_CONTENT_QUEUE`

**Headers (row 1):**
```
content_id | week_number | strategy_phase | content_type | subreddit | title | body | post_url | status | generated_at | posted_at | upvotes | comments | notes
```

Leave empty. Worker will populate.

---

### 2. Verify GitHub Secrets

Already configured:
- `OPENAI_API_KEY` ✅
- `OPENAI_MODEL` ✅
- `GCP_SA_JSON` ✅
- `SPREADSHEET_ID` ✅

---

## 12-Week Authority Bridge Strategy

### Phase 1: Lurk & Learn (Week 1-4)
**Goal:** Build credibility through helpful comments

**Content type:** Comments on existing threads  
**Frequency:** 3-5 per week  
**Brand mentions:** ❌ Not allowed  
**Topics:**
- Week 1: Winter sun destinations (Jan-Mar)
- Week 2: Shoulder season tactics
- Week 3: Flight deal patterns (airline sales cycles)
- Week 4: Budget carrier hacks

**Success metric:** Upvotes + quality replies

---

### Phase 2: Original Value (Week 5-8)
**Goal:** Establish authority through original posts

**Content type:** Original educational posts  
**Frequency:** 1-2 per week  
**Brand mentions:** ❌ Not allowed  
**Topics:**
- Week 5: **Winter sun FOMO** (book now vs summer) ← **DEMO FOCUS**
- Week 6: Open-jaw routing explained
- Week 7: Mistake fare hunting tactics
- Week 8: Destination timing (avoid crowds)

**Success metric:** Post engagement + comment quality

---

### Phase 3: Subtle Integration (Week 9-12)
**Goal:** Tasteful TravelTxter mentions in context

**Content type:** Mix of comments + posts  
**Frequency:** 3-5 comments OR 1 post per week  
**Brand mentions:** ✅ Allowed (but tasteful)  
**Topics:**
- Week 9: Continue helpful commenting + soft mentions
- Week 10: Case study post (real deals from winter)
- Week 11: Value-add comments + mentions when relevant
- Week 12: Reflection post (what I learned tracking deals)

**Success metric:** Maintain authority + subscriber growth

**Target:** Break-even (Reddit subscribers cover campaign time investment)

---

## Target Subreddits

1. **r/TravelHacks** (budget-conscious, deal-oriented)
2. **r/Shoestring** (ultra-budget travelers)
3. **r/digitalnomad** (flexible, experience-first)

---

## Usage

### Automatic weekly run

**Schedule:** Every Sunday at 9 PM UTC  
**What happens:**
1. Reads current week from REDDIT_CAMPAIGN_CALENDAR
2. Generates 3-5 content items (1 per subreddit)
3. Writes to REDDIT_CONTENT_QUEUE with status `PENDING_REVIEW`

You'll get a notification Monday morning with content ready to review.

---

### Manual trigger (if needed)

1. Go to **Actions** tab in GitHub
2. Select **"Reddit Content Generator"**
3. Click **"Run workflow"** → **"Run workflow"**
4. Wait 2-3 minutes

---

### Review and post content

#### Step 1: Review queue
Open Google Sheet → `REDDIT_CONTENT_QUEUE` tab

Filter by `status = PENDING_REVIEW`

#### Step 2: Edit content
- Improve title (make it more Reddit-native)
- Adjust tone (match subreddit culture)
- Add personal touches
- Verify no hype language

#### Step 3: Post to Reddit
- Manually post to target subreddit
- Copy Reddit post URL
- Paste into `post_url` column
- Update `status` to `POSTED`
- Add `posted_at` timestamp

#### Step 4: Track engagement
After 24-48 hours:
- Update `upvotes` column
- Update `comments` column
- Add `notes` (e.g., "Good engagement", "Controversial", "Spam accusations")

---

## Demo Focus: Winter Sun FOMO (Week 5)

**Strategy:** Contrarian timing advice

**Core message:**
> "Everyone's booking summer 2026 right now. Smart travelers are locking in winter sun deals for Jan-Mar while prices are still low. By December, you'll be paying 40% more."

**Example post structure:**

**Title:**  
"PSA: Stop looking at summer flights. Winter sun is where the value is right now."

**Body:**
```
I've been tracking flight prices for 2+ years and there's a pattern most people miss.

Right now (February), everyone's searching for July/August trips. Airlines know this. Prices for summer are already climbing.

Meanwhile, winter sun destinations (Canary Islands, Egypt, Thailand, Mexico) for Jan-Mar 2027 are still at off-peak pricing. Once we hit October/November, these same routes jump 30-40%.

Examples I'm seeing today:
- London → Tenerife in January: £140 return
- Manchester → Sharm el-Sheikh in February: £180 return
- Same routes in August? £300+

Trade-off: You need to book 10-11 months out. If your dates aren't flexible, this doesn't work. But if you can commit early, you're essentially paying half price for guaranteed sun.

Anyone else booking winter instead of summer? What are you seeing for prices?
```

**Why this works:**
- ✅ Contrarian (goes against the herd)
- ✅ Specific examples with numbers
- ✅ Acknowledges trade-offs (builds trust)
- ✅ Invites discussion (encourages comments)
- ✅ No hype, no self-promotion (Phase 2 = no brand mentions yet)

---

## Voice Guidelines (Locked)

### ✅ Do:
- Sound like a well-travelled friend sharing patterns
- Use specific examples: "I flew Lisbon for £180 in March"
- Acknowledge downsides: "You'll sacrifice direct flights but save £200"
- Reference seasonal timing: "November is shoulder season"
- Be conversational and helpful

### ❌ Don't:
- Use hype: "hidden gem", "bucket list", "life-changing"
- Self-promote: "check out my blog", "follow me", "DM for secrets"
- Sound like a bot or guidebook
- Give generic advice: "just be flexible" (too vague)
- Gatekeep: "only real travelers know this"

---

## Anti-Patterns (Do Not)

❌ Auto-post to Reddit (kills authenticity)  
❌ Use the same content across subreddits (looks like spam)  
❌ Mention TravelTxter before Week 9 (builds no credibility)  
❌ Post without editing (AI tone is detectable)  
❌ Ignore downvotes/negative feedback (learn and adapt)  
❌ Spam links (instant ban)

---

## Metrics & Success Criteria

### Week 1-4 (Comments):
- **Target:** 5+ upvotes per comment
- **Good:** Replies from community members
- **Great:** "Thanks, this is helpful" responses

### Week 5-8 (Posts):
- **Target:** 20+ upvotes per post
- **Good:** 10+ engaged comments
- **Great:** Post referenced in other threads

### Week 9-12 (Integration):
- **Target:** Maintain credibility (no spam accusations)
- **Good:** 5+ TravelTxter signups from Reddit
- **Great:** Break-even on time investment

**Break-even calculation:**
- Time: 1 hour/week editing + posting = 12 hours total
- Subscribers needed: ~10-15 (at £3/month = £30-45 recovered)

---

## Troubleshooting

**"Content sounds too AI"**
→ Edit heavily. Add personal anecdotes. Make it messier.

**"Getting downvoted"**
→ Check tone. Are you being preachy? Too salesy?

**"No engagement"**
→ Title might be boring. Try controversial/contrarian angle.

**"Spam accusations"**
→ You mentioned TravelTxter too early. Pull back to Week 9.

**"Calendar not populating"**
→ Delete REDDIT_CAMPAIGN_CALENDAR tab, rerun worker

---

## Future Enhancements (Not V1)

- Automated sentiment tracking
- A/B test title variations
- Subreddit culture analysis
- Auto-scheduling via Reddit API (requires approval)

---

**Status:** Locked and ready to ship  
**First run:** Will demo Week 5 (Winter sun FOMO)  
**Last updated:** 2026-02-06
