#!/usr/bin/env python3
"""
TravelTxter V5 - Reddit Authority Bridge Content Generator
Purpose: Generate credibility-building Reddit content across 12-week campaign
Contract: Reads THEMES + RAW_DEALS + REDDIT_CAMPAIGN_CALENDAR, writes to REDDIT_CONTENT_QUEUE only
Human loop: Richard reviews queue → edits → posts manually to Reddit
"""

import os
import sys
import uuid
from datetime import datetime, timezone, timedelta
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
REDDIT_CALENDAR_TAB = 'REDDIT_CAMPAIGN_CALENDAR'
REDDIT_QUEUE_TAB = 'REDDIT_CONTENT_QUEUE'
RAW_DEALS_TAB = 'RAW_DEALS'
THEMES_TAB = 'THEMES'

# Target subreddits
TARGET_SUBREDDITS = ['TravelHacks', 'Shoestring', 'digitalnomad']

# Voice guidance (locked)
VOICE_GUIDE = """
TravelTxter Reddit voice (natural, helpful, experienced):

You're the traveler who's figured out the patterns. Helpful, not preachy. Knowledgeable, not guru.

Write like you're typing quickly:
- Short sentences. Use commas for flow, full stops for separation.
- Use & instead of and where natural
- Casual shortcuts: convo, vs, tho, tbh, imo, demo
- First person experience: "I've tracked prices for 2 years"
- Specific details with numbers: "Jan to Mar & Oct to Nov"
- Honest trade offs: "You sacrifice X but gain Y"
- Invite discussion at the end: "Anyone else seeing this?"

Never:
- Vent or complain about bad trips
- Ask for help or validation  
- Show struggle or uncertainty
- Use bullet points with dashes
- Use hype language: "hidden gem", "bucket list", "life changing"
- Use sales language: "check out", "sign up", "follow me"
- Sound like a content writer or blog post

Good Reddit comment examples:
"March & April is the move for Morocco. Warm enough for coast, cool enough for desert. Summer crowds haven't hit Marrakech yet. I usually see flights from London around £180 in that window."

"I've flown Lisbon 4 times in 2 years. Best prices are Jan to Mar & Oct to Nov. TAP runs sales every 6 weeks or so. You sacrifice direct flights sometimes but save 30 to 40%. Anyone else tracking airline patterns?"

"If you're flexible on dates I'd skip peak summer & go shoulder season. Greece in May is half the price of August, no queues. Flew Athens for £120 last May vs £280 in July."

Bad Reddit comment examples:
"OMG you MUST visit this hidden gem! It's absolutely life changing!" (hype, cringe)
"Check out my travel blog for more amazing tips!" (spam, self promotion)
"Here are 5 tips for booking flights: - Book early - Be flexible - Use comparison sites" (blog post format, not natural)
"""

# Campaign calendar structure (12 weeks)
CAMPAIGN_CALENDAR = [
    # PHASE 1: Lurk & Learn (Week 1-4) - Build credibility through helpful comments
    {
        'week_number': 1,
        'strategy_phase': 'Lurk & Learn',
        'content_type': 'comment',
        'post_frequency': '3-5 comments',
        'brand_mention_allowed': False,
        'focus_topic': 'Winter sun destinations (Jan-Mar)',
        'tone_guidance': 'Helpful friend who knows the patterns',
        'success_metric': 'Upvotes + replies'
    },
    {
        'week_number': 2,
        'strategy_phase': 'Lurk & Learn',
        'content_type': 'comment',
        'post_frequency': '3-5 comments',
        'brand_mention_allowed': False,
        'focus_topic': 'Shoulder season tactics',
        'tone_guidance': 'Pattern spotter, not deal pusher',
        'success_metric': 'Upvotes + replies'
    },
    {
        'week_number': 3,
        'strategy_phase': 'Lurk & Learn',
        'content_type': 'comment',
        'post_frequency': '3-5 comments',
        'brand_mention_allowed': False,
        'focus_topic': 'Flight deal patterns (airline sales cycles)',
        'tone_guidance': 'Pattern spotter, not deal pusher',
        'success_metric': 'Upvotes + replies'
    },
    {
        'week_number': 4,
        'strategy_phase': 'Lurk & Learn',
        'content_type': 'comment',
        'post_frequency': '3-5 comments',
        'brand_mention_allowed': False,
        'focus_topic': 'Budget carrier hacks',
        'tone_guidance': 'Pragmatic advisor on trade offs',
        'success_metric': 'Upvotes + replies'
    },
    # PHASE 2: Original Value (Week 5-8) - Establish authority through posts
    {
        'week_number': 5,
        'strategy_phase': 'Original Value',
        'content_type': 'post',
        'post_frequency': '1-2 posts',
        'brand_mention_allowed': False,
        'focus_topic': 'Winter sun FOMO (book now vs summer)',
        'tone_guidance': 'Educational, contrarian timing advice',
        'success_metric': 'Post engagement + comment quality'
    },
    {
        'week_number': 6,
        'strategy_phase': 'Original Value',
        'content_type': 'post',
        'post_frequency': '1-2 posts',
        'brand_mention_allowed': False,
        'focus_topic': 'Open-jaw routing explained',
        'tone_guidance': 'Teach something useful, not obvious',
        'success_metric': 'Post engagement + comment quality'
    },
    {
        'week_number': 7,
        'strategy_phase': 'Original Value',
        'content_type': 'post',
        'post_frequency': '1-2 posts',
        'brand_mention_allowed': False,
        'focus_topic': 'Mistake fare hunting tactics',
        'tone_guidance': 'Insider knowledge, realistic expectations',
        'success_metric': 'Post engagement + comment quality'
    },
    {
        'week_number': 8,
        'strategy_phase': 'Original Value',
        'content_type': 'post',
        'post_frequency': '1-2 posts',
        'brand_mention_allowed': False,
        'focus_topic': 'Destination timing (avoid crowds)',
        'tone_guidance': 'Contrarian but backed by patterns',
        'success_metric': 'Post engagement + comment quality'
    },
    # PHASE 3: Subtle Integration (Week 9-12) - Tasteful TravelTxter mentions
    {
        'week_number': 9,
        'strategy_phase': 'Subtle Integration',
        'content_type': 'comment',
        'post_frequency': '3-5 comments',
        'brand_mention_allowed': True,
        'focus_topic': 'Mix: continue helpful commenting',
        'tone_guidance': 'Mention TravelTxter only when genuinely relevant',
        'success_metric': 'Upvotes + no spam accusations'
    },
    {
        'week_number': 10,
        'strategy_phase': 'Subtle Integration',
        'content_type': 'post',
        'post_frequency': '1 post',
        'brand_mention_allowed': True,
        'focus_topic': 'Case study: Real deals from winter',
        'tone_guidance': 'Show, do not sell',
        'success_metric': 'Engagement + TravelTxter clicks'
    },
    {
        'week_number': 11,
        'strategy_phase': 'Subtle Integration',
        'content_type': 'comment',
        'post_frequency': '3-5 comments',
        'brand_mention_allowed': True,
        'focus_topic': 'Continue value-add + soft mentions',
        'tone_guidance': 'Helpful first, promotional last',
        'success_metric': 'Authority maintained'
    },
    {
        'week_number': 12,
        'strategy_phase': 'Subtle Integration',
        'content_type': 'post',
        'post_frequency': '1 post',
        'brand_mention_allowed': True,
        'focus_topic': 'Reflection: What I learned tracking deals',
        'tone_guidance': 'Authentic, personal, useful',
        'success_metric': 'Break-even: subs from Reddit'
    }
]

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

def setup_campaign_calendar(sheet):
    """Create/update REDDIT_CAMPAIGN_CALENDAR tab"""
    print(f"\n📅 Setting up {REDDIT_CALENDAR_TAB}...")
    try:
        try:
            tab = sheet.worksheet(REDDIT_CALENDAR_TAB)
            print("   Tab exists, checking structure...")
        except:
            print("   Creating new tab...")
            tab = sheet.add_worksheet(title=REDDIT_CALENDAR_TAB, rows=100, cols=10)
        
        headers = [
            'week_number', 'start_date', 'strategy_phase', 'content_type',
            'post_frequency', 'brand_mention_allowed', 'focus_topic',
            'tone_guidance', 'success_metric'
        ]
        
        # Write headers
        tab.update('A1:I1', [headers])
        
        # Write calendar data (if empty)
        existing = tab.get_all_values()
        if len(existing) <= 1:
            print("   Populating 12-week calendar...")
            rows = []
            start_date = datetime.now()
            for week_data in CAMPAIGN_CALENDAR:
                week_start = (start_date + timedelta(weeks=week_data['week_number']-1)).strftime('%Y-%m-%d')
                rows.append([
                    week_data['week_number'],
                    week_start,
                    week_data['strategy_phase'],
                    week_data['content_type'],
                    week_data['post_frequency'],
                    week_data['brand_mention_allowed'],
                    week_data['focus_topic'],
                    week_data['tone_guidance'],
                    week_data['success_metric']
                ])
            tab.append_rows(rows, value_input_option='RAW')
            print(f"✅ Calendar populated with 12-week plan")
        else:
            print("✅ Calendar already populated")
        
    except Exception as e:
        print(f"❌ Failed to setup calendar: {e}")
        sys.exit(1)

def get_current_week_strategy(sheet):
    """Determine which week we're in and get strategy"""
    print("\n📖 Reading campaign calendar...")
    try:
        tab = sheet.worksheet(REDDIT_CALENDAR_TAB)
        rows = tab.get_all_values()
        
        if len(rows) <= 1:
            print("❌ Calendar is empty")
            sys.exit(1)
        
        headers = rows[0]
        
        # For demo purposes, use Week 5 (Winter sun FOMO post)
        # In production, calculate based on campaign start date
        target_week = 5
        
        for row in rows[1:]:
            week_num = int(row[0]) if row[0].isdigit() else 0
            if week_num == target_week:
                strategy = {
                    'week_number': week_num,
                    'strategy_phase': row[2],
                    'content_type': row[3],
                    'post_frequency': row[4],
                    'brand_mention_allowed': row[5].lower() == 'true',
                    'focus_topic': row[6],
                    'tone_guidance': row[7],
                    'success_metric': row[8]
                }
                print(f"✅ Using Week {target_week} strategy: {strategy['focus_topic']}")
                return strategy
        
        print(f"❌ Week {target_week} not found in calendar")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ Failed to read calendar: {e}")
        sys.exit(1)

def generate_reddit_content(openai_client, strategy, subreddit):
    """Generate Reddit post or comment based on strategy"""
    
    content_type = strategy['content_type']
    focus_topic = strategy['focus_topic']
    tone = strategy['tone_guidance']
    brand_ok = strategy['brand_mention_allowed']
    
    if content_type == 'post':
        prompt = f"""Generate a Reddit post for r/{subreddit}.

Topic: {focus_topic}
Tone: {tone}
Brand mentions allowed: {brand_ok}

{VOICE_GUIDE}

Requirements:
1. Title: Catchy but not clickbait (8 to 12 words)
2. Body: 150 to 300 words
3. Format: Conversational paragraphs, no bullet points
4. Include 1 or 2 specific examples with numbers & dates
5. Acknowledge trade offs honestly
6. End with open question to invite comments
7. NO self promotion unless brand_ok is True, and even then make it subtle
8. Write like you're typing quickly. Short sentences. Use & not and. Use commas for flow.

Structure:
Hook with contrarian observation. Why this matters now. Specific examples with numbers. What you sacrifice. Question to invite community input.

Generate the post:"""

    else:  # comment
        prompt = f"""Generate a helpful Reddit comment for r/{subreddit}.

Topic: {focus_topic}
Tone: {tone}
Brand mentions allowed: {brand_ok}

{VOICE_GUIDE}

Imagine replying to: "Anyone have tips for cheap flights to warm places this winter?"

Requirements:
1. Length: 50 to 150 words
2. Be specific with destinations, months, price ranges
3. Sound like personal experience, not a guidebook
4. Acknowledge downsides & trade offs
5. NO self promotion unless brand_ok is True
6. Write like typing quickly. Short sentences. Use & not and. Use commas for flow.

Generate the comment:"""

    try:
        response = openai_client.chat.completions.create(
            model=os.getenv('OPENAI_MODEL'),
            messages=[
                {
                    "role": "system",
                    "content": "You are an experienced traveler who frequents Reddit travel communities. You're helpful, specific, never pushy. Write like you're typing quickly in a comment box. Short sentences. Use & instead of and. Use commas for flow. Sound natural, not like a content writer."
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.8,
            max_tokens=500
        )
        
        content = response.choices[0].message.content.strip()
        
        # Parse title and body for posts
        if content_type == 'post':
            lines = content.split('\n')
            title = ''
            body = ''
            
            # Find title (usually prefixed or first line)
            for i, line in enumerate(lines):
                if 'title:' in line.lower():
                    title = line.split(':', 1)[1].strip()
                    body = '\n'.join(lines[i+1:]).strip()
                    break
            
            if not title:
                # Assume first line is title
                title = lines[0].strip()
                body = '\n'.join(lines[1:]).strip()
            
            # Clean up body
            body = body.replace('Body:', '').replace('body:', '').strip()
            
            return {'title': title, 'body': body}
        else:
            return {'title': '', 'body': content}
    
    except Exception as e:
        print(f"❌ OpenAI call failed: {e}")
        return {'title': '', 'body': ''}

def write_to_queue(sheet, strategy, content_items):
    """Write generated content to REDDIT_CONTENT_QUEUE"""
    print(f"\n📝 Writing {len(content_items)} items to {REDDIT_QUEUE_TAB}...")
    
    try:
        try:
            tab = sheet.worksheet(REDDIT_QUEUE_TAB)
        except:
            print("   Creating new tab...")
            tab = sheet.add_worksheet(title=REDDIT_QUEUE_TAB, rows=500, cols=15)
            headers = [
                'content_id', 'week_number', 'strategy_phase', 'content_type',
                'subreddit', 'title', 'body', 'post_url', 'status',
                'generated_at', 'posted_at', 'upvotes', 'comments', 'notes'
            ]
            tab.update('A1:N1', [headers])
        
        rows = []
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        for item in content_items:
            content_id = str(uuid.uuid4())[:8]
            rows.append([
                content_id,
                strategy['week_number'],
                strategy['strategy_phase'],
                strategy['content_type'],
                item['subreddit'],
                item['content']['title'],
                item['content']['body'],
                '',  # post_url (filled after posting)
                'PENDING_REVIEW',
                timestamp,
                '',  # posted_at
                '',  # upvotes
                '',  # comments
                ''   # notes
            ])
        
        if rows:
            tab.append_rows(rows, value_input_option='RAW')
            print(f"✅ Wrote {len(rows)} content items to queue")
        
    except Exception as e:
        print(f"❌ Failed to write to queue: {e}")
        sys.exit(1)

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("🚀 TravelTxter V5 - Reddit Authority Bridge Content Generator")
    print("=" * 70)
    
    # Validate environment
    check_secrets()
    
    # Initialize clients
    sheet = init_sheets()
    openai_client = init_openai()
    
    # Setup campaign calendar
    setup_campaign_calendar(sheet)
    
    # Get current week strategy
    strategy = get_current_week_strategy(sheet)
    
    print(f"\n🎯 Generating content for Week {strategy['week_number']}:")
    print(f"   Phase: {strategy['strategy_phase']}")
    print(f"   Type: {strategy['content_type']}")
    print(f"   Topic: {strategy['focus_topic']}")
    print(f"   Frequency: {strategy['post_frequency']}")
    
    # Generate content for each subreddit
    content_items = []
    for subreddit in TARGET_SUBREDDITS:
        print(f"\n📝 Generating for r/{subreddit}...")
        content = generate_reddit_content(openai_client, strategy, subreddit)
        
        if content['body']:
            content_items.append({
                'subreddit': subreddit,
                'content': content
            })
            if content['title']:
                print(f"   Title: {content['title'][:60]}...")
            print(f"   Body: {content['body'][:100]}...")
        else:
            print(f"   ⚠️ Generation failed for r/{subreddit}")
    
    # Write to queue
    if content_items:
        write_to_queue(sheet, strategy, content_items)
    else:
        print("\n⚠️ No content generated")
    
    print("\n" + "=" * 70)
    print("✅ REDDIT CONTENT GENERATOR COMPLETE")
    print(f"✅ {len(content_items)} items in queue for review")
    print(f"✅ Next: Review {REDDIT_QUEUE_TAB} → edit → post to Reddit → update status")
    print("=" * 70)

if __name__ == '__main__':
    main()
