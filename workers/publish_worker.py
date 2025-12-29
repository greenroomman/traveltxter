#!/usr/bin/env python3
"""
TravelTxter V3.2(A) - Publish Worker
Posts approved deals to Facebook and Instagram on schedule.

This worker:
1. Connects to Google Sheets to fetch approved deals
2. Filters deals ready for publishing
3. Posts to Facebook and Instagram
4. Updates sheet with publish status
5. Logs all activities for monitoring

GitHub Actions Compatible - Works with environment variables
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import traceback

# Third-party imports
try:
    from google.oauth2 import service_account
    import gspread
    import requests
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Run: pip install gspread google-auth requests")
    sys.exit(1)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Logging setup
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/publish_worker.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Environment variables
DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
FORCE_RUN = os.getenv('FORCE_RUN', 'false').lower() == 'true'
WORKER_ID = os.getenv('WORKER_ID', 'publish_worker')

# GitHub context (for logging)
GITHUB_RUN_ID = os.getenv('GITHUB_RUN_ID', 'local')
GITHUB_RUN_NUMBER = os.getenv('GITHUB_RUN_NUMBER', '0')

# ============================================================================
# GOOGLE SHEETS CONNECTION
# ============================================================================

def get_sheets_credentials():
    """
    Get Google Sheets credentials.
    Supports both environment variable (GitHub Actions) and file (local dev).
    
    Returns:
        google.oauth2.service_account.Credentials
    """
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # Try environment variable first (GitHub Actions)
    gcp_json_str = os.getenv('GCP_SA_JSON')
    
    if gcp_json_str:
        logger.info("📋 Loading credentials from GCP_SA_JSON environment variable")
        try:
            creds_info = json.loads(gcp_json_str)
            creds = service_account.Credentials.from_service_account_info(
                creds_info, 
                scopes=scopes
            )
            logger.info("✅ Credentials loaded from environment variable")
            return creds
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Failed to parse GCP_SA_JSON: {e}")
            raise ValueError("Invalid GCP_SA_JSON format")
    
    else:
        # Local development: credentials in file
        SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_JSON', 'service_account.json')
        logger.info(f"📋 Loading credentials from file: {SERVICE_ACCOUNT_FILE}")
        
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            raise FileNotFoundError(
                f"Service account file not found: {SERVICE_ACCOUNT_FILE}\n"
                f"For GitHub Actions: Set GCP_SA_JSON secret\n"
                f"For local dev: Place service_account.json in project root"
            )
        
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=scopes
        )
        logger.info("✅ Credentials loaded from file")
        return creds


def get_worksheet():
    """
    Connect to Google Sheets and return the worksheet.
    
    Returns:
        gspread.Worksheet: The deals worksheet
    """
    try:
        # Get credentials
        creds = get_sheets_credentials()
        
        # Authorize gspread
        gc = gspread.authorize(creds)
        logger.info("✅ Authorized with Google Sheets API")
        
        # Get sheet ID
        sheet_id = os.getenv('SHEET_ID')
        if not sheet_id:
            raise ValueError("SHEET_ID environment variable required")
        
        # Open spreadsheet
        try:
            sh = gc.open_by_key(sheet_id)
            logger.info(f"✅ Opened spreadsheet: '{sh.title}'")
        except gspread.exceptions.SpreadsheetNotFound:
            raise ValueError(
                f"Spreadsheet not found: {sheet_id}\n"
                f"Make sure the service account has access to the sheet"
            )
        
        # Get worksheet
        tab_name = os.getenv('RAW_DEALS_TAB', 'Raw Deals')
        try:
            ws = sh.worksheet(tab_name)
            logger.info(f"✅ Using worksheet: '{ws.title}' ({ws.row_count} rows)")
            return ws
            
        except gspread.exceptions.WorksheetNotFound:
            available = [s.title for s in sh.worksheets()]
            raise ValueError(
                f"Worksheet '{tab_name}' not found\n"
                f"Available: {', '.join(available)}"
            )
            
    except Exception as e:
        logger.error(f"❌ Failed to connect to Google Sheets: {e}")
        raise


# ============================================================================
# DEAL PROCESSING
# ============================================================================

def get_column_index(headers: List[str], column_name: str) -> Optional[int]:
    """
    Get the index of a column by name.
    
    Args:
        headers: List of column headers
        column_name: Name of column to find
        
    Returns:
        int: Column index (0-based), or None if not found
    """
    try:
        return headers.index(column_name)
    except ValueError:
        logger.warning(f"Column '{column_name}' not found in headers")
        return None


def fetch_deals_to_publish(ws) -> List[Dict]:
    """
    Fetch deals from sheet that are ready to publish.
    
    Args:
        ws: gspread.Worksheet object
        
    Returns:
        List of deal dictionaries ready for publishing
    """
    logger.info("📥 Fetching deals from sheet...")
    
    try:
        # Get all data
        all_data = ws.get_all_values()
        
        if len(all_data) < 2:
            logger.info("ℹ️ Sheet is empty or has only headers")
            return []
        
        headers = all_data[0]
        rows = all_data[1:]
        
        logger.info(f"📊 Found {len(rows)} total rows in sheet")
        logger.debug(f"Headers: {headers}")
        
        # Get column indices
        idx_status = get_column_index(headers, 'publish_status')
        idx_scheduled = get_column_index(headers, 'scheduled_date')
        idx_approved = get_column_index(headers, 'approved')
        
        # Required columns
        required_cols = {
            'origin': get_column_index(headers, 'origin'),
            'destination': get_column_index(headers, 'destination'),
            'price': get_column_index(headers, 'price'),
            'outbound_date': get_column_index(headers, 'outbound_date'),
            'deal_url': get_column_index(headers, 'deal_url')
        }
        
        # Check if we have minimum required columns
        missing = [k for k, v in required_cols.items() if v is None]
        if missing:
            logger.warning(f"⚠️ Missing required columns: {missing}")
        
        # Filter deals ready to publish
        deals_to_publish = []
        today = datetime.now().date()
        
        for row_idx, row in enumerate(rows, start=2):  # start=2 because row 1 is headers
            # Skip if not enough columns
            if len(row) <= max(filter(None, [idx_status, idx_scheduled, idx_approved])):
                continue
            
            # Check approval status
            if idx_approved is not None:
                approved = row[idx_approved].lower().strip()
                if approved not in ['yes', 'true', '1', 'approved']:
                    continue
            
            # Check publish status
            if idx_status is not None:
                status = row[idx_status].lower().strip()
                if status in ['published', 'posted', 'done']:
                    continue  # Already published
            
            # Check scheduled date
            if idx_scheduled is not None and row[idx_scheduled]:
                try:
                    scheduled_date = datetime.strptime(
                        row[idx_scheduled].strip(), 
                        '%Y-%m-%d'
                    ).date()
                    
                    if scheduled_date > today and not FORCE_RUN:
                        continue  # Not scheduled yet
                        
                except (ValueError, IndexError):
                    logger.warning(f"⚠️ Invalid date format in row {row_idx}: {row[idx_scheduled]}")
            
            # Build deal object
            deal = {
                'row_number': row_idx,
                'origin': row[required_cols['origin']] if required_cols['origin'] is not None else '',
                'destination': row[required_cols['destination']] if required_cols['destination'] is not None else '',
                'price': row[required_cols['price']] if required_cols['price'] is not None else '',
                'outbound_date': row[required_cols['outbound_date']] if required_cols['outbound_date'] is not None else '',
                'deal_url': row[required_cols['deal_url']] if required_cols['deal_url'] is not None else '',
            }
            
            # Add optional fields
            for col_name in headers:
                idx = get_column_index(headers, col_name)
                if idx is not None and idx < len(row) and col_name not in deal:
                    deal[col_name] = row[idx]
            
            deals_to_publish.append(deal)
        
        logger.info(f"✅ Found {len(deals_to_publish)} deals ready to publish")
        return deals_to_publish
        
    except Exception as e:
        logger.error(f"❌ Error fetching deals: {e}")
        logger.debug(traceback.format_exc())
        raise


# ============================================================================
# SOCIAL MEDIA POSTING
# ============================================================================

def format_deal_message(deal: Dict) -> str:
    """
    Format a deal into a social media post message.
    
    Args:
        deal: Deal dictionary
        
    Returns:
        Formatted message string
    """
    origin = deal.get('origin', 'UK')
    destination = deal.get('destination', 'Unknown')
    price = deal.get('price', '???')
    date = deal.get('outbound_date', '')
    
    # Clean up price (ensure it starts with £)
    if not price.startswith('£'):
        price = f"£{price}"
    
    # Format message
    message = f"""✈️ {origin} → {destination}

💰 From just {price}
📅 {date if date else 'Flexible dates'}

🔥 Limited availability - book fast!

#TravelTxter #CheapFlights #Backpacking #TravelDeals
"""
    
    return message.strip()


def post_to_facebook(deal: Dict) -> Tuple[bool, str]:
    """
    Post a deal to Facebook.
    
    Args:
        deal: Deal dictionary
        
    Returns:
        Tuple of (success: bool, post_id or error message: str)
    """
    if DRY_RUN:
        logger.info("🧪 [DRY RUN] Would post to Facebook")
        return True, "dry_run_fb_123"
    
    try:
        access_token = os.getenv('FB_ACCESS_TOKEN')
        if not access_token:
            raise ValueError("FB_ACCESS_TOKEN not set")
        
        # Format message
        message = format_deal_message(deal)
        deal_url = deal.get('deal_url', '')
        
        # Facebook Graph API endpoint
        # Note: You'll need your Page ID - get it from Facebook Business Settings
        page_id = os.getenv('FB_PAGE_ID', 'me')  # 'me' for user, or specific page ID
        endpoint = f"https://graph.facebook.com/v18.0/{page_id}/feed"
        
        # Post data
        post_data = {
            'message': message,
            'access_token': access_token
        }
        
        # Add link if available
        if deal_url:
            post_data['link'] = deal_url
        
        # Make request
        logger.info(f"📘 Posting to Facebook: {deal['destination']}")
        response = requests.post(endpoint, data=post_data, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            post_id = result.get('id', 'unknown')
            logger.info(f"✅ Posted to Facebook: {post_id}")
            return True, post_id
        else:
            error_msg = response.json().get('error', {}).get('message', 'Unknown error')
            logger.error(f"❌ Facebook API error: {error_msg}")
            return False, f"FB Error: {error_msg}"
            
    except Exception as e:
        logger.error(f"❌ Failed to post to Facebook: {e}")
        return False, str(e)


def post_to_instagram(deal: Dict) -> Tuple[bool, str]:
    """
    Post a deal to Instagram.
    
    Args:
        deal: Deal dictionary
        
    Returns:
        Tuple of (success: bool, post_id or error message: str)
    """
    if DRY_RUN:
        logger.info("🧪 [DRY RUN] Would post to Instagram")
        return True, "dry_run_ig_456"
    
    try:
        access_token = os.getenv('FB_ACCESS_TOKEN')  # Same token for IG
        ig_user_id = os.getenv('IG_USER_ID')
        
        if not access_token or not ig_user_id:
            raise ValueError("FB_ACCESS_TOKEN and IG_USER_ID required")
        
        # Format message (Instagram has different character limits)
        message = format_deal_message(deal)
        
        # For Instagram, you typically need to:
        # 1. Upload image to Instagram
        # 2. Create container
        # 3. Publish container
        
        # Note: Instagram API requires images. This is a simplified version.
        # You'll need to generate/provide deal images
        
        logger.info(f"📸 Would post to Instagram: {deal['destination']}")
        logger.warning("⚠️ Instagram posting requires image - implement image generation")
        
        # Placeholder - implement actual Instagram posting
        # See: https://developers.facebook.com/docs/instagram-api/guides/content-publishing
        
        return True, "ig_placeholder"
        
    except Exception as e:
        logger.error(f"❌ Failed to post to Instagram: {e}")
        return False, str(e)


def update_publish_status(ws, row_number: int, status: str, post_ids: Dict[str, str] = None):
    """
    Update the publish status in the sheet.
    
    Args:
        ws: Worksheet object
        row_number: Row number to update (1-based)
        status: New status ('published', 'failed', etc.)
        post_ids: Dictionary of platform -> post_id
    """
    if DRY_RUN:
        logger.info(f"🧪 [DRY RUN] Would update row {row_number} status to: {status}")
        return
    
    try:
        # Get headers to find status column
        headers = ws.row_values(1)
        
        # Find or create publish_status column
        if 'publish_status' in headers:
            status_col = headers.index('publish_status') + 1  # 1-based
        else:
            # Add column if it doesn't exist
            logger.info("Adding 'publish_status' column to sheet")
            ws.update_cell(1, len(headers) + 1, 'publish_status')
            status_col = len(headers) + 1
        
        # Update status
        ws.update_cell(row_number, status_col, status)
        
        # Update timestamp
        if 'published_at' in headers:
            ts_col = headers.index('published_at') + 1
        else:
            ws.update_cell(1, len(headers) + 2, 'published_at')
            ts_col = len(headers) + 2
        
        ws.update_cell(row_number, ts_col, datetime.now().isoformat())
        
        # Add post IDs if provided
        if post_ids:
            if 'post_ids' in headers:
                ids_col = headers.index('post_ids') + 1
            else:
                ws.update_cell(1, len(headers) + 3, 'post_ids')
                ids_col = len(headers) + 3
            
            ws.update_cell(row_number, ids_col, json.dumps(post_ids))
        
        logger.info(f"✅ Updated row {row_number}: {status}")
        
    except Exception as e:
        logger.error(f"❌ Failed to update status for row {row_number}: {e}")


# ============================================================================
# MAIN WORKER LOGIC
# ============================================================================

def publish_deals(deals: List[Dict], ws) -> Dict[str, int]:
    """
    Publish deals to social media and update sheet.
    
    Args:
        deals: List of deals to publish
        ws: Worksheet object
        
    Returns:
        Dictionary with statistics (published, failed, skipped)
    """
    stats = {
        'published': 0,
        'failed': 0,
        'skipped': 0,
        'post_ids': []
    }
    
    for deal in deals:
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Publishing: {deal['origin']} → {deal['destination']} (£{deal['price']})")
            logger.info(f"Row: {deal['row_number']}")
            
            post_ids = {}
            all_success = True
            
            # Post to Facebook
            fb_success, fb_result = post_to_facebook(deal)
            if fb_success:
                post_ids['facebook'] = fb_result
                logger.info(f"✅ Facebook: {fb_result}")
            else:
                all_success = False
                logger.error(f"❌ Facebook failed: {fb_result}")
            
            # Post to Instagram
            ig_success, ig_result = post_to_instagram(deal)
            if ig_success:
                post_ids['instagram'] = ig_result
                logger.info(f"✅ Instagram: {ig_result}")
            else:
                # Instagram failure is non-critical for now
                logger.warning(f"⚠️ Instagram skipped: {ig_result}")
            
            # Update sheet status
            if all_success:
                update_publish_status(ws, deal['row_number'], 'published', post_ids)
                stats['published'] += 1
                stats['post_ids'].append({
                    'deal': f"{deal['origin']}-{deal['destination']}",
                    'ids': post_ids
                })
            else:
                update_publish_status(ws, deal['row_number'], 'failed')
                stats['failed'] += 1
            
            # Rate limiting - be nice to APIs
            import time
            time.sleep(2)  # 2 second delay between posts
            
        except Exception as e:
            logger.error(f"❌ Error publishing deal: {e}")
            logger.debug(traceback.format_exc())
            stats['failed'] += 1
            
            try:
                update_publish_status(ws, deal['row_number'], 'error')
            except:
                pass
    
    return stats


def save_stats(stats: Dict):
    """Save publishing statistics to JSON file for GitHub Actions."""
    try:
        stats_with_meta = {
            **stats,
            'timestamp': datetime.now().isoformat(),
            'run_id': GITHUB_RUN_ID,
            'run_number': GITHUB_RUN_NUMBER,
            'dry_run': DRY_RUN,
            'worker_id': WORKER_ID
        }
        
        with open('logs/publish_stats.json', 'w') as f:
            json.dump(stats_with_meta, f, indent=2)
        
        logger.info(f"📊 Stats saved to logs/publish_stats.json")
        
    except Exception as e:
        logger.warning(f"⚠️ Failed to save stats: {e}")


def main():
    """Main worker execution."""
    logger.info("\n" + "="*60)
    logger.info("🚀 TravelTxter V3.2(A) Publish Worker Starting")
    logger.info("="*60)
    logger.info(f"⏰ Timestamp: {datetime.now().isoformat()}")
    logger.info(f"🆔 Worker ID: {WORKER_ID}")
    logger.info(f"📋 Run: #{GITHUB_RUN_NUMBER} (ID: {GITHUB_RUN_ID})")
    logger.info(f"🧪 Dry Run: {DRY_RUN}")
    logger.info(f"💪 Force Run: {FORCE_RUN}")
    logger.info("="*60 + "\n")
    
    stats = {
        'published': 0,
        'failed': 0,
        'skipped': 0
    }
    
    try:
        # Validate environment
        logger.info("🔍 Validating environment...")
        required_vars = ['SHEET_ID', 'GCP_SA_JSON', 'FB_ACCESS_TOKEN']
        missing = [v for v in required_vars if not os.getenv(v)]
        
        if missing:
            logger.error(f"❌ Missing required environment variables: {missing}")
            sys.exit(1)
        
        logger.info("✅ Environment validated")
        
        # Connect to Google Sheets
        logger.info("\n📊 Connecting to Google Sheets...")
        ws = get_worksheet()
        
        # Fetch deals to publish
        logger.info("\n📥 Fetching deals to publish...")
        deals = fetch_deals_to_publish(ws)
        
        if not deals:
            logger.info("ℹ️ No deals ready to publish at this time")
            stats['skipped'] = 0
        else:
            # Publish deals
            logger.info(f"\n📤 Publishing {len(deals)} deal(s)...")
            stats = publish_deals(deals, ws)
        
        # Log summary
        logger.info("\n" + "="*60)
        logger.info("📊 PUBLISH SUMMARY")
        logger.info("="*60)
        logger.info(f"✅ Published: {stats['published']}")
        logger.info(f"❌ Failed: {stats['failed']}")
        logger.info(f"⏭️ Skipped: {stats['skipped']}")
        logger.info("="*60 + "\n")
        
        # Save stats for GitHub Actions
        save_stats(stats)
        
        # Exit with error if any failed (so GitHub Actions retries)
        if stats['failed'] > 0:
            logger.error(f"❌ Worker completed with {stats['failed']} failures")
            sys.exit(1)
        
        logger.info("✅ Publish worker completed successfully")
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Worker interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        logger.error(f"\n❌ Worker failed with error: {e}")
        logger.debug(traceback.format_exc())
        
        # Save error stats
        stats['error'] = str(e)
        save_stats(stats)
        
        sys.exit(1)


if __name__ == '__main__':
    main()
