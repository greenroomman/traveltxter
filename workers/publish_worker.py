#!/usr/bin/env python3
"""
TravelTxter V3.2(A) - Publish Worker
Posts approved deals to Facebook and Instagram on schedule.

This worker:
1. Connects to Google Sheets to fetch approved deals
2. Filters deals ready for publishing (workflow=READY_TO_POST)
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
        
        # Get worksheet - Use RAW_DEALS by default
        tab_name = os.getenv('RAW_DEALS_TAB', 'RAW_DEALS')
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
    Get the index of a column by name (case-insensitive).
    
    Args:
        headers: List of column headers
        column_name: Name of column to find
        
    Returns:
        int: Column index (0-based), or None if not found
    """
    try:
        # Try exact match first
        return headers.index(column_name)
    except ValueError:
        # Try case-insensitive match
        column_name_lower = column_name.lower()
        for i, h in enumerate(headers):
            if h.lower() == column_name_lower:
                return i
        logger.warning(f"Column '{column_name}' not found in headers")
        return None


def fetch_deals_to_publish(ws) -> List[Dict]:
    """
    Fetch deals from sheet that are ready to publish.
    Looks for: workflow=READY_TO_POST
    
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
        logger.debug(f"Headers: {headers[:10]}...")  # Show first 10 headers
        
        # Get column indices (using your actual column names)
        idx_workflow = get_column_index(headers, 'workflow')
        idx_raw_status = get_column_index(headers, 'raw_status')
        idx_ig_status = get_column_index(headers, 'ig_status')
        
        # Required columns from your sheet
        required_cols = {
            'deal_id': get_column_index(headers, 'deal_id'),
            'origin_city': get_column_index(headers, 'origin_city'),
            'destination_city': get_column_index(headers, 'destination_city'),
            'price_gbp': get_column_index(headers, 'price_gbp'),
            'outbound_date': get_column_index(headers, 'outbound_date'),
            'graphic_url': get_column_index(headers, 'graphic_url'),
        }
        
        # Check if we have minimum required columns
        missing = [k for k, v in required_cols.items() if v is None]
        if missing:
            logger.error(f"❌ Missing required columns: {missing}")
            raise ValueError(f"Missing columns: {missing}")
        
        if idx_workflow is None:
            logger.error("❌ Missing 'workflow' column - cannot determine which deals to publish")
            raise ValueError("Missing 'workflow' column")
        
        logger.info(f"✅ Found all required columns")
        
        # Filter deals ready to publish
        deals_to_publish = []
        
        for row_idx, row in enumerate(rows, start=2):  # start=2 because row 1 is headers
            # Pad row to match header length
            if len(row) < len(headers):
                row = row + [''] * (len(headers) - len(row))
            
            # Check workflow status
            workflow = row[idx_workflow].strip().upper() if idx_workflow < len(row) else ''
            
            # Check if already posted to Instagram
            ig_status = row[idx_ig_status].strip().upper() if idx_ig_status is not None and idx_ig_status < len(row) else ''
            
            # Skip if not READY_TO_POST or already posted
            if workflow != 'READY_TO_POST':
                continue
            
            if ig_status in ['POSTED', 'PUBLISHED', 'DONE']:
                logger.debug(f"Row {row_idx}: Already posted to Instagram, skipping")
                continue
            
            # Check if we have a graphic_url
            graphic_url = row[required_cols['graphic_url']] if required_cols['graphic_url'] < len(row) else ''
            if not graphic_url or not graphic_url.strip():
                logger.warning(f"Row {row_idx}: Missing graphic_url, skipping")
                continue
            
            # Build deal object
            deal = {
                'row_number': row_idx,
                'deal_id': row[required_cols['deal_id']],
                'origin_city': row[required_cols['origin_city']],
                'destination_city': row[required_cols['destination_city']],
                'price_gbp': row[required_cols['price_gbp']],
                'outbound_date': row[required_cols['outbound_date']],
                'graphic_url': graphic_url.strip(),
            }
            
            # Add optional fields that might be useful
            optional_fields = ['return_date', 'trip_length_days', 'stops', 'airline', 'ai_caption']
            for field in optional_fields:
                idx = get_column_index(headers, field)
                if idx is not None and idx < len(row):
                    deal[field] = row[idx]
            
            deals_to_publish.append(deal)
            logger.info(f"✅ Row {row_idx}: {deal['origin_city']} → {deal['destination_city']} (£{deal['price_gbp']})")
        
        logger.info(f"✅ Found {len(deals_to_publish)} deals ready to publish")
        return deals_to_publish
        
    except Exception as e:
        logger.error(f"❌ Error fetching deals: {e}")
        logger.debug(traceback.format_exc())
        raise


# ============================================================================
# SOCIAL MEDIA POSTING
# ============================================================================

def format_instagram_caption(deal: Dict) -> str:
    """
    Format a deal into an Instagram caption.
    
    Args:
        deal: Deal dictionary
        
    Returns:
        Formatted caption string
    """
    origin = deal.get('origin_city', 'UK')
    destination = deal.get('destination_city', 'Unknown')
    price = deal.get('price_gbp', '???')
    date = deal.get('outbound_date', '')
    
    # Clean up price (ensure it starts with £)
    if not price.startswith('£'):
        price = f"£{price}"
    
    # Use AI caption if available
    ai_caption = deal.get('ai_caption', '')
    if ai_caption and ai_caption.strip():
        caption = ai_caption.strip()
    else:
        # Default caption
        caption = f"""✈️ {origin} → {destination}

💰 From just {price}
📅 {date if date else 'Flexible dates'}

🔥 Limited availability - book fast!

Want ALL the best deals? Join our Telegram! Link in bio 👆

#TravelTxter #CheapFlights #Backpacking #TravelDeals #BudgetTravel"""
    
    return caption.strip()


def post_to_instagram(deal: Dict) -> Tuple[bool, str]:
    """
    Post a deal to Instagram using the Graph API.
    
    Args:
        deal: Deal dictionary
        
    Returns:
        Tuple of (success: bool, post_id or error message: str)
    """
    if DRY_RUN:
        logger.info("🧪 [DRY RUN] Would post to Instagram")
        return True, "dry_run_ig_123"
    
    try:
        access_token = os.getenv('FB_ACCESS_TOKEN')
        ig_user_id = os.getenv('IG_USER_ID')
        
        if not access_token or not ig_user_id:
            raise ValueError("FB_ACCESS_TOKEN and IG_USER_ID required")
        
        # Get image URL
        image_url = deal.get('graphic_url', '')
        if not image_url:
            raise ValueError("No graphic_url found for deal")
        
        # Format caption
        caption = format_instagram_caption(deal)
        
        logger.info(f"📸 Posting to Instagram: {deal['destination_city']}")
        logger.info(f"   Image URL: {image_url}")
        
        # Step 1: Create container
        graph_version = os.getenv('GRAPH_VERSION', 'v19.0')
        container_endpoint = f"https://graph.facebook.com/{graph_version}/{ig_user_id}/media"
        
        container_data = {
            'image_url': image_url,
            'caption': caption,
            'access_token': access_token
        }
        
        logger.info("   Creating Instagram media container...")
        container_response = requests.post(container_endpoint, data=container_data, timeout=30)
        
        if container_response.status_code != 200:
            error_data = container_response.json()
            error_msg = error_data.get('error', {}).get('message', 'Unknown error')
            logger.error(f"❌ Container creation failed: {error_msg}")
            return False, f"Container Error: {error_msg}"
        
        container_id = container_response.json().get('id')
        logger.info(f"   ✅ Container created: {container_id}")
        
        # Step 2: Publish container
        publish_endpoint = f"https://graph.facebook.com/{graph_version}/{ig_user_id}/media_publish"
        
        publish_data = {
            'creation_id': container_id,
            'access_token': access_token
        }
        
        logger.info("   Publishing container...")
        publish_response = requests.post(publish_endpoint, data=publish_data, timeout=30)
        
        if publish_response.status_code == 200:
            result = publish_response.json()
            post_id = result.get('id', 'unknown')
            logger.info(f"✅ Posted to Instagram: {post_id}")
            return True, post_id
        else:
            error_data = publish_response.json()
            error_msg = error_data.get('error', {}).get('message', 'Unknown error')
            logger.error(f"❌ Publishing failed: {error_msg}")
            return False, f"Publish Error: {error_msg}"
            
    except Exception as e:
        logger.error(f"❌ Failed to post to Instagram: {e}")
        logger.debug(traceback.format_exc())
        return False, str(e)


def update_publish_status(ws, row_number: int, status: str, media_id: str = None):
    """
    Update the publish status in the sheet.
    
    Args:
        ws: Worksheet object
        row_number: Row number to update (1-based)
        status: New status ('POSTED', 'FAILED', etc.)
        media_id: Instagram media ID
    """
    if DRY_RUN:
        logger.info(f"🧪 [DRY RUN] Would update row {row_number} ig_status to: {status}")
        return
    
    try:
        # Get headers to find columns
        headers = ws.row_values(1)
        
        # Find ig_status column
        ig_status_col = None
        for i, h in enumerate(headers, start=1):
            if h.lower() == 'ig_status':
                ig_status_col = i
                break
        
        if not ig_status_col:
            logger.warning("ig_status column not found, cannot update status")
            return
        
        # Update status
        ws.update_cell(row_number, ig_status_col, status)
        logger.info(f"   Updated ig_status to: {status}")
        
        # Update media ID if provided
        if media_id:
            ig_media_col = None
            for i, h in enumerate(headers, start=1):
                if h.lower() == 'ig_media_id':
                    ig_media_col = i
                    break
            
            if ig_media_col:
                ws.update_cell(row_number, ig_media_col, media_id)
                logger.info(f"   Updated ig_media_id: {media_id}")
        
        # Update timestamp
        ig_timestamp_col = None
        for i, h in enumerate(headers, start=1):
            if h.lower() == 'ig_published_timestamp':
                ig_timestamp_col = i
                break
        
        if ig_timestamp_col:
            ws.update_cell(row_number, ig_timestamp_col, datetime.now().isoformat())
        
    except Exception as e:
        logger.error(f"❌ Failed to update status for row {row_number}: {e}")
        logger.debug(traceback.format_exc())


# ============================================================================
# MAIN WORKER LOGIC
# ============================================================================

def publish_deals(deals: List[Dict], ws) -> Dict[str, int]:
    """
    Publish deals to Instagram and update sheet.
    
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
            logger.info(f"Publishing: {deal['origin_city']} → {deal['destination_city']} (£{deal['price_gbp']})")
            logger.info(f"Row: {deal['row_number']}")
            logger.info(f"Deal ID: {deal['deal_id']}")
            
            # Post to Instagram
            ig_success, ig_result = post_to_instagram(deal)
            
            if ig_success:
                logger.info(f"✅ Instagram: {ig_result}")
                update_publish_status(ws, deal['row_number'], 'POSTED', ig_result)
                stats['published'] += 1
                stats['post_ids'].append({
                    'deal_id': deal['deal_id'],
                    'destination': deal['destination_city'],
                    'instagram_id': ig_result
                })
            else:
                logger.error(f"❌ Instagram failed: {ig_result}")
                update_publish_status(ws, deal['row_number'], 'FAILED')
                stats['failed'] += 1
            
            # Rate limiting - be nice to Instagram API
            import time
            time.sleep(3)  # 3 second delay between posts
            
        except Exception as e:
            logger.error(f"❌ Error publishing deal: {e}")
            logger.debug(traceback.format_exc())
            stats['failed'] += 1
            
            try:
                update_publish_status(ws, deal['row_number'], 'ERROR')
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
    logger.info("🚀 TravelTxter Instagram Publisher Starting")
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
        required_vars = ['SHEET_ID', 'GCP_SA_JSON', 'FB_ACCESS_TOKEN', 'IG_USER_ID']
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
            logger.info(f"\n📤 Publishing {len(deals)} deal(s) to Instagram...")
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
