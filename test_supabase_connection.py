#!/usr/bin/env python3
"""
Minimal Supabase connection test - isolate the problem
"""
import os
import sys
from supabase import create_client

print("=" * 50)
print("SUPABASE CONNECTION TEST")
print("=" * 50)

# Get credentials
url = os.getenv("SUPABASE_URL", "")
key = os.getenv("SUPABASE_KEY", "")

print(f"URL set: {bool(url)}")
print(f"KEY set: {bool(key)}")
print(f"KEY starts with: {key[:20]}..." if key else "No key")

if not url or not key:
    print("❌ Missing credentials")
    sys.exit(1)

# Try to connect
print("\nAttempting connection...")
try:
    supabase = create_client(url, key)
    print("✅ Client created")
except Exception as ex:
    print(f"❌ Failed to create client: {ex}")
    sys.exit(1)

# Try to read one row
print("\nAttempting SELECT query...")
try:
    result = supabase.table('snapshots').select('snapshot_id').limit(1).execute()
    print(f"✅ Query succeeded: {len(result.data)} rows")
except Exception as ex:
    print(f"❌ Query failed: {ex}")
    sys.exit(1)

print("\n" + "=" * 50)
print("✅ ALL TESTS PASSED")
print("=" * 50)
