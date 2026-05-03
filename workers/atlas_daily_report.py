name: Atlas Snapshot Capture

on:
  schedule:
    - cron: "30 8 * * *"
  workflow_dispatch:

jobs:
  snapshot-capture:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Verify snapshot capture script exists
        run: |
          if [ ! -f workers/atlas_snapshot_capture.py ]; then
            echo "ERROR: workers/atlas_snapshot_capture.py not found"
            echo "Available worker files:"
            ls -la workers/
            exit 1
          fi

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install supabase requests httpx

      - name: Run snapshot capture
        env:
          SUPABASE_URL: ${{ secrets.MIZAR_SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.MIZAR_SUPABASE_SERVICE_ROLE_KEY }}
          DUFFEL_ACCESS_TOKEN: ${{ secrets.DUFFEL_ACCESS_TOKEN }}
          EIA_API_KEY: ${{ secrets.EIA_API_KEY }}
          ATLAS_MAX_SEARCHES: 157
        run: python workers/atlas_snapshot_capture.py