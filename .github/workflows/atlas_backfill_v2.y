name: Atlas v2 Feature Backfill

on:
  workflow_dispatch:

jobs:
  backfill:
    runs-on: ubuntu-latest
    timeout-minutes: 60

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install supabase pandas numpy

      - name: Run backfill
        env:
          MIZAR_SUPABASE_URL: ${{ secrets.MIZAR_SUPABASE_URL }}
          MIZAR_SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.MIZAR_SUPABASE_SERVICE_ROLE_KEY }}
        run: python workers/atlas_backfill_v2.py
