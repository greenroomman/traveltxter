name: V3.2(A) Route Planner (Weekly)

on:
  workflow_dispatch:
  schedule:
    - cron: "10 4 * * 0"  # Sunday 04:10 UTC

concurrency:
  group: v32a-route-planner
  cancel-in-progress: false

jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4
      
      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: 'pip'
      
      - name: Install Dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
      
      - name: Run Route Planner
        env:
          # Required
          SHEET_ID: ${{ secrets.SHEET_ID }}
          GCP_SA_JSON: ${{ secrets.GCP_SA_JSON }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          
          # Optional configuration
          THEMES_TAB: ${{ vars.THEMES_TAB }}
          FEEDER_CONFIG_TAB: ${{ vars.FEEDER_CONFIG_TAB }}
          OPENAI_MODEL_PLANNER: ${{ vars.OPENAI_MODEL_PLANNER }}
          PLANNER_MAX_DESTS_PER_THEME: ${{ vars.PLANNER_MAX_DESTS_PER_THEME }}
          FEEDER_ORIGINS_JSON: ${{ vars.FEEDER_ORIGINS_JSON }}
          
          PYTHONUNBUFFERED: "1"
        run: python workers/route_planner.py
      
      - name: Save Planner Logs
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: planner-logs-${{ github.run_number }}
          path: logs/
          retention-days: 14
          if-no-files-found: warn
      
      - name: Generate Summary
        if: always()
        run: |
          echo "## 🗺️ Route Planner Summary" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "- 🆔 Run: #${{ github.run_number }}" >> $GITHUB_STEP_SUMMARY
          echo "- ⏰ Time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')" >> $GITHUB_STEP_SUMMARY
          echo "- 📋 Weekly route optimization complete" >> $GITHUB_STEP_SUMMARY
