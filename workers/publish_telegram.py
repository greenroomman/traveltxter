name: Publish Telegram Deal

on:
  schedule:
    - cron: "30 7 * * *"   # 07:30 UTC
    - cron: "30 16 * * *"  # 16:30 UTC
  workflow_dispatch:

jobs:
  publish:
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
        run: pip install -r requirements.txt
      
      - name: Publish Deal to Telegram
        env:
          PYTHONPATH: ${{ github.workspace }}
          
          # Google Sheets
          GCP_SA_JSON: ${{ secrets.GCP_SA_JSON }}
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          DEALS_SHEET_NAME: RAW_DEALS
          
          # Telegram
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHANNEL: ${{ secrets.TELEGRAM_CHANNEL }}
          
          # Optional filters
          TELEGRAM_ALLOW_VERDICTS: "GOOD"
          TELEGRAM_MIN_AI_SCORE: "70"
        run: python workers/telegram_publisher.py
      
      - name: Generate Summary
        if: always()
        run: |
          echo "## 📱 Telegram Publisher Summary" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "- 🆔 Run: #${{ github.run_number }}" >> $GITHUB_STEP_SUMMARY
          echo "- ⏰ Time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')" >> $GITHUB_STEP_SUMMARY
          echo "- 📋 Tab: RAW_DEALS" >> $GITHUB_STEP_SUMMARY
          echo "- 🎯 Filters: GOOD deals, score ≥70" >> $GITHUB_STEP_SUMMARY
