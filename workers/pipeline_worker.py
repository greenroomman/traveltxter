name: TravelTxter V4.5x Pipeline

on:
  workflow_dispatch:
  schedule:
    - cron: "30 7 * * *"
    - cron: "30 16 * * *"

concurrency:
  group: v45x-pipeline
  cancel-in-progress: false

jobs:
  pipeline:
    runs-on: ubuntu-latest
    env:
      PYTHONPATH: ${{ github.workspace }}

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      # ✅ FIXED: RUN_SLOT is set AND visible in this step AND exported for later steps
      - name: Resolve RUN_SLOT
        shell: bash
        run: |
          HOUR=$(date -u +%H)

          if [ "$HOUR" -lt "12" ]; then
            RUN_SLOT="AM"
          else
            RUN_SLOT="PM"
          fi

          echo "RUN_SLOT=$RUN_SLOT" >> $GITHUB_ENV
          echo "Resolved RUN_SLOT=$RUN_SLOT (UTC hour=$HOUR)"

      # ✅ This makes it obvious which branch is active
      - name: Show run mode
        shell: bash
        run: |
          echo "RUN_SLOT from env is: $RUN_SLOT"

      # ================= AM RUN =================

      - name: Feeder
        if: env.RUN_SLOT == 'AM'
        run: python workers/pipeline_worker.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          DUFFEL_API_KEY: ${{ secrets.DUFFEL_API_KEY }}
          DUFFEL_ROUTES_PER_RUN: ${{ vars.DUFFEL_ROUTES_PER_RUN }}
          DUFFEL_MAX_SEARCHES_PER_RUN: ${{ vars.DUFFEL_MAX_SEARCHES_PER_RUN }}
          DUFFEL_MAX_INSERTS: ${{ vars.DUFFEL_MAX_INSERTS }}

      - name: Scorer
        if: env.RUN_SLOT == 'AM'
        run: python workers/ai_scorer.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          WINNERS_PER_RUN: ${{ vars.WINNERS_PER_RUN }}
          DEST_REPEAT_PENALTY: ${{ vars.DEST_REPEAT_PENALTY }}
          VARIETY_LOOKBACK_HOURS: ${{ vars.VARIETY_LOOKBACK_HOURS }}

      - name: Link Router
        if: env.RUN_SLOT == 'AM'
        run: python workers/link_router.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          DUFFEL_API_KEY: ${{ secrets.DUFFEL_API_KEY }}
          REDIRECT_BASE_URL: ${{ secrets.REDIRECT_BASE_URL }}

      - name: Render
        if: env.RUN_SLOT == 'AM'
        run: python workers/render_client.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          RENDER_URL: ${{ secrets.RENDER_URL }}
          RENDER_MAX_ROWS: 1

      - name: Cooldown
        if: env.RUN_SLOT == 'AM'
        run: sleep 20

      - name: Instagram
        if: env.RUN_SLOT == 'AM'
        run: python workers/instagram_publisher.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          IG_ACCESS_TOKEN: ${{ secrets.IG_ACCESS_TOKEN }}
          IG_USER_ID: ${{ secrets.IG_USER_ID }}
          STRIPE_MONTHLY_LINK: ${{ vars.STRIPE_MONTHLY_LINK }}
          STRIPE_YEARLY_LINK: ${{ vars.STRIPE_YEARLY_LINK }}

      - name: Promotion Logger (Instagram)
        if: env.RUN_SLOT == 'AM'
        run: python workers/promotion_logger.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          PROMOTION_QUEUE_TAB: PROMOTION_QUEUE
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          POSTED_CHANNEL: INSTAGRAM
          PROMO_LOGGER_MAX_ROWS: 50

      - name: Telegram VIP
        if: env.RUN_SLOT == 'AM'
        run: python workers/telegram_publisher.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          TELEGRAM_BOT_TOKEN_VIP: ${{ secrets.TELEGRAM_BOT_TOKEN_VIP }}
          TELEGRAM_CHANNEL_VIP: ${{ secrets.TELEGRAM_CHANNEL_VIP }}
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHANNEL: ${{ secrets.TELEGRAM_CHANNEL }}
          RUN_SLOT: AM

      - name: Promotion Logger (Telegram VIP)
        if: env.RUN_SLOT == 'AM'
        run: python workers/promotion_logger.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          PROMOTION_QUEUE_TAB: PROMOTION_QUEUE
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          POSTED_CHANNEL: TELEGRAM_VIP
          PROMO_LOGGER_MAX_ROWS: 50

      # ================= PM RUN =================

      - name: Telegram FREE
        if: env.RUN_SLOT == 'PM'
        run: python workers/telegram_publisher.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHANNEL: ${{ secrets.TELEGRAM_CHANNEL }}
          TELEGRAM_BOT_TOKEN_VIP: ${{ secrets.TELEGRAM_BOT_TOKEN_VIP }}
          TELEGRAM_CHANNEL_VIP: ${{ secrets.TELEGRAM_CHANNEL_VIP }}
          RUN_SLOT: PM
          STRIPE_MONTHLY_LINK: ${{ vars.STRIPE_MONTHLY_LINK }}
          STRIPE_YEARLY_LINK: ${{ vars.STRIPE_YEARLY_LINK }}

      - name: Promotion Logger (Telegram FREE)
        if: env.RUN_SLOT == 'PM'
        run: python workers/promotion_logger.py
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          PROMOTION_QUEUE_TAB: PROMOTION_QUEUE
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          POSTED_CHANNEL: TELEGRAM_FREE
          PROMO_LOGGER_MAX_ROWS: 50
