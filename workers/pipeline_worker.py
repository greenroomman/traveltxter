name: TravelTxter Pipeline (AM / PM / Manual TEST)

on:
  schedule:
    - cron: "30 7 * * *"
    - cron: "30 16 * * *"
  workflow_dispatch:
    inputs:
      run_slot:
        description: "Run slot"
        required: true
        default: "TEST"
        type: choice
        options: [TEST, AM, PM]

concurrency:
  group: traveltxter-${{ github.event_name == 'workflow_dispatch' && inputs.run_slot == 'TEST' && 'test' || 'prod' }}
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
          if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

      - name: Set RUN_SLOT
        shell: bash
        run: |
          if [ "${{ github.event_name }}" = "workflow_dispatch" ]; then
            echo "RUN_SLOT=${{ inputs.run_slot }}" >> $GITHUB_ENV
          else
            if [ "${{ github.event.cron }}" = "30 7 * * *" ]; then
              echo "RUN_SLOT=AM" >> $GITHUB_ENV
            else
              echo "RUN_SLOT=PM" >> $GITHUB_ENV
            fi
          fi
          echo "RUN_SLOT resolved to: $RUN_SLOT"

      - name: Feeder
        run: python workers/pipeline_worker.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          DUFFEL_API_KEY: ${{ secrets.DUFFEL_API_KEY }}
          DUFFEL_MAX_INSERTS: ${{ vars.DUFFEL_MAX_INSERTS }}
          DUFFEL_MAX_SEARCHES_PER_RUN: ${{ vars.DUFFEL_MAX_SEARCHES_PER_RUN }}
          DUFFEL_ROUTES_PER_RUN: ${{ vars.DUFFEL_ROUTES_PER_RUN }}
          PRICE_GATE_ENABLED: "true"
          PRICE_GATE_MULTIPLIER: ${{ vars.PRICE_GATE_MULTIPLIER }}
          PRICE_GATE_MIN_CAP_GBP: ${{ vars.PRICE_GATE_MIN_CAP_GBP }}
          PRICE_GATE_FALLBACK_BEHAVIOR: ${{ vars.PRICE_GATE_FALLBACK_BEHAVIOR }}

      - name: Wait for Sheets recalculation
        shell: bash
        run: |
          SEC="${{ vars.MIN_INGEST_AGE_SECONDS }}"
          if [ -z "$SEC" ]; then SEC="90"; fi
          echo "Sleeping ${SEC}s..."
          sleep "$SEC"

      - name: AI Scorer
        run: python workers/ai_scorer.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          MIN_INGEST_AGE_SECONDS: ${{ vars.MIN_INGEST_AGE_SECONDS }}

      - name: Link Router
        run: python workers/link_router.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          DUFFEL_API_KEY: ${{ secrets.DUFFEL_API_KEY }}
          REDIRECT_BASE_URL: ${{ vars.REDIRECT_BASE_URL }}

      - name: Render
        run: python workers/render_client.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          RENDER_URL: ${{ secrets.RENDER_URL }}

      - name: Instagram
        run: python workers/instagram_publisher.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          IG_ACCESS_TOKEN: ${{ secrets.IG_ACCESS_TOKEN }}
          IG_USER_ID: ${{ secrets.IG_USER_ID }}

      - name: Telegram
        run: python workers/telegram_publisher.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
          RAW_DEALS_TAB: RAW_DEALS
          GCP_SA_JSON_ONE_LINE: ${{ secrets.GCP_SA_JSON_ONE_LINE }}
          TELEGRAM_BOT_TOKEN_VIP: ${{ secrets.TELEGRAM_BOT_TOKEN_VIP }}
          TELEGRAM_CHANNEL_VIP: ${{ secrets.TELEGRAM_CHANNEL_VIP }}
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHANNEL: ${{ secrets.TELEGRAM_CHANNEL }}
