name: TravelTxter Pipeline

on:
  workflow_dispatch:
    inputs:
      run_slot:
        description: "Run slot (AM or PM)"
        required: true
        default: "AM"

  schedule:
    # AM run (07:30 UTC)
    - cron: "30 7 * * *"
    # PM run (16:30 UTC)
    - cron: "30 16 * * *"

env:
  # For scheduled runs, infer slot from the cron that fired.
  # For manual runs, use workflow_dispatch input.
  RUN_SLOT: >-
    ${{ github.event_name == 'schedule' && github.event.schedule == '30 7 * * *' && 'AM'
        || github.event_name == 'schedule' && github.event.schedule == '30 16 * * *' && 'PM'
        || github.event.inputs.run_slot
        || 'AM' }}

jobs:
  pipeline:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Echo RUN_SLOT
        run: |
          echo "RUN_SLOT=${RUN_SLOT}"
          echo "EVENT=${{ github.event_name }}"
          echo "SCHEDULE=${{ github.event.schedule }}"

      # ============================================================
      # FEEDER
      # ============================================================
      - name: Run Feeder
        run: python workers/pipeline_worker.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}

      # ============================================================
      # SCORER
      # ============================================================
      - name: Run Scorer
        run: python workers/ai_scorer.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}

      # ============================================================
      # RENDER (PA ASSET CREATION)
      # ============================================================
      - name: Run Render Client
        run: python workers/render_client.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}

      # ============================================================
      # INSTAGRAM (MARKETING ONLY) — AM + PM
      # ============================================================
      - name: Run Instagram Publisher
        run: python workers/instagram_publisher.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}

      # ============================================================
      # TELEGRAM (VIP then FREE inside file / or separate)
      # ============================================================
      - name: Run Telegram Publisher
        run: python workers/telegram_publisher.py
        env:
          RUN_SLOT: ${{ env.RUN_SLOT }}
