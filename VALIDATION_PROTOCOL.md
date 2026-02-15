Date locked: [TODAY'S DATE]
Signed off by: Richard

HOLDOUT PERIOD
- Data collected between [START DATE] and [DAY 21] = training set
- Data collected between [DAY 22] and [DAY 28] = holdout set
- Model is fitted on training set only
- Brier score is calculated exclusively on holdout set

WHAT WE ARE PREDICTING
- Binary outcome: did the price on this route increase by 10% or more
  within 14 days of the snapshot date?
- Yes = 1, No = 0

BRIER SCORE CALCULATION
- Formula: (1/N) × Σ(predicted_probability - actual_outcome)²
- Pass threshold: score below 0.2
- Fail threshold: score 0.2 or above

MINIMUM SAMPLE SIZE
- We require at least [X] prediction events to consider the result valid
- Below this number the result is inconclusive, not a failure

WHAT WE WILL NOT DO
- We will not change this methodology after day 28
- We will not cherry-pick routes where the model performed well
- We will report the aggregate Brier score across all routes
