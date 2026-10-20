# Overfitting diagnostics and the opponent strength idea

2026-10-20. Props only. Method taken from the NFL project (`docs/diag/shrinkage_tuning.md` in NFLStatsPredictor); the write up of the method itself is in the vault note "Walk forward overfitting diagnostics for prop models". Tables are in `nba_generalization_tables.md`. Reproduce with:

```
python scripts/diag/extract_walkforward.py 2023 2024 2025
python scripts/diag/generalization_report.py --out docs/diag/nba_generalization_tables.md
python scripts/diag/opponent_factor_experiment.py
```

## Short version

1. **The model is not badly overfit, but its stacking step is.** The Ridge stacker is trained on the XGBoost models' predictions of their own training rows. Out of sample it is the worst stage in every market, 1.5 to 2.2% worse than the same stacker fit on honest inputs. The hand set pooling and caps then spend most of their effort repairing it.
2. **The simplest stage is as good as the whole stack.** A per minute rate model times predicted minutes, with no stacker, pooling or caps, matches or beats the production `final` in every market. Threes is the clearest case: 0.932 against 0.962.
3. **Opponent strength is real but small, and the opponent features the model had were empty.** A correct defense factor correlates about +0.06 with how much a player beats his own expectation next game. That is too little to improve MAE or P(over) log loss when applied as a multiplier, in any market, under any memory length I tried.
4. **A data bug sat under all of it.** Team and opponent ids were null for 2022 to 2024 and 89% of 2025, so every opponent and team feature was one constant for about 97% of training rows. Fixed (commit 506395a). Accuracy moved by about 0.05%, so it is a correctness fix, not an accuracy gain.

## How the NFL method maps here

| NFL step | NBA version |
|---|---|
| Fit on seasons before Y, predict Y | Eval seasons 2023, 2024, 2025, each fit on all earlier seasons. 79,822 rows per market |
| Calibration slope, actual on predicted | Same, per season and pooled, plus bias by predicted quintile |
| Optimal blend weight by least squares | Trust weight of each stage against the project's own history anchor |
| Leave one season out stability | Same, with a caveat (see 4) |
| Reject list | The last section |

One addition: every pipeline stage is kept as its own column, so the question is "which stage helps or hurts" and not just "is the final mean good".

## Results

### 1. Gap between in sample and out of sample error

| stage | typical gap |
|---|---|
| XGBoost direct | +6 to +11% |
| Rate model | +5 to +7% |
| Stacker | **+10 to +18%** |
| Final (after pooling and caps) | +3 to +6% |

The XGBoost models overfit mildly, which is normal for boosted trees. The stacker is the outlier.

### 2. The stacker leak

In `StatModelSuite.fit`, the stacker and the variance model are fit on `direct_fitted` and `rate_fitted`, the XGBoost models' predictions on the very rows they were trained on. Those are optimistically good, so the stacker learns to trust them too much. The `TimeSeriesSplit` inside `_fit_time_series_stacker` only picks the Ridge penalty. Its inputs are still the leaked predictions, so it cannot detect the leak.

Evidence, MAE on the later season, mean of 2024 and 2025:

| market | production stacker | stacker fit on out of sample inputs | rate model alone | production final |
|---|---:|---:|---:|---:|
| points | 4.863 | 4.791 (-1.5%) | 4.757 | 4.784 |
| rebounds | 1.986 | 1.949 (-1.8%) | 1.951 | 1.959 |
| assists | 1.441 | 1.410 (-2.2%) | 1.395 | 1.400 |
| threes | 0.959 | 0.946 (-1.4%) | 0.940 | 0.970 |
| turnovers | 0.948 | 0.932 (-1.7%) | 0.930 | 0.930 |
| pra | 6.318 | 6.212 (-1.7%) | 6.174 | 6.202 |

The honest stacker's weights also differ from what the leaked one would pick: it puts 0.4 to 0.6 on the rate model and 0.2 to 0.6 on the baseline, and almost nothing (0.04 to 0.2) on the direct model.

The calibration slope agrees. Pooled out of sample slope of the stacked mean is 0.84 to 0.96 (spread out too far, the overfit signature), against 0.98 to 1.04 for the direct model.

Caveat: the honest stacker here is a proxy. I fit it on out of sample predictions from earlier eval folds, not on proper nested out of fold predictions inside each training set. A real fix needs the nested version.

### 3. The caps and pooling

The stabilize caps bind on 26% (threes) to 50% (rebounds) of rows. On those rows the pre cap mean is biased high (points +0.70), and capping lowers MAE from 4.73 to 4.59. That is the caps repairing the stacker's overshoot, not a free gain: the rate model alone is already as accurate without them. They also cost something on threes, where the final is 1.8% worse than a plain season average, and the predicted quintile bias runs from +3% to +14% (the model over projects threes, more so for high predicted values).

### 4. Trust weights and stability

Least squares weight of the final mean against the history anchor:

| market | w (all) | by season 2023, 2024, 2025 |
|---|---:|---|
| points | 0.81 | 0.57, 1.05, 1.09 |
| rebounds | 0.95 | 0.70, 1.07, 1.23 |
| assists | 0.63 | 0.46, 0.64, 1.05 |
| threes | 0.65 | 0.57, 0.74, 0.69 |
| turnovers | 1.00 | 0.92, 0.91, 1.26 |
| pra | 0.71 | 0.48, 0.96, 1.01 |

By the NFL rule most of these read "noisy" or "unstable". They are not noise. The weight rises with every fold because the training window grows from 25k to 76k rows: the model deserves more trust as it sees more data. Leave one season out is the wrong stability test for expanding windows. By 2025 five markets have w at or above 1, so the final mean is slightly under trusted there, and only threes (0.69) is over trusted.

### 5. Tree count

Out of sample RMSE bottoms out at 80 trees on 25k training rows and 120 on 76k rows. At 220 trees it costs about 0.1% at full size. Tree count is not where the overfitting is.

## Result of the out of fold stacking fix

The stacker and the variance model now learn from out of fold base predictions: four time ordered blocks, each predicted by models fit only on earlier blocks, so the first block is training data only (`_out_of_fold_base_predictions` in `models.py`). Frames under 400 rows keep the old in sample behavior. The same walk forward was rerun before and after.

| | stacked stage MAE | final MAE (all rows) | P(over) log loss |
|---|---|---|---|
| leak fixed, pooling and caps kept (B) | **-2.8% to -4.7%** in every market | -0.2% to +0.4%, mean +0.06% | +0.11% (assists +0.39, turnovers +0.52) |
| leak fixed, pooling and caps removed (C) | same stacked stage | mean **-0.95%** | mean **-0.66%**, better in all six markets |

What this says:

- The leak is real and the fix works at the stage it targets. The honest stacker now matches the rate model alone (points 4.771 against 4.757).
- Shipped as is (B), the app's output does not improve. The pooling and caps were tuned against the leaky stacker and now give nothing back: on players with fewer than 5 games they make `final` 3 to 13% worse than the stacked mean, and on players with history they cost threes 2.4%.
- C is the version that pays. Threes improves 3.0% in each of the three seasons. Points and PRA are worse in the 2023 fold (one season of training data) and better in 2024 and 2025, which fits a stacker that gains from more data.
- Intervals got slightly wider (80% intervals now cover 85 to 90%). The hand set variance inflations in `_inflate_variance` were sized for the old, too small variance, so they likely double count now. That is the next thing to retune.
- Cost: each fit trains about 1.7 times as many models, roughly 40 seconds more per fit on 76k rows. A full training run with its 10 calibration folds takes several minutes longer.

Not done, and a decision for whoever owns the safety layers: removing pooling and caps (C). The caps also guard against wild predictions that MAE does not show, so check the tail (largest errors, extreme projections) before removing them.

## The opponent strength idea

### What the model already had

`features.py` builds `opponent_allowed_{market}_10`, position and profile versions of it, and `vs_opponent_*_avg_3`. Two problems:

- The window is 10 **player rows**, which is about one game's worth of one team's opponents, not 10 games. It is a raw average with no adjustment for who the defense happened to face.
- For about 97% of training rows it was a constant, because the ids were null (see the data bug above).

With ids fixed, the old feature's correlation with next game excess is 0.004 to 0.011 in five markets, effectively zero. It is noise.

### What I built and tested

Both use only games before the one being predicted, and both compare players against their own pre game expectation, so "how good were the players this defense faced" is normalized away.

- **Team defense factor.** For each defense, the minutes weighted average of (actual / expected) over its recent opponents, decayed by game, shrunk toward 1.0.
- **Skill matched factor.** The same, but each past player counts in proportion to how close his level is to the player being projected (a Gaussian kernel on log expected stat). This is the "skill gap" idea: a star is compared with stars.

Then, per market, out of sample: correlation with the next game's actual over final, a least squares lambda in `actual - final = lambda * final * (factor - 1)`, and an honest version where lambda comes only from earlier seasons. The honest result is scored on MAE and on log loss of P(over) at a posted style line near the anchor, since that is what a bet prices.

### Result

| | team defense | skill matched | old feature |
|---|---|---|---|
| spread of the factor (sd) | 4 to 8% | 6 to 10% | 39 to 57% (noise) |
| correlation with next game excess | +0.05 to +0.08 | +0.05 to +0.10 | 0.00 to 0.01 |
| lambda | +0.1 to +0.75 in five markets, negative in threes | similar | about 0 |
| honest change in MAE | +0.0 to +0.8% (worse) | -0.0 to +0.5% | about 0 |
| honest change in P(over) log loss | +0.0 to +0.6% (worse) | +0.0 to +0.4% (worse) | about 0 |

Assists and turnovers have the cleanest signal (lambda 0.45 to 1.0, positive in all three seasons), and still do not improve.

Sensitivity: five combinations of decay (0.90 to 0.995) and shrinkage (480 to 2400 minutes) give a correlation of +0.053 to +0.061 in every case, and average honest log loss 0.22 to 0.24% worse in every case. Two markets (rebounds, threes) show tiny gains in some settings, but threes has a lambda that flips sign between seasons, so I do not trust it. The factor quality is not the bottleneck.

### Why a real signal does not help

A correlation of 0.06 means the factor explains about 0.4% of the variation in how a player compares with his expectation. The best possible gain is a fraction of a percent of error. Applying it adds its own noise: the factor is estimated from a few hundred player games, and lambda is estimated from other seasons. Those errors are larger than the gain. NBA player stats are dominated by game to game noise, and the opposing defense is a small piece.

This does not say defense is irrelevant to a single bet. It says that as a post hoc multiplier, with the data available, it does not pay.

### Options, and what I would do

| option | what it is | verdict |
|---|---|---|
| A. Post hoc multiplier on the final mean | what I tested | Rejected for now. Honest accuracy and P(over) log loss both get worse |
| B. Features into the rate model | feed the team defense factor and the skill matched factor to XGBoost as inputs, replacing the broken 10 row features. It can learn where defense matters (by role, usage, minutes) instead of one global lambda | Worth trying, after the stacker fix. Cheap with the harness: add two columns, rerun the walk forward |
| C. Defense given who is playing | adjust for opposing starters out, since a defense without its rim protector is a different defense | Plausible bigger signal. Needs injury and lineup data by game for past seasons, which the parquet does not have |
| D. Pair interaction model | a low rank factorization of player by defense residuals | Rejected. Far too many parameters for about 80,000 rows per market; the NFL tiered weight attempt showed the pattern |
| E. Simulate against the defense | shift the Monte Carlo mean and variance by the factor | Same information as A, so same verdict until B shows a gain |

Gate for adopting B (or A later): the honest P(over) log loss improves by at least 0.2% in four of six markets and is not worse than 0.1% in any, with lambda or feature effect the same sign in all three seasons.

### What the factor is good for today

The factor is a sound description of a defense and could be shown in the app as context (this defense has been allowing 6% more assists than expected), without feeding the model. That costs nothing and does not risk accuracy. I have not built it.

## Recommended order

1. ~~Fix the stacker leak~~ Done, see the result section above.
2. **Fit the pooling and the caps again, or drop them (option C above), and retune the variance inflation.** This is where the measured gain is.
3. **Fix the player id split.** Parquet rows use nba_api ids and the database rows use BallDontLie ids, so a player's history is split in two, and 94% of the database rows from 2026 02 09 to 2026 05 13 (2,744 player games, identical points) are also in the parquet, so those games are double counted. A name and date match can unify the ids and drop the duplicates. Not done here; it changes training data and needs its own walk forward check.
4. **Retest opponent features (option B)** after 1 to 3.
5. **Look at threes.** It over projects, more at the top, and is worse than a season average. A leaner model or a different distribution family (counts) may do better.
6. **Home and away is missing for all parquet rows** (`is_home` is null). Pulling the schedule flag again in the backfill would restore a well known effect.

## Reject list

| idea | outcome |
|---|---|
| Opponent factor as a multiplier on the final mean | worse honest MAE and log loss in all six markets |
| Longer or shorter memory for the factor | no effect on correlation (+0.05 to +0.06 throughout) |
| Skill matched weighting | slightly stronger correlation (+0.05 to +0.10) but no better honest result |
| Cut XGBoost trees from 220 to 80 | worth about 0.1% at full data size, not worth a retrain on its own |
| Restoring team and opponent ids alone | about -0.05% MAE, noise level, but correct and kept |
| Leave one season out stability as the test | confounded by training size, use a fixed window if this is repeated |

## Limits

- Three eval seasons is one draw. The 2023 fold trains on one season only.
- The P(over) test uses a normal approximation with the model's own variance and a synthetic line near the anchor, not real posted lines.
- The honest stacker is a proxy (see 2).
- The dedupe and id findings come from name and date matching, which can be off by a few players.
