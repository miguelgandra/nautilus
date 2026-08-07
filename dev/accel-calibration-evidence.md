# Accelerometer calibration - measured evidence (2026-08-06)

## 1. accel.scale is blind to the errors that corrupt tilt  (simulated, realistic posture)
error injected        accel.scale metric   pitch RMSE   roll RMSE
x offset +0.02 g      0.0002               1.11 deg     0.00
y offset +0.02 g      0.0002               0.05         10.42 deg
z offset +0.02 g      0.0195               0.29         2.30
x offset +0.05 g      0.0012               2.76         0.00
x gain +3%            0.0007               0.36         0.00
x gain +10%           0.0024               1.18         0.00
all gains +10%        0.1000               0.00         0.00   <- only mode it sees; harmless to angles
-> thresholds are warning 0.2 / error 0.5.  A 10% uniform gain error does NOT trip the warning.
-> the check is maximally sensitive to the one error that does not affect angles, and ~1000x
   under-sensitive to the per-axis errors that do.

## 2. In-situ sphere fitting is NOT routinely identifiable on free-swimming data
- .ellipsoidFit recovers a known offset EXACTLY in noiseless simulation, even at pitch sd 12 / roll sd 10.
- On real 03_checked data it returns offsets of 0.18-0.91 g and gain ratios to 4.7 - physically impossible.
- Cause: static-accel cloud planarity 0.017-0.18 on-animal (mag path rejects above planarity.max = 0.6).
- .ellipsoidFit's own cond.max gate did NOT catch this (returned ok=TRUE).
=> any accel-cal path MUST gate on coverage and will DECLINE on many deployments.

## 3. The off-animal window is sometimes a real calibration rotation
             on-animal                    off-animal
PIN_CAM_13   plan .029 pitch 108 roll  79 | plan .197 pitch  93 roll 332
PIN_06D      plan .068 pitch  63 roll 177 | plan .005 pitch 111 roll 186
PIN_05       plan .023 pitch  25 roll 120 | plan .080 pitch 147 roll  73
-> inconsistent, but PIN_CAM_13's post-release float sweeps 332 deg of roll vs 79 on-animal.
   nautilus already imports this period and filterDeploymentData already knows where it is.

## 4. Estimator design settled (2026-08-06)
Model: static a = G u + c  =>  ||a||^2 = sum g_i^2 u_i^2 + 2 sum g_i c_i u_i + O(|c|^2)
LINEAR in (u_i^2, u_i) with u the measured unit vector. No intercept (sum u_i^2 = 1 is collinear).
Recovers g_i = sqrt(beta_i), c_i = gamma_i / (2 g_i).
- Simulation: offset err 0.008 g at posture sd 12 deg / noise 0.005 g; beats .ellipsoidFit everywhere
  (ellipsoid gave NA or 0.16-0.62 g error at narrow coverage).
- BEWARE: stats::filter keeps length; dropping NA rows then indexing the raw matrix by position
  MISALIGNS the dynamic-residual gate and produces gains of 0.6-2.9. Cost me one wrong conclusion.

## 5. The gate is MODEL RESIDUAL, not conditioning
             med|A|  kappa  resid_sd  offs_n  tilt_deg  gains
 clean  PIN_12 0.947 1965.1  0.0089   0.057   0.32   0.992 0.991 0.999
        PIN_01 0.984  495.9  0.0115   0.041   0.79   0.997 0.971 0.948
        PIN_02 1.029  327.0  0.0168   0.024   1.27   1.062 1.028 1.039
        PIN_05 0.986  160.3  0.0180   0.013   0.14   0.992 0.986 1.000
        PIN_08 0.972  134.4  0.0245   0.066   1.28   0.975 1.003 1.028
        PIN_09 1.019  468.6  0.0246   0.030   0.74   1.018 1.012 1.013
 wild   PIN_10 0.974  224.7  0.0578   0.421  12.44   0.946 1.110 1.320
        PIN_04 0.901  139.2  0.0719   0.262   2.45   1.093 1.174 1.194
        PIN_03 0.896  146.8  0.1028   0.621   2.73   1.363 1.403 1.703
        PIN_06D 0.923 211.1  0.1496   0.964   2.85   1.911 1.632 2.295
-> resid_sd separates cleanly (<=0.025 vs >=0.058); kappa does NOT (PIN_12 worst kappa, best fit).
-> resid_sd measures MODEL ADEQUACY: are the "static" samples actually gravity-only?
   Gate at 0.04.  Clean deployments show implied tilt error 0.14-1.28 deg - real and worth reporting.

## 6. WHY THIS IS NOT IN v1 (decision, 2026-08-07)
Deferred deliberately, not abandoned. The reasoning:
- NOT ACTIONABLE. The check measures per-axis error but cannot correct it; step 3 (calibrateAccelerometer)
  is unbuilt, so the only user response is a caveat sentence.
- IMPACT SMALLER THAN IT FIRST LOOKED. tan(pitch) is brutally sensitive near level (28.7% speed error
  from a 2 deg bias at 5 deg pitch, and PIN_08's median |pitch| is 5.1 deg with 88% of samples < 20 deg)
  BUT reconstructTrackControl(speed.method=) defaults to "constant", and depth.rate.min.pitch defaults
  to 45 deg, so shallow samples are NA'd and back-filled. Above that guard the cost is < 7%.
  The alarming number is real and unreachable on defaults.
- NOT VALIDATED. N = 11, one species, one tag model, one sampling rate (100 Hz).
- FOUR DEFECTS IN ONE SESSION: false positives on error-free sensors (narrow posture), the heave axis
  invisible to both checks, two wrong worst-case formulas, and a +-50% sensitivity to `quiet`.
- CREDIBILITY. A false "your accelerometer is miscalibrated" in a first CRAN release is expensive.

### To revisit, in order
1. Replace the `quiet` knob with a multi-fit stability gate: fit at q in {0.2,0.3,0.4,0.5}, report the
   MEDIAN and use the SPREAD as a third gate + as the reported range. PIN_01 spans 1.00-2.52 across that
   range; PIN_05 flips clean->warning; PIN_12 flips warning->error. Reporting "1.66 deg" is false precision.
2. min.n must be SECONDS, not samples (5000 samples = 50 s at 100 Hz but 83 min at 1 Hz - the check
   would silently never run on a slow tag).
3. Keep residual/condition INTERNAL. Relaxing either is always wrong and is what a confused user would do.
   Public surface should be `warning` and `error` only.
4. Validate on >= 2 further taxa and >= 1 further tag model BEFORE exposing anything.
5. Ship only once a correction path exists to make the finding actionable.

### Files here
- accel-calibration-blindness-demo.R        table 1: accel.scale is blind to per-axis error
- accel-calibration-estimator-validation.R  the 6-parameter fit vs .ellipsoidFit, real + simulated
- accel-calibration-conditioning-check.R    the narrow-posture false positive (the review blocker)
- accel-calibration-quiet-sensitivity.R     the +-50% sensitivity that motivates the stability gate
