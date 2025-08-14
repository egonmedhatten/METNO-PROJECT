# Purpose

Collect and evaluate weather forecasts for Ljungskile, and try to improve the forecats by protection.

Setup with cron: 0 */6 * * * nice -10 /home/johan/miniconda3/envs/weather-forecast/bin/python /home/johan/Documents/PhD/PREMACOP/METNO-project/download_MEPS_latest.py >>/home/johan/Documents/PhD/PREMACOP/METNO-project/MEPS_latest_ljungskile/cron.log 2>&1