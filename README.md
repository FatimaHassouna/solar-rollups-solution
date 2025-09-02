# Solar Metrics Rollup Solution

## Overview
Python script to calculate daily, weekly, monthly, and yearly rollups for solar energy data stored in MongoDB.

## Features
- Finds last record of each day using aggregation pipeline
- Calculates sums for weekly, monthly, and yearly periods
- Saves results to MongoDB with proper formatting

## Setup
1. Install MongoDB and Python 3.8+
2. Run: `pip install -r requirements.txt`
3. Run: `python src/create_sample_data.py`
4. Run: `python src/solar_rollups.py`
