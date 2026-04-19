# Product Requirements Document: Project "Sharp Edge" MVP

1. Objective

To build an automated, Python-based ETL pipeline that identifies +EV (Positive Expected Value) NBA player prop bets by comparing mathematically derived median projections against live PrizePicks lines, delivering actionable alerts to the user.

2. Scope

In Scope (MVP):

Target Sport: NBA Playoffs (Points, Rebounds, Assists, and PRA combinations).

Data Ingestion: Automated fetching of historical NBA box scores and player game logs.

Line Scraping: Intercepting and parsing the public JSON payload from the PrizePicks web app.

Statistical Engine: Calculating 10-game and 15-game rolling medians, means, and standard deviations for active players.

Alerting System: Pushing formatted alerts to a private Discord channel via Webhook when a line discrepancy exceeds a defined threshold (e.g., > 15% difference).

Out of Scope (Non-Goals for Phase 1):

Automated Bet Placement: Strictly forbidden by PrizePicks TOS; risk of account ban and fund seizure.

Machine Learning Models: No XGBoost, Random Forest, or neural networks. We are sticking to robust statistical medians to establish a baseline win rate before introducing complex predictive models.

Automated Injury Adjustments: Parsing Twitter/X for injury news is notoriously difficult and prone to breaking. For the MVP, the model assumes standard rotations. You (the human) will manually adjust for breaking news.

Other Sports: No NFL, MLB, or soccer until the NBA logic proves profitable.

3. Core System Components

Module A: The Scraper (Data Fetch)

Requirement: Use nba_api to pull daily player logs.

Requirement: Use requests to fetch the PrizePicks board JSON.

Module B: The Engine (Transformation & Logic)

Requirement: Use pandas to clean data and handle missing values.

Requirement: Calculate the "True Median" and "Volatility" (standard deviation).

Requirement: Compare the "True Median" against the "PrizePicks Line" to calculate the delta (edge percentage).

Module C: The Notifier (Output)

Requirement: Filter for plays where the absolute delta is > 15%.

Requirement: Format a clean string message and POST to Discord.

4. Success Criteria

Technical: The script runs end-to-end without crashing, successfully parsing live lines and outputting alerts within a 60-second execution window.

Performance: The model achieves a > 54.2% hit rate on identified props over a sample size of 100 paper-traded bets (simulated bets without real money).

5. Milestones & Implementation Plan

Milestone 1: Establish data ingestion (Successfully query nba_api and the PrizePicks JSON).

Milestone 2: Build the math engine (Merge the two datasets using Pandas and calculate the discrepancies).

Milestone 3: Set up the alerting infrastructure (Discord Webhook integration).

Milestone 4: Deployment & scheduling (Setting up a cron job or background task to run the script hourly).