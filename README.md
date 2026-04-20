# 🏀 NBA Sharp Edge Bot (V1.0)
**Quantitative Analysis & Machine Learning for NBA Player Props**

The NBA Sharp Edge Bot is a high-performance quantitative system designed to identify and exploit statistical discrepancies in the NBA player prop market. It moves beyond simple averages by utilizing median-trend analysis, defensive matchup context, and an XGBoost Machine Learning model trained on over 40,000 historical data points.

---

## 🚀 The Pipeline: From Raw Data to Sharp Bets

The bot operates in a high-speed, 5-stage pipeline designed to minimize network latency and maximize predictive accuracy.

### 1. The Extraction
The system uses `cloudscraper` in `src/main.py` to bypass anti-bot protections and fetch the live PrizePicks board. It extracts hundreds of lines and filters for standard NBA props, ensuring "Free Squares" or promotional lines do not skew the data.

### 2. The "God-Call" Optimization
To prevent `429 Resource Exhausted` errors and API rate-limiting, the bot utilizes a "God-Call" strategy. Instead of hitting the NBA API for every player individually, `src/nba_fetcher.py` makes **one massive call** to the `LeagueGameLog` endpoint. This loads the entire league's season box scores into a Pandas DataFrame in memory for millisecond-fast slicing.

### 3. Defensive Matchup Multipliers
The bot calculates context using the `LeagueDashTeamStats` (Opponent) endpoint. 
* **The Math:** If a team allows 15% more Rebounds than the league average, the bot applies a **1.15x multiplier** to the player's True Median.
* **The Impact:** This dynamically adjusts projections based on the opponent's defensive efficiency, preventing "Over" bets against lockdown defenses.

### 4. The XGBoost ML Brain
For "Points" props, the system passes the data into a trained **XGBoost Classifier**.
* **Veto Power:** The AI evaluates fatigue (`Days_Rest`), Home/Away status, and recent volume. If the raw math says "Bet OVER" but the AI calculates a win probability below 50%, the bet is automatically vetoed.
* **Accuracy:** The current model achieves a **~54.9% OVER Precision**, which sits well above the standard -110 break-even threshold (52.4%).

### 5. Smart Grading with Live-Pivot
`src/grader.py` manages the settlement of bets using a **Live-Pivot** logic. If a game is marked as `FINAL` but has not yet hit the NBA's historical logs (the "Midnight Batch" problem), the grader pivots to the `BoxScoreTraditionalV3` endpoint to pull real-time player stats.

---

## 🛠 Project Components

| Module | Purpose |
| :--- | :--- |
| `main.py` | Orchestrator for fetching, analysis, and alerting. |
| `engine.py` | Logic core. Applies multipliers, median filters, and ML probabilities. |
| `nba_fetcher.py` | Data layer. Handles God-Calls and defensive stat extraction. |
| `grader.py` | Smart accountant. Settles bets using live-pivot logic and caches. |
| `prep_ml_data.py` | Time-traveler. Generates 40,000+ simulated historical bets for training. |
| `train_model.py` | The Teacher. Trains the XGBoost brain on historical performance. |
| `db.py` | Persistence layer. Manages the SQLite database and alert filtering. |
| `utils.py` | The Observer. Provides `@timer` decorators and rotating system logs. |

---

## 📊 Statistical Strategy: The Dual-Median Filter

To avoid "slump traps," the bot calculates two different medians for every prop:
1.  **15-Game Median:** The long-term baseline of the player's true output.
2.  **5-Game Median:** The short-term trend.

**The Filter:**
* If betting an **OVER**, the 5-game median must be at least **75%** of the 15-game median.
* If betting an **UNDER**, the 5-game median must not exceed the 15-game median by more than **25%**.
* This ensures the bot only bets on players maintaining their rotational volume.

---

## 📂 Project Structure
```text
nba-sharp-edge-bot/
├── data/               # CSV training datasets (Generated locally)
├── models/             # Trained XGBoost .pkl files (Generated locally)
├── logs/               # Rotating system logs (system.log)
├── src/
│   ├── main.py         # Entry point (Daily Pipeline)
│   ├── engine.py       # Math & ML Edge Logic
│   ├── nba_fetcher.py  # High-speed API God-Calls
│   ├── grader.py       # Smart Grading with Live-Pivot
│   ├── prep_ml_data.py # Historical Backtesting & Data Prep
│   ├── train_model.py  # XGBoost Model Training
│   ├── db.py           # SQLite Prediction Tracking
│   ├── notifier.py     # Discord & Gemini AI Logic
│   └── utils/          # Shared utilities
│       ├── utils.py    # Latency tracking & logging decorators
│       └── constants.py# Shared stat mappings
├── requirements.txt    # Python dependencies
└── .gitignore          # Prevents tracking of .db, .csv, and .pkl files
```

---

## ⚡ Installation & Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
brew install libomp  # Required for XGBoost on macOS
```

### 2. Initialize the AI Brain

Model and data files are excluded from Git. You must generate them locally:

```bash
# Generate 40k+ historical simulated bets
python src/prep_ml_data.py

# Train the XGBoost Classifier
python src/train_model.py
```

### 3. Environment Configuration

Create a `.env` file in the root directory:

```
DISCORD_WEBHOOK_URL=your_discord_webhook_url
GOOGLE_API_KEY=your_gemini_api_key
```

### 4. Run the Bot

```bash
# To find new edges and alert Discord
python src/main.py

# To grade and settle previous bets
python src/grader.py
```

---

## 📉 Observability

The bot includes built-in latency tracking. Every core function is timed and logged to `logs/system.log`.

* **[LATENCY]**: Tracks how long API calls and Pandas operations take.
* **[INFO]**: Logs found edges and successful Discord deliveries.
* **[ERROR]**: Detailed stack traces for API timeouts or data mismatches.

---

## 🛡 License

This project is for educational and analytical purposes.  
MIT License
