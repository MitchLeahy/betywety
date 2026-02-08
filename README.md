# Bet Opportunity Finder

An application that identifies opportunistic betting opportunities by comparing Kalshi lines against other sportsbooks, and detects arbitrage opportunities for guaranteed profit.

## Features

- 🔍 **Opportunistic Bet Detection**: Find better odds on Kalshi compared to other sportsbooks
- 💰 **Arbitrage Detection**: Identify guaranteed profit opportunities across multiple books
- 📱 **SMS Alerts**: Real-time notifications sent directly to your phone
- ⚙️ **Customizable Preferences**: Set your own thresholds and filters
- 🏗️ **Medallion Architecture**: Bronze/Silver/Gold data layers for scalable data processing

## Architecture

This application uses a **medallion architecture** with three layers:

- **Bronze Layer**: Raw API responses stored in PostgreSQL and Parquet files
- **Silver Layer**: Cleaned and normalized betting lines ready for analysis
- **Gold Layer**: Matched markets, opportunities, and arbitrage calculations

## Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL 15+
- Redis (optional, for caching)

### Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Set up environment variables (copy `.env.example` to `.env` and fill in):
   - `DATABASE_URL`: PostgreSQL connection string
   - `KALSHI_API_KEY`: Your Kalshi API key
   - `KALSHI_API_SECRET`: Your Kalshi API secret
   - `THE_ODDS_API_KEY`: Your The Odds API key

### Database Setup

1. Start PostgreSQL (using Docker Compose):
   ```bash
   docker-compose up -d postgres redis
   ```

2. Initialize the database:
   ```bash
   python scripts/init_db.py
   ```

3. Run migrations (if using Alembic):
   ```bash
   alembic upgrade head
   ```

### Running the Pipeline

Run the complete data pipeline (bronze → silver → gold):
```bash
python scripts/run_pipeline.py
```

This will:
1. Collect raw data from Kalshi and The Odds API (Bronze)
2. Transform and normalize the data (Silver)
3. Match markets and detect opportunities/arbitrage (Gold)

### Scheduled Execution

To run the pipeline on a schedule, use the scheduler:
```python
from src.orchestration.scheduler import PipelineScheduler

scheduler = PipelineScheduler(
    bronze_interval_minutes=5,
    silver_interval_minutes=10,
    gold_interval_minutes=15
)
scheduler.start()
```

## Project Structure

```
betywety/
├── src/
│   ├── data/            # Data layer modules
│   │   ├── bronze/      # Bronze layer - raw data ingestion
│   │   │   ├── collectors/  # API collectors (Kalshi, Odds API)
│   │   │   └── storage.py   # Storage to PostgreSQL and Parquet
│   │   ├── silver/      # Silver layer - cleaned data
│   │   │   ├── transformers/# Data transformers
│   │   │   ├── validators.py# Data quality validators
│   │   │   └── storage.py   # Storage to PostgreSQL and Parquet
│   │   └── gold/        # Gold layer - business intelligence
│   │       ├── matchers/    # Market matching logic
│   │       ├── analyzers/   # Opportunity and arbitrage detection
│   │       └── storage.py   # Storage to Parquet
│   ├── models/          # Database models (SQLAlchemy)
│   ├── orchestration/   # Pipeline jobs and scheduler
│   └── utils/           # Utilities (odds conversion, matching)
├── scripts/             # Utility scripts
├── data/                # Parquet file storage (bronze/silver/gold)
└── alembic/            # Database migrations
```

## Data Flow

1. **Bronze Ingestion**: Collectors fetch data from APIs → Store raw responses
2. **Silver Transformation**: Transform raw data → Clean and normalize → Store betting lines
3. **Gold Aggregation**: Match markets across sources → Detect opportunities → Calculate arbitrage

## Querying Data

### Latest Lines (Silver Layer)
```python
from src.models.database import SessionLocal
from src.models.silver import BettingLine

db = SessionLocal()
lines = db.query(BettingLine).filter(
    BettingLine.is_active == True
).order_by(BettingLine.timestamp.desc()).limit(100).all()
```

### Opportunities (Gold Layer)
```python
from src.models.gold import Opportunity

opportunities = db.query(Opportunity).filter(
    Opportunity.is_active == True,
    Opportunity.expected_value >= 5.0
).all()
```

## Technology Stack

- **Backend**: Python 3.9+
- **Database**: PostgreSQL 15+ (with bronze/silver/gold schemas)
- **Data Storage**: Parquet files (partitioned by date)
- **Data Processing**: Pandas, PyArrow
- **Task Scheduling**: APScheduler
- **APIs**: Kalshi API, The Odds API

## Next Steps

- [ ] Add WebSocket support for real-time Kalshi updates
- [ ] Implement SMS alert system
- [ ] Add user management and preferences
- [ ] Create web interface for viewing opportunities
- [ ] Add more sportsbook integrations

## License

[Add your license here]
