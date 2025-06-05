# Football Data ETL Pipeline

A comprehensive Extract-Transform-Load (ETL) pipeline for processing football statistics data into a structured PostgreSQL database.

## Motivation

My motivation for this project is simple, I wanted to learn. Also, I had been meaning to make an FPL(Fantasy Premier League) bot that would make decisions on my behalf, if needed, so I don't miss a deadline again. I want this bot to be working with the real information and make data-informed decisions with minimal supervision. That brings about the birth of this pipeline which will feed advanced statistics to the bot to aiid the decisions. In essence, this pipeline is part of a larger project.

## Project Overview

This project creates a unified database schema for football statistics of teams and players in the English premier League for the 24/25 season. It extracts raw football data, processes it, and loads it into a PostgreSQL database with proper relationships between teams, players, leagues, and seasons.

## Features

- **Automated Schema Creation**: Dynamically generates database tables based on CSV structure
- **Composite Key Architecture**: Maintains proper relationships between entities across seasons
- **Docker Integration**: PostgreSQL database runs in containerized environment
- **Flexible Data Processing**: Handles both team and player statistics
- **Modular Design**: Separated extract, transform, and load components

## Tech Stack

- **Python**: Core programming language
- **PostgreSQL**: Database for storing processed football statistics
- **pandas**: Data manipulation and transformation
- **psycopg2**: PostgreSQL database adapter
- **Docker**: Containerization for database services

## Project Structure

```
Football-Data-ETL/
.
├── src
│   ├── extract                             
│   │   ├── extract_player_stats.py         # Extract raw player data
│   │   └── extract_teams_stats.py          # Extract raw team data
│   ├── transform                           
│   │   ├── transform_player_stats.py       # Processed player stats
│   │   └── transform_team_stats.py         # Processed team stats
│   └── load                                
│       └── schema.py                       # Tables & schema creation
├── config.py                               # Database configuration
├── connect.py                              # Database connection settings
├── docker-compose.yml                      # Docker configuration
└── requirements.txt                        # Required dependencies
```

## Getting Started

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- PostgreSQL client (optional, for direct database access)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/Football-Data-ETL.git
   cd Football-Data-ETL
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables (create a .env file):
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=football_stats
   DB_USER=postgres
   DB_PASSWORD=your_password
   ```

4. Start the PostgreSQL database:
   ```bash
   docker-compose up -d
   ```

5. Create the database schema:
   ```bash
   python -m src.load.schema
   ```

## Database Structure

The database uses a `football` schema with the following key tables:

- **Reference Tables**:
  - `leagues`: League information
  - `seasons`: Season information
  - `teams`: Team information with league and season references
  - `players`: Player information with team, league, and season references

- **Statistics Tables**:
  - `team_*`: Various team statistics tables (automatically generated)
  - `player_stats`: Player statistics across different categories

## Usage

### Creating the Database Schema

```python
from src.load.schema import create_schema

# Create the entire database schema
create_schema()
```

### Accessing the Database

Connect to the database using psql:

```bash
docker exec -it football_stats_db psql -U postgres -d football_stats
```

## Future Plans

1. **Multi-League and Multi-Season Support**: 
   - Expand coverage beyond the English Premier League
   - Support historical data across multiple seasons
   - Implement standardized data structures for cross-league analysis

2. **Apache Airflow Integration**: 
   - Implement workflow orchestration for regular data updates
   - Add scheduling and monitoring capabilities
   - Create DAGs for different data processing workflows

3. **Machine Learning with PySpark**:
   - Develop predictive models for game outcomes
   - Implement player performance prediction
   - Create feature engineering pipelines for model training

4. **CI/CD with GitHub Actions**:
   - Automated testing of ETL components
   - Continuous deployment to cloud infrastructure
   - Quality checks for data integrity

5. **Data Visualization Dashboard**:
   - Build interactive dashboards for statistics exploration
   - Create APIs to expose processed data
   - Deploy metrics to monitor pipeline performance and data quality

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.