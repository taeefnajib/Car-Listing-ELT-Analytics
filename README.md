# B2B SaaS Analytics Pipeline

This project sets up an end-to-end analytics pipeline that loads data from Supabase into Snowflake, transforms it with dbt, and visualizes it in Apache Superset.

## Prerequisites

- Python 3.8+
- PostgreSQL
- Supabase account and credentials
- Snowflake account and credentials

## Setup Instructions

### 1. Set Up Python Environment

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On Unix or MacOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start PostgreSQL Database in Docker

```bash
# Start postgres in docker
docker compose up --build
```

Make sure you have a `secrets.toml` file in the `.dlt` folder with the following structure:


### 3. Set Up Dagster

```bash
# Start Dagster webserver
dagster dev
```

The Dagster UI will be available at http://localhost:3000

### 4. Run the Pipeline

In the Dagster UI:
1. Navigate to the Assets tab
2. Select all assets
3. Click "Materialize Selected"

This will:
- Load data from CSV to PostgreSQL database using dlt
- Run dbt transformations on the loaded data

### 6. Set Up Apache Superset

```bash
# Change directory to superset folder
cd superset

# Make scripts executable
chmod +x setup_db.sh run_superset.sh

# Set up the database
./setup_db.sh

# Start Superset
./run_superset.sh
```

When running `run_superset.sh`, you'll be prompted to create an admin user. Follow the prompts to set up your credentials.

Superset will be available at http://localhost:8088

### 7. Create Dashboard in Superset

1. Log in to Superset using your admin credentials
2. Go to Data → Databases and add your PostgreSQL connection
3. Create new datasets from your analytics table
4. Create charts using these datasets
5. Combine charts into a dashboard

## Troubleshooting

- If you encounter database connection issues, verify your credentials in `secrets.toml` and `profiles.yml`
- For Superset connection issues, check `superset_config.py` settings
- Make sure all required ports are available and not blocked by firewall

## Security Note

Never commit files containing credentials (`secrets.toml`, `profiles.yml`, `superset_config.py`) to version control. Add them to your `.gitignore` file. For demonstration purpose, we have unblocked some of these files.

## Additional Resources

- [dlt Documentation](https://dlthub.com/docs)
- [dbt Documentation](https://docs.getdbt.com)
- [Apache Superset Documentation](https://superset.apache.org/docs/intro)
