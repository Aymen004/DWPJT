# Bank Agency Reviews Data Warehouse - Setup Guide

This guide provides step-by-step instructions to set up and run the Bank Agency Reviews Data Warehouse project.

## Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher
- Apache Airflow 2.x
- dbt (Data Build Tool) 1.5.x
- Looker Studio account

## Project Setup

### 1. Clone the repository

Start by cloning this repository to your local machine.

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Set up PostgreSQL

1. Create a new PostgreSQL database:

```sql
CREATE DATABASE bank_reviews_dw;
```

2. Run the database schema script:

```bash
psql -U your_username -d bank_reviews_dw -f database/schema.sql
```

### 4. Configure Airflow

1. Set AIRFLOW_HOME environment variable:

```bash
export AIRFLOW_HOME=/path/to/project/airflow
```

2. Initialize the Airflow database:

```bash
airflow db init
```

3. Create an Airflow user:

```bash
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

4. Configure PostgreSQL connection in Airflow:
   - Open the Airflow web UI: `http://localhost:8080`
   - Go to Admin > Connections
   - Add a new connection:
     - Conn Id: postgres_default
     - Conn Type: Postgres
     - Host: your_postgres_host
     - Schema: bank_reviews_dw
     - Login: your_postgres_username
     - Password: your_postgres_password
     - Port: 5432

5. Start Airflow services:

```bash
airflow webserver --port 8080
airflow scheduler
```

### 5. Configure DBT

1. Create a profiles.yml file in your ~/.dbt/ directory:

```yaml
default:
  target: dev
  outputs:
    dev:
      type: postgres
      host: your_postgres_host
      user: your_postgres_username
      password: your_postgres_password
      port: 5432
      dbname: bank_reviews_dw
      schema: public
      threads: 4
```

2. Install DBT packages:

```bash
cd dbt
dbt deps
```

## Running the Project

### 1. Data Collection

You can run the data collection script manually:

```bash
python data_collection/google_maps_scraper.py --output reviews.json --banks "Attijariwafa Bank,Bank of Africa,CIH Bank"
```

Or let Airflow handle it according to the schedule (weekly by default).

### 2. Data Transformation with DBT

Run the DBT transformations to create the data warehouse:

```bash
cd dbt
dbt run
```

This will execute all the DBT models and build the star schema in the database.

### 3. Viewing the Dashboard

1. Open Looker Studio: https://lookerstudio.google.com/
2. Create a new report
3. Connect to your PostgreSQL database using the connection details
4. Use the dashboard specifications from `dashboard/dashboard_specifications.md` as a guide to build your visualizations

## Project Maintenance

### Scheduling Updates

The Airflow DAG is configured to run weekly to collect new reviews. You can modify the schedule in the DAG file:

```python
schedule_interval=timedelta(days=7)  # Adjust as needed
```

### Adding New Banks

To add new banks for review collection, update the BANK_LIST variable in the Airflow DAG:

```python
BANK_LIST = "Attijariwafa Bank,Bank of Africa,BMCE Bank,CIH Bank,Crédit Agricole du Maroc,YOUR_NEW_BANK"
```

### Troubleshooting

If you encounter any issues:

1. Check the Airflow logs in the web UI
2. For DBT issues, run `dbt debug` to check your configuration
3. Verify database connections and credentials
4. Ensure the Google Maps scraper has the correct selectors for the current Google Maps HTML structure

## Next Steps

Consider enhancing the project with:

1. More advanced NLP processing for better topic extraction
2. Integration with a proper sentiment analysis API
3. Automated alerts for negative reviews
4. Mobile app for real-time monitoring