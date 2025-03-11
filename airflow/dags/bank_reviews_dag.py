"""
Airflow DAG for Bank Reviews Collection and Processing

This DAG orchestrates the collection of bank reviews from Google Maps
and loads them into a PostgreSQL database for further processing.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the list of banks to scrape
BANK_LIST = "Attijariwafa Bank,Bank of Africa,BMCE Bank,CIH Bank,Crédit Agricole du Maroc"

# Define the DAG
dag = DAG(
    'bank_reviews_collection',
    default_args=default_args,
    description='Collect bank reviews from Google Maps and store in PostgreSQL',
    schedule_interval=timedelta(days=7),  # Run weekly
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['bank_reviews', 'data_warehouse'],
)

# Task to create a staging table if it doesn't exist
create_staging_table = PostgresOperator(
    task_id='create_staging_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS staging_reviews (
        id SERIAL PRIMARY KEY,
        bank_name VARCHAR(100),
        agency_name VARCHAR(200),
        location VARCHAR(500),
        review_text TEXT,
        rating FLOAT,
        review_date DATE,
        language VARCHAR(10),
        raw_data JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

# Define a function to scrape reviews and return a file path
def scrape_reviews(**kwargs):
    """
    Run the Google Maps scraper to collect bank reviews
    """
    import subprocess
    import os
    from datetime import datetime
    
    # Create a unique output file name
    output_dir = "/tmp/bank_reviews"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = f"{output_dir}/reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Get the correct path to the scraper script, making sure it exists
    base_dir = os.environ.get('AIRFLOW_HOME', '')
    if base_dir:
        script_path = os.path.join(base_dir, '../data_collection/google_maps_scraper.py')
    else:
        # Fallback to a direct path if AIRFLOW_HOME is not set
        script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                              'data_collection', 'google_maps_scraper.py')
    
    # Verify the script exists
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Scraper script not found at {script_path}")
    
    cmd = [
        "python", 
        script_path,
        "--output", output_file,
        "--banks", kwargs['banks']
    ]
    
    # Add headless mode for server environment
    cmd.extend(["--headless"])
    
    # Add cities if provided
    if 'cities' in kwargs:
        cmd.extend(["--cities", kwargs['cities']])
    
    # Add max_reviews if provided
    if 'max_reviews' in kwargs:
        cmd.extend(["--max_reviews", str(kwargs['max_reviews'])])
    
    subprocess.run(cmd, check=True)
    
    # Return the file path to the collected data
    return output_file

# Task to scrape reviews from Google Maps
scrape_reviews_task = PythonOperator(
    task_id='scrape_reviews',
    python_callable=scrape_reviews,
    op_kwargs={
        'banks': BANK_LIST
    },
    dag=dag,
)

# Define a function to load reviews into PostgreSQL
def load_reviews_to_postgres(**kwargs):
    """
    Load the scraped reviews JSON file into PostgreSQL staging table
    """
    import json
    import pandas as pd
    from airflow.hooks.postgres_hook import PostgresHook
    
    # Get the task instance
    ti = kwargs['ti']
    
    # Get the output file path from the previous task
    reviews_file = ti.xcom_pull(task_ids='scrape_reviews')
    
    # Read the JSON file
    with open(reviews_file, 'r', encoding='utf-8') as f:
        reviews = json.load(f)
    
    # Convert to DataFrame for easier processing
    df = pd.DataFrame(reviews)
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Insert each review into staging table
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO staging_reviews 
            (bank_name, agency_name, location, review_text, rating, review_date, language, raw_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row['bank'],
                row['agency_name'],
                row['location'],
                row['text'],
                row['rating'],
                row['date'],
                row['language'],
                json.dumps(row.to_dict())
            )
        )
    
    # Commit the transaction
    conn.commit()
    
    # Close the connection
    cursor.close()
    conn.close()
    
    return f"Loaded {len(df)} reviews into PostgreSQL"

# Task to load reviews into PostgreSQL
load_reviews_task = PythonOperator(
    task_id='load_reviews_to_postgres',
    python_callable=load_reviews_to_postgres,
    provide_context=True,
    dag=dag,
)

# Create task dependencies
create_staging_table >> scrape_reviews_task >> load_reviews_task