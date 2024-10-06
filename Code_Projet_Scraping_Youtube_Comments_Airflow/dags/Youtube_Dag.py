from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime
import Comments_scraping
import Cleaning_comments
import Sentiment_Analysis
import Summerization
import to_audio
import OurDashBoard

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

# Define the DAG
dag = DAG(
    'youtube_comments_Sentiment_analysis',
    default_args=default_args,
    description='Scraping, cleaning, analysing and presenting a Dashboard',
    schedule_interval=None,
)

# Define tasks to execute the producer and consumer functions
Scraping = PythonOperator(
    task_id='Scraping_comments',
    python_callable=Comments_scraping.scrape_comments,
    op_kwargs={'output_dir': "/opt/airflow/dags/CommentsDestination"},
    dag=dag
)

# Define tasks to execute the producer and consumer functions
Cleaning = PythonOperator(
    task_id='Cleaning_comments',
    python_callable=Cleaning_comments.Clean,
    dag=dag
)

# Define tasks to execute the producer and consumer functions
Analysing = PythonOperator(
    task_id='Analysing_comments',
    python_callable=Sentiment_Analysis.Analyse,
    dag=dag
)

Summerizing = PythonOperator(
    task_id='Summerizing_comments',
    python_callable=Summerization.Summarize,
    dag=dag
)

Speech = PythonOperator(
    task_id='to_audio',
    python_callable=to_audio.to_audio,
    dag=dag
)
dashing = PythonOperator(
    task_id='DashBoard',
    python_callable=OurDashBoard.OurDashBoard,
    dag=dag
)


# Set the task dependencies
Scraping >> Cleaning >> [Analysing,Summerizing]
Summerizing >> Speech 
[Analysing,Speech]>> dashing