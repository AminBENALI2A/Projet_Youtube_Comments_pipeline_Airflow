# Start with the Airflow base image
FROM apache/airflow:latest

# Switch to the airflow user
USER airflow

# Install additional Python dependencies for Airflow
RUN pip install torch pandas tranformers scipy fpdf spacy
