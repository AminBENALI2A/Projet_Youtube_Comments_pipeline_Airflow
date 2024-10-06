import pandas as pd
from transformers import pipeline

def analyze_sentiment(comments, sentiment_pipeline):
    if isinstance(comments, str):  # If a single string is provided
        results = sentiment_pipeline([comments])  # Wrap the string in a list
    elif isinstance(comments, list):
        # Filter comments to only include strings
        string_comments = [comment for comment in comments if isinstance(comment, str)]
        results = sentiment_pipeline(string_comments)  # Process only string comments
    else:
        results = []  # Empty list if comments are not provided
    labels = [result['label'] if result else None for result in results]
    return labels

def Analyse():
    # Load sentiment analysis pipeline
    sentiment_pipeline = pipeline("text-classification", model="/opt/airflow/models/models--finiteautomata--bertweet-base-sentiment-analysis")

    # Load merged CSV file
    merged_csv_path = '/opt/airflow/dags/mergedCsv/merged_comments.csv'
    merged_df = pd.read_csv(merged_csv_path)

    # Add sentiment labels to DataFrame
    merged_df['Sentiment'] = analyze_sentiment(merged_df['Comment'].tolist(), sentiment_pipeline)

    # Write results to new CSV file
    output_csv_path = '/opt/airflow/dags/mergedCsv/sentiment_analysis_results.csv'
    merged_df.to_csv(output_csv_path, index=False)
    print(f"Sentiment analysis complete. Results saved to {output_csv_path}")

