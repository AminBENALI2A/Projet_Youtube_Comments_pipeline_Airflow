import os
import pandas as pd
from transformers import pipeline

def Summarize():
    # Load the merged comments DataFrame
    input_file_path = "/opt/airflow/dags/mergedCsv/merged_comments.csv"
    comments_df = pd.read_csv(input_file_path)

    # Initialize the summarization pipeline
    model_path = "/opt/airflow/models/models--Falconsai--text-summarization"
    summarizer = pipeline("summarization", model=model_path)

    # Prepare the output DataFrame
    summary_df = pd.DataFrame(columns=['ChannelName', 'Summary'])

    # Group comments by ChannelName and summarize each group
    grouped_comments = comments_df.groupby('ChannelName')
    for name, group in grouped_comments:
        # Combine all comments for the channel into a single string
        all_comments_text = "\n".join(group["Comment"].tolist())

        # Generate summary for the comments
        summary = summarizer(all_comments_text, max_length=1024, min_length=50, do_sample=False)
        summary_text = summary[0]["summary_text"]

        # Append the results to the summary DataFrame
        summary_df = pd.concat([summary_df, pd.DataFrame({'ChannelName': [name], 'Summary': [summary_text]})], ignore_index=True)

    # Save the summaries to a new CSV file
    output_dir = "/opt/airflow/dags/mergedCsv"
    output_file = "channel_summaries.csv"
    summary_df.to_csv(os.path.join(output_dir, output_file), index=False)

