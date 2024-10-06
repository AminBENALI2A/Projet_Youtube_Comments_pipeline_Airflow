import pandas as pd
import os
import re
import numpy as np
import csv

def clean_comment(comment):
    # Check if the comment is NaN
    if isinstance(comment, float) and np.isnan(comment):
        return ""  # Return empty string for NaN values
    # Remove emojis
    comment = re.sub(r'[^\x00-\x7F]+', '', str(comment))
    # Remove punctuation
    comment = re.sub(r'[^\w\s]', '', comment)
    return comment.strip()

def Clean():
    # Path to the folder containing CSV files
    input_folder = '/opt/airflow/dags/CommentsDestination'
    # Path to the folder where you want to save the merged CSV file
    output_folder = '/opt/airflow/dags/mergedCsv'
    # Name of the merged CSV file
    output_file = 'merged_comments.csv'

    # Initialize the output CSV file
    output_path = os.path.join(output_folder, output_file)
    with open(output_path, 'w') as output_csv:
        # Write the header to the output CSV file
        output_csv.write("ChannelName,VideoTitle,PublishedAt,Comment\n")

        # Iterate over each file in the input folder
        for filename in os.listdir(input_folder):
            if filename.endswith('.csv'):
                file_path = os.path.join(input_folder, filename)
                try:
                    # Read comments from CSV file
                    df = pd.read_csv(file_path)  # Specify encoding if needed
                    # Clean comments and write directly to the output CSV file
                    for index, row in df.iterrows():
                        cleaned_comment = clean_comment(row['Comment'])
                        if cleaned_comment:  # Check if the comment is not empty
                            output_csv.write(f"{row['ChannelName']},{row['VideoTitle']},{row['PublishedAt']},\"{cleaned_comment}\"\n")
                except Exception as e:
                    print("error : " + str(e))
                    continue
    with open(output_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        filtered_rows = [row for row in reader if len(row) == 4]

    # Overwriting the CSV with the filtered rows
    with open(output_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(filtered_rows)
    df = pd.read_csv(output_path)
    df = df.head(300)
    # Step 5: Save the DataFrame to the original file path, overwriting the original file
    df.to_csv(output_path, index=False)
    print("Cleaning and merging completed.")
