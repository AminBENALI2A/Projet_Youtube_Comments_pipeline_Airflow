import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF

def OurDashBoard():
    # Define paths/opt/airflow/dags/mergedCsv
    sentiment_file_path = '/opt/airflow/dags/mergedCsv/sentiment_analysis_results.csv'
    summaries_file_path = '/opt/airflow/dags/mergedCsv/channel_summaries.csv'
    plot_file_path = '/opt/airflow/dags/mergedCsv/sentiment_analysis_plot.png'
    pdf_output_path = '/opt/airflow/dags/mergedCsv/dashboard.pdf'

    # Load the CSV file for sentiment analysis
    data = pd.read_csv(sentiment_file_path)

    # Group data by ChannelName and Sentiment
    grouped = data.groupby(['ChannelName', 'Sentiment']).size().unstack(fill_value=0)

    # Plotting sentiment analysis results
    fig, ax = plt.subplots(figsize=(10, 6))
    grouped.plot(kind='bar', stacked=True, ax=ax)
    ax.set_title('Number of Positive, Negative, and Neutral Comments per Channel')
    ax.set_xlabel('Channel Name')
    ax.set_ylabel('Number of Comments')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(plot_file_path)
    plt.close()

    # Load summaries from CSV
    summaries_df = pd.read_csv(summaries_file_path)

    # Define a PDF class
    class PDF(FPDF):
        def header(self):
            self.set_font('Arial', 'B', 12)
            self.cell(0, 10, 'Channels Analysis Dashboard', 0, 1, 'C')

        def footer(self):
            self.set_y(-15)
            self.set_font('Arial', 'I', 8)
            self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    # Create instance of FPDF class & add a page
    pdf = PDF()
    pdf.add_page()

    # Add the plot to the PDF
    pdf.image(plot_file_path, x=10, y=20, w=180)

    # Add a new page for summaries
    pdf.add_page()
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'Summaries', 0, 1, 'L')

    # Add summaries to the PDF
    for index, row in summaries_df.iterrows():
        pdf.set_font('Arial', 'B', 10)
        pdf.cell(0, 10, f"{row['ChannelName']}", 0, 1, 'L')
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, row['Summary'])
        pdf.ln(10)

    # Output the PDF to file
    pdf.output(pdf_output_path)
    print(f"Dashboard PDF saved to: {pdf_output_path}")
