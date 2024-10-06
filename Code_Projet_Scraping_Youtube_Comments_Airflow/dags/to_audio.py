from transformers import VitsModel, AutoTokenizer
import torch
import scipy.io.wavfile as wavfile
import os
import pandas as pd

def to_audio():
    # Load the MMS English TTS model and tokenizer
    model = VitsModel.from_pretrained("/opt/airflow/models/models--facebook--mms-tts-eng")
    tokenizer = AutoTokenizer.from_pretrained("/opt/airflow/models/models--facebook--mms-tts-eng")

    # Define the path to your CSV file containing summaries
    input_csv_file = "/opt/airflow/dags/mergedCsv/channel_summaries.csv"

    # Load summaries from CSV
    summaries_df = pd.read_csv(input_csv_file)

    # Define the output directory for audio files
    output_dir = "/opt/airflow/dags/audios"
    os.makedirs(output_dir, exist_ok=True)

    # Process each summary
    for index, row in summaries_df.iterrows():
        channel_name = row['ChannelName']
        summary_text = row['Summary']

        # Tokenize the summary text
        inputs = tokenizer(summary_text, return_tensors="pt")

        # Generate speech waveform
        with torch.no_grad():
            output = model(**inputs).waveform

        # Rescale the audio data to the range [-1, 1]
        audio_data = output.squeeze().numpy()
        audio_data /= abs(audio_data).max()

        # Define the output file path for the synthesized speech
        output_file_path = os.path.join(output_dir, f"{channel_name}_summary.wav")

        # Save the synthesized speech as a WAV file
        wavfile.write(output_file_path, model.config.sampling_rate, audio_data)

        print(f"Speech for {channel_name} saved to: {output_file_path}")

