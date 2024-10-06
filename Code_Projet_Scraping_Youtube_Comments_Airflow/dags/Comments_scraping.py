import os
import csv
import re
import pandas as pd
from googleapiclient.discovery import build # type: ignore

# Your YouTube API key
API_KEY = 'AIzaSyBtGBWMND_Cjf1hP4KxvNSG5I5bqbZAFCg'

# List of channel IDs
channel_ids = [
    'UCtYLUTtgS3k1Fg4y5tAhLbw' # Statquest

    
]
''', 'UCfzlCWGWYyIQ0aLC5w48gBQ'  # Sentdex     'UCCezIgC97PvUuR4_gbFUs5g' # Corey Schafer
    'UCNU_lfiiWBdtULKOw6X0Dig', # Krish Naik
    'UCLLw7jmFsvfIVaUFsLs8mlQ', # DatascienceDoJo
    'UCiT9RITQ9PW6BhXK0y2jaeg', # Ken Jee
    'UC7cs8q-gJRlGwj4A8OmCmXg', # Alex the analyst
    'UCmOwsoHty5PrmE-3QhUBfPQ'  # Jay Alammar'''

def get_video_info(video_id, youtube):
    request = youtube.videos().list(
        part='snippet',
        id=video_id
    )
    response = request.execute()
    if 'items' in response and response['items']:
        item = response['items'][0]
        channel_name = item['snippet']['channelTitle']
        video_title = item['snippet']['title']
        return channel_name, video_title
    else:
        return None, None

def get_all_comments(video_id, youtube):
    comments = []
    next_page_token = None
    while True:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token
            )
            response = request.execute()
            for item in response["items"]:
                comments.append(item["snippet"]["topLevelComment"]["snippet"])
                if "replies" in item:
                    for reply_item in item["replies"]["comments"]:
                        comments.append(reply_item["snippet"])
            next_page_token = response.get('nextPageToken')
        except:
            print("Error at video with ID:", video_id)
        if not next_page_token:
            break
    return comments

def is_valid_comment(comment):
    if len(comment) > 5000:
        return False
    if re.search(r'https?://|www\.|<[^>]+>', comment):
        return False
    alphabetic_chars = re.findall(r'[a-zA-Z]', comment)
    non_alphabetic_chars = re.findall(r'[^a-zA-Z\s]', comment)
    if len(non_alphabetic_chars) > len(alphabetic_chars):
        return False
    return True

def save_comments_to_csv(video_id, comments, output_dir, youtube):
    filename = os.path.join(output_dir, f"{video_id}_comments.csv")
    channel_name, video_title = get_video_info(video_id, youtube)
    data = {'ChannelName': [], 'VideoTitle': [], 'Comment': [], 'PublishedAt': []}
    for item in comments:
        comment_text = item['textDisplay']
        if is_valid_comment(comment_text):
            data['ChannelName'].append(channel_name)
            data['VideoTitle'].append(video_title)
            data['Comment'].append(comment_text)
            data['PublishedAt'].append(item['publishedAt'])
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding='utf-8')
    # Re-reading the CSV file to filter out rows with incorrect field counts
    with open(filename, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        filtered_rows = [row for row in reader if len(row) == 4]

    # Overwriting the CSV with the filtered rows
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(filtered_rows)

def scrape_comments(output_dir):
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    for channel_id in channel_ids:
        playlist_request = youtube.channels().list(part="contentDetails", id=channel_id)
        playlist_response = playlist_request.execute()
        playlist_id = playlist_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        playlist_items_request = youtube.playlistItems().list(part="contentDetails", playlistId=playlist_id, maxResults=50)
        while playlist_items_request:
            playlist_items_response = playlist_items_request.execute()
            for item in playlist_items_response['items']:
                video_id = item['contentDetails']['videoId']
                comments = get_all_comments(video_id, youtube)
                save_comments_to_csv(video_id, comments, output_dir, youtube)
            playlist_items_request = youtube.playlistItems().list_next(playlist_items_request, playlist_items_response)


