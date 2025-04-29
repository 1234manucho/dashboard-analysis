import os
from googleapiclient.discovery import build
from datetime import datetime
import json
import sys
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# --- Configuration ---
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")
OUTPUT_FILENAME = os.getenv("OUTPUT_FILENAME")

def validate_config():
    if not YOUTUBE_API_KEY:
        print("Error: YOUTUBE_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)
    if not CHANNEL_ID:
        print("Error: YOUTUBE_CHANNEL_ID environment variable not set.", file=sys.stderr)
        sys.exit(1)

def build_youtube_client():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def get_playlist_id(youtube, channel_id):
    try:
        request = youtube.channels().list(part="contentDetails", id=channel_id)
        response = request.execute()
        return response['items'][0]['contentDetails']['relatedPlaylists']['uploads'] if response['items'] else None
    except Exception as e:
        print(f"Error fetching channel details: {e}", file=sys.stderr)
        return None

def get_video_ids(youtube, playlist_id):
    video_ids, next_page_token = [], None
    while True:
        try:
            request = youtube.playlistItems().list(
                part="contentDetails", 
                playlistId=playlist_id, 
                maxResults=50, 
                pageToken=next_page_token
            )
            response = request.execute()
            video_ids.extend([item['contentDetails']['videoId'] for item in response['items']])
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except Exception as e:
            print(f"Error fetching playlist items: {e}", file=sys.stderr)
            break
    return video_ids

def get_video_details(youtube, video_ids):
    all_video_data = []
    for i in range(0, len(video_ids), 50):
        video_chunk = ",".join(video_ids[i:i+50])
        try:
            request = youtube.videos().list(part="snippet,statistics", id=video_chunk)
            response = request.execute()
            timestamp = datetime.utcnow().isoformat() + 'Z'
            for item in response['items']:
                video_data = {
                    'videoId': item['id'],
                    'title': item['snippet']['title'],
                    'publishedAt': item['snippet']['publishedAt'],
                    'viewCount': item['statistics'].get('viewCount'),
                    'likeCount': item['statistics'].get('likeCount'),
                    'commentCount': item['statistics'].get('commentCount'),
                    'extractionTimestamp': timestamp
                }
                all_video_data.append(video_data)
        except Exception as e:
            print(f"Error fetching video details: {e}", file=sys.stderr)
    return all_video_data

if __name__ == "__main__":
    print("Starting YouTube data extraction...")

    # Step 1: Validate configuration
    validate_config()

    # Step 2: Build YouTube client
    youtube = build_youtube_client()

    # Step 3: Get the uploads playlist ID
    playlist_id = get_playlist_id(youtube, CHANNEL_ID)
    if not playlist_id:
        print(f"Could not retrieve playlist ID for channel {CHANNEL_ID}.", file=sys.stderr)
        sys.exit(1)

    # Step 4: Fetch video IDs from the playlist
    video_ids = get_video_ids(youtube, playlist_id)
    if not video_ids:
        print(f"No videos found for channel {CHANNEL_ID}.", file=sys.stderr)
        sys.exit(1)

    # Step 5: Get video details
    video_data = get_video_details(youtube, video_ids)
    if not video_data:
        print("No video data fetched.", file=sys.stderr)
        sys.exit(1)

    # Step 6: Save video data to file
    try:
        with open(OUTPUT_FILENAME, 'w') as f:
            json.dump(video_data, f, indent=4)
        print(f"Data saved to {OUTPUT_FILENAME}")
    except IOError as e:
        print(f"Error writing data to {OUTPUT_FILENAME}: {e}", file=sys.stderr)
        sys.exit(1)

    print("Extraction process finished.")
    sys.exit(0)
