import json
import os
import urllib.request
import boto3
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    api_key = os.environ['YOUTUBE_API_KEY']
    bucket_name = os.environ['S3_BUCKET_BRONZE']
    # .lower() ensures that even if you typed 'US' or 'Us', it turns into 'us'
    regions = os.environ['YOUTUBE_REGIONS'].lower().split(',')
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    current_hour = datetime.now().strftime('%H')
    
    for region in regions:
        # The YouTube API expects uppercase region codes for the API call, so we use .upper() here
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&chart=mostPopular&regionCode={region.upper()}&maxResults=50&key={api_key}"
        
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                
            # Forces LOWERCASE region string for your S3 folder path
            s3_key = f"youtube/raw_statistics/region={region.lower()}/date={current_date}/hour={current_hour}/raw_trending_videos.json"
            
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(data)
            )
            print(f"Successfully uploaded trending data for region {region} to {s3_key}")
            
        except Exception as e:
            print(f"Error processing region {region}: {str(e)}")
            raise e
            
    return {
        'statusCode': 200,
        'body': json.dumps('Ingestion completed successfully!')
    }