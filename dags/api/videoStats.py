import requests
import json
from datetime import date

import os
from dotenv import load_dotenv
load_dotenv(dotenv_path="./.env")

from airflow.decorators import task
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
MAX_RESULT = 50

@task
def getPlaylistId():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        print(channel_playlistId)
        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e

@task
def getVideoIds(playlistId):
    videoIds = []
    pageToken = None
    baseURL = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULT}&playlistId={playlistId}&key={API_KEY}'
    try:
        while True:
            url = baseURL
            if pageToken:
                url += f"&pageToken={pageToken}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get("items",[]):
                video_id = item['contentDetails']['videoId']
                videoIds.append(video_id)
            
            pageToken = data.get("nextPageToken")

            if not pageToken:
                break
        return videoIds
    except requests.exceptions.RequestException as e:
        raise e
        
@task
def extractVideoData(videoIds):
    extractedData = []

    def batchList(videoIdList,batchSize):
        for videoId in range(0,len(videoIdList),batchSize):
            yield videoIdList[videoId: videoId + batchSize]
    
    try:
        for batch in batchList(videoIds,MAX_RESULT):
            videoIdsStr = ",".join(batch)
            url =  f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=Snippet&part=Statistics&id=Wl{videoIdsStr}&key={API_KEY}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items',[]):
                videoId = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
                videoData = {
                    "video_id":videoId,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewCount": statistics.get('viewCount',None),
                    "likeCount": statistics.get('likeCount',None),
                    "commentCount": statistics.get('commentCount',None)
                }
                extractedData.append(videoData)
            return extractedData
    except requests.exceptions.RequestException as e:
        raise e

@task
def saveToJson(extractedData):
    filePath = f"./data/youtube_data_{date.today()}.json"
    with open(filePath,"w",encoding="utf-8") as jsonOutfile:
        json.dump(extractedData,jsonOutfile,indent=4,ensure_ascii=False)

if __name__ == "__main__":
    playlistId = getPlaylistId()
    videoIds = getVideoIds(playlistId)
    extractedData = extractVideoData(videoIds)
    saveToJson(extractedData)