

from googleapiclient.discovery import build
youtube = build('youtube', 'v3', developerKey="AIzaSyBgfB31GN_OSPIaK1P86VScqkXG8-5siBQ")

# channel_id = 'UCCj956IF62FbT7Gouszaj9w'
# channel_id='UC_x5XG1OV2P6uZZ5FSM9Ttw'
channel_id="UCMfRfVxPrb0mMRzzfi6r2Cg"

channel = youtube.channels().list(
    part='snippet, statistics',
    id=channel_id
).execute()

video_ids_list_by_playlistid = []
video_title_list_by_playlist_id = []
dictionary_of_video_details_by_videoid = {}


#-----------GEt relevant channel info--------------------
channel_name = channel['items'][0]['snippet']['title']
subscription_count = channel['items'][0]['statistics']['subscriberCount']
channel_views = channel['items'][0]['statistics']['viewCount'] #viewCount
channel_desc=channel['items'][0]['snippet']['description']




# print(channel_name,subscription_count,channel_views,"\n",channel_desc,'\n',channel)

#making channel dictionary---------------------------

from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Select the database
db = client['Youtubeproj']
collection = db[f'{channel_id}']
# collection = db.create_collection(f"{channel_name}")
# db[channel_name].drop()




# print(f"Inserted document IDs: {result.inserted_ids}")

#--------------make list dic------------------

# Call the API to retrieve playlists of the channel
playlists = youtube.playlists().list(
    part='snippet',
    channelId=channel_id,
    maxResults=50  # Maximum number of playlists to retrieve (adjust as needed)
).execute()
# print(playlists)
playlistid_list=[]
playlisttitle_list=[]
# Iterate through each playlist and print its title and ID
for playlist in playlists['items']:
    playlist_id = playlist['id']
    playlist_title = playlist['snippet']['title']
    # print(f"Playlist ID: {playlist_id}, Title: {playlist_title}")
    # playlist_dic={playlist_id:playlist_title}
    playlistid_list.append(playlist_id)
    playlisttitle_list.append(playlist_title)

playlist_dic=dict(zip(playlistid_list, playlisttitle_list))
# print(playlist_dic)




videos_byplaylist_dictionary={}
#------------- putting videos by playlist here--------------------------
for each in playlistid_list:
    next_page_token = None
    videos = []

    # Retrieve all videos in the playlist
    while True:
        playlist_items = youtube.playlistItems().list(
            part='snippet',
            playlistId=each,
            maxResults=50,  # Maximum number of videos per page
            pageToken=next_page_token
        ).execute()

        videos.extend(playlist_items['items'])

        next_page_token = playlist_items.get('nextPageToken')

        if not next_page_token:
            break


    # Print video information
    for video in videos:
        video_id = video['snippet']['resourceId']['videoId']
        video_title = video['snippet']['title']
        # print(f"Video ID: {video_id}, Title: {video_title}")
        video_ids_list_by_playlistid.append(video_id)
        video_title_list_by_playlist_id.append(video_title)
    videos_dic = dict(zip(video_ids_list_by_playlistid, video_title_list_by_playlist_id))
    videos_byplaylist_dictionary[each]=videos_dic

# print(videos_byplaylist_dictionary)



#------- make dictionary of videodata by videoids--------

for each in video_ids_list_by_playlistid:
    # Call the API to retrieve video details
    video_details = youtube.videos().list(
        part='snippet,statistics,contentDetails',
        id=each
    ).execute()
    # print(video_details)

    # Extract relevant information from the response
    try:
        snippet = video_details['items'][0]['snippet']
        statistics = video_details['items'][0]['statistics']
        content_details = video_details['items'][0]['contentDetails']

        # Extract specific details

        video_description = snippet['description']
        video_duration = content_details['duration']
        like_count = statistics['likeCount']
        # dislike_count = statistics['dislikeCount']
        favorite_count = statistics['favoriteCount']
        published_date = snippet['publishedAt']
        thumbnail_url = snippet['thumbnails']['default']['url']
        # caption_status = snippet['caption']
        # tags = snippet['tags']
        comment_count = statistics['commentCount']
        video_url = f"https://www.youtube.com/watch?v={each}"

        # Retrieve individual comments
        comments = youtube.commentThreads().list(
            part='snippet',
            videoId=each,
            maxResults=10
        ).execute()

        # Print individual comments
        comments_list_by_videoid = []
        # dictionary_of_comments_list_by_videoid={}
        # print("Comments:")
        for comment in comments['items']:
            comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay']
            comments_list_by_videoid.append(comment_text)
            # print(comment_text)
        # print(comments_list_by_videoid)

        video_data = {
            "Video ID": each,
            "Description": video_description,
            "Duration": video_duration,
            "Likes": like_count,
            "Favorites": favorite_count,
            "Published Date": published_date,
            "Thumbnail URL": thumbnail_url,
            "Comment Count": comment_count,
            "Video URL": video_url,
            "comments": comments_list_by_videoid
        }
        dictionary_of_video_details_by_videoid[each] = video_data
    except Exception as e:
        print("An error occurred:", e)







channel_dic={"Channel name":channel_name,
             "Channel ID":channel_id,
             "Subscription Count":subscription_count,
             "Channel Views":channel_views,
             "Channel Description":channel_desc,
             "playlists":playlist_dic,
             "Videos by Playlist":videos_byplaylist_dictionary,
             "Video details by video id":dictionary_of_video_details_by_videoid}

result = collection.insert_one(channel_dic)

print(channel_dic)

#-------------pushing to mysql
import pandas as pd

channel_df=pd.DataFrame.from_dict({"Channel ID":[channel_id],
                                   "Channel name":[channel_name],
                                   "Subscription Count":[subscription_count],
                                   "Channel views":[channel_views],
                                   "Channel description":[channel_desc]})
print(channel_df.head())
# channel_df.to_csv("test1.csv")



# print(dictionary_of_video_details_by_videoid)
# print(video_ids_list_by_playlistid)


# import pymongo
# import mysql.connector
#
# mongo_client = pymongo.MongoClient('mongodb://localhost:27017')
# mongo_db = mongo_client['Youtubeproj']
# mongo_collection = mongo_db['UCMfRfVxPrb0mMRzzfi6r2Cg']
#
# mysql_connection = mysql.connector.connect(
#     host='localhost',
#     user='root',
#     password='serendipity',
#     database='youtubeproj'
# )
# mysql_cursor = mysql_connection.cursor()
#
# mongo_documents = mongo_collection.find()
#
# for document in mongo_documents:
#     # Extract relevant data from MongoDB document
#     Channel_name=document['Channel name']
#     ChannelID=document['Channel ID'],
#     SubscriptionCount=document['Subscription Count'],
#     ChannelViews=document['Channel Views'],
#     ChannelDescription=document['Channel Description'],
#     playlists=document['playlists'],
#     VideosbyPlaylist=document['Videos by Playlist'],
#     Videodetailsbyvideoid=document['Video details by video id']
#
#     # Prepare SQL query
#     sql_query = "INSERT INTO your_mysql_table (channel_name, " \
#                 "channel_id,subscription_count,channel_views,channel_desc,playlist_dic" \
#                 ",videos_byplaylist_dictionary,dictionary_of_video_details_by_videoid" \
#                 " ) VALUES (%s, %s, %s,%s,%s,%s,%s,%s)"
#
#     # Execute SQL query with document data
#     mysql_cursor.execute(sql_query, (channel_name, channel_id,
#     subscription_count,channel_views,channel_desc,playlist_dic,videos_byplaylist_dictionary,dictionary_of_video_details_by_videoid))
#
# # Commit and close MySQL connection
# mysql_connection.commit()
# mysql_connection.close()
# print(comment_count)