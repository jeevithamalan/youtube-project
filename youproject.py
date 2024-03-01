from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st



def Api_connect():
    Api_Id='AIzaSyDdLr4sjl8NHnpUrTyIx_0Zg-Q0h5ChpR8'

    api_service_name='youtube'
    api_version='v3'
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube 

youtube=Api_connect()

##get channel info

def get_channel_info (channel_id):
  request=youtube.channels().list(
         part='snippet,ContentDetails, statistics' ,
         id=channel_id)

  response=request.execute()
 
  for i in response['items']:
       information=(dict(channel_name=i['snippet']['title'],
                     channel_id= i['id'],
                     subscribers=i['statistics']['subscriberCount'],
                     views=i['statistics']['viewCount'],
                     total_videos=i['statistics']['videoCount'],
                     channel_description=i['snippet']['description'],
                     playlist_id=i['contentDetails']['relatedPlaylists']['uploads']))
          
  return information

##get videos ids
def get_videos_ids(channel_id):

   video_ids=[]
   response=youtube.channels().list(
         id=channel_id,
         part='ContentDetails').execute()
   Paylist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']


   next_page_token=None

   while True:
       response1=youtube.playlistItems().list(
                         part='snippet',
                         playlistId=Paylist_Id,
                         maxResults=50,
                         pageToken=next_page_token).execute()

       for i in range(len(response1['items'])):
           video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
       next_page_token=response1.get('nextpageToken')
       
       if next_page_token is None:
            break
         
   return video_ids

##get video info
def get_video_info(video_ids):
  video_data=[]
  for video_id in video_ids:
 
    request=youtube.videos().list(
              part='snippet,contentDetails,statistics',
              id=video_id
    )
    response=request.execute()


    for item in response['items']:
        data=dict(Channel_Name=item['snippet']['channelTitle'],
                  channel_Id=item['snippet']['channelId'],
                  Video_Id=item['id'],
                  Title=item['snippet']['title'],
                  Tags=item['snippet'].get('tags'),
                  Thumbnail=item['snippet']['thumbnails']['default']['url'],
                  Description=item['snippet'].get('description'),
                  Published_Date=item['snippet']['publishedAt'],
                  Duration=item['contentDetails']['duration'],
                  Views=item['statistics'].get('viewCount'),
                  Likes=item['statistics'].get('likeCount'),
                  Comments=item['statistics'].get('commentCount'),
                  Favorite_Count=item['statistics']['favoriteCount'],
                  Definition=item['contentDetails']['definition'],
                  Caption_Status=item['contentDetails']['caption']
                  )
        video_data.append(data)
    return video_data
    

##get comment information

def get_comment_info(video_ids):
    Comment_Data=[]

    try:
      for video_id in video_ids:
          request=youtube.commentThreads().list(
              part='snippet',
              videoId=video_id,
              maxResults=50

           )
          response=request.execute()

          for item in response['items']:
              data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                      Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                      Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                      Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                      Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
              Comment_Data.append(data)
    except:
      pass
    return Comment_Data

###get playlist details

def get_playlist_details(channel_id):
 next_page_token=None
 All_data=[]
 while True:
  request = youtube.playlists().list(
    part='snippet,contentDetails',
    channelId=channel_id,
    maxResults=50,
    pageToken=next_page_token

  )

  response=request.execute()

  for item in response['items']:
    data=dict(Playlist_Id=item['id'],
              Title=item['snippet']['title'],
              Channel_Id=item['snippet']['channelId'],
              Channel_Name=item['snippet']['channelTitle'],
              PublishedAt=item['snippet']['publishedAt'],
              Video_Count=item['contentDetails']['itemCount'])
    All_data.append(data)

  next_page_token=response.get('nextPageToken')
  if next_page_token is None:
   break
 return All_data


##mongodb details

client=pymongo.MongoClient("mongodb://jeevitha:Prajee123@ac-a1fmbij-shard-00-00.fl2ujl6.mongodb.net:27017,ac-a1fmbij-shard-00-01.fl2ujl6.mongodb.net:27017,ac-a1fmbij-shard-00-02.fl2ujl6.mongodb.net:27017/?ssl=true&replicaSet=atlas-xacfq8-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
                            
db=client["youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details= get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_info=get_video_info(vi_ids)
    com_info=get_comment_info(vi_ids)

    collection1=db['youtube_data']
    collection1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_info,"comment_information":com_info})


    return "upload completed successfully"

##channel table

def channels_table():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="root",
                        database="youtube_details",port="5432")
    cursor=mydb.cursor()

    

    try:
        create_query="""create table channels(channel_name varchar(100) ,
                                                            channel_id varchar(80) primary key,
                                                            subscribers bigint,
                                                            views bigint,
                                                            total_videos int,
                                                            channel_description text,
                                                            playlist_id varchar(80)
                                                            )"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        print('channels table already created') 
        



    ch_list=[]
    db=client["youtube_data"]
    collection1=db["youtube_data"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    
    df=pd.DataFrame(ch_list)



    for index,row in df.iterrows():
        insert_query=''' insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['channel_name'],
                row['channel_id'],
                row['subscribers'],
                row['views'],
                row['total_videos'],
                row['channel_description'],
                row['playlist_id'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('channels values are already inserted')

## playlist table
def playlist_table():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="root",
                        database="youtube_details",port="5432")
    cursor=mydb.cursor()


    create_query="""create table if not exists playlists(Playlist_Id varchar(100)primary key ,
                                                            Title varchar(80) ,
                                                            Channel_Id varchar(100),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_Count int
                                                            )"""
    cursor.execute(create_query)
    mydb.commit()   

    pl_list=[]
    db=client["youtube_data"]
    collection1=db["youtube_data"]
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
           pl_list.append(pl_data["playlist_information"][i])
            
    df1=pd.DataFrame(pl_list)
    print(pl_list)
    df1

    for index,row in df1.iterrows():
            insert_query=''' insert into playlists(Playlist_Id ,
                                        Title,
                                        Channel_Id,
                                        Channel_Name,
                                        PublishedAt,
                                        Video_Count
                                        )
                                        values(%s,%s,%s,%s,%s,%s)'''


            values=(row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count'])


            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                print('channels values are already inserted')


##video table

def videos_table():
        mydb=psycopg2.connect(host="localhost",user="postgres",password="root",
                        database="youtube_details",port="5432")
        cursor=mydb.cursor()
    
        create_query="""create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30),
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(150),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(30),
                                                        Caption_Status varchar(50)
                                                        )"""
        cursor.execute(create_query)
        mydb.commit()
      

        vi_list=[]
        db=client["youtube_data"]
        collection1=db["youtube_data"]
        for vi_data in collection1.find({},{"_id":0,"video_information":1}):
                for i in range (len(vi_data["video_information"])):
                   vi_list.append(vi_data["video_information"][i])
        df=pd.DataFrame(vi_list)
        print(vi_list)
        
        print("step 1")
        for index,row in df.iterrows():
                insert_query=''' insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status
                                                )
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                values=(row['Channel_Name'],
                        row['channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status'])
                try:
                        cursor.execute(insert_query,values)
                        mydb.commit()
                except:
                    print('channels values are already inserted')


##comment table
                    
def comments_details():
        mydb=psycopg2.connect(host="localhost",user="postgres",password="root",
                            database="youtube_details",port="5432")

        cursor=mydb.cursor()
        
   
        create_query="""create table if not exists comments(Comment_Id varchar(100) primary key,
                                                                Video_Id varchar(50),
                                                                Comment_Text text,
                                                                Comment_Author varchar(150),
                                                                Comment_Published timestamp
                                                                )"""
                                                          
        cursor.execute(create_query)
        mydb.commit()
   

        com_list=[]
        db=client["youtube_data"]
        collection1=db["youtube_data"]
        for com_data in collection1.find({},{"_id":0,"comment_information":1}):
            for i in range (len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])
        df3=pd.DataFrame(com_list)
        
        print("Step 1")
        for index,row in df3.iterrows():
                print(index)
                print(row)
                insert_query=''' insert into comments(Comment_Id ,
                                                        Video_Id ,
                                                        Comment_Text ,
                                                        Comment_Author,
                                                        Comment_Published
                                                    )                                                                                            
                                                    values(%s,%s,%s,%s,%s)'''


                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                    )
                try:
                        cursor.execute(insert_query,values)
                        mydb.commit()
                except:
                    print('comments are already inserted')

##tables creation
                    
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_details()

    return "tables created successfully"

##show_channels_table

def show_channels_table():
    ch_list=[]
    db=client["youtube_data"]
    collection1=db["youtube_data"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    
    df=st.dataframe(ch_list)
    return df

## show_playlist_table

def show_playlist_table():
    pl_list=[]
    db=client["youtube_data"]
    collection1=db["youtube_data"]
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
           pl_list.append(pl_data["playlist_information"][i])
            
    df=st.dataframe(pl_list)
    return df

##show_videos_table

def show_videos_table():
    vi_list=[]
    db=client["youtube_data"]
    collection1=db["youtube_data"]
    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
            for i in range (len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
    df=st.dataframe(vi_list)
   
    return df

##show_comments_details

def show_comments_details():
    com_list=[]
    db=client["youtube_data"]
    collection1=db["youtube_data"]
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        for i in range (len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df=st.dataframe(com_list)
    return df

####STREAMLIT PART

with st.sidebar:
    st.title(":orange[YOUTUBE HAVERSTING AND WAREHOUSING]")
    st.header("skills")
    st.caption("Python")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management Using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    collection1=db["youtube_data"]
    for ch_data in collection1.find({},{"_id:0","channel_information"}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])

    if channel_id in ch_ids:
         st.success("Channel Details of the given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert) 

if st.button("Migrate to sql"):
    table=tables()
    st.success(table)
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_details()


####sql connection

mydb=psycopg2.connect(host="localhost",user="postgres",password="root",
                    database="youtube_details",port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                                               "2.Which channels have the most number of videos and how many videos do they have?",
                                               "3.What are the top 10 most viewed videos and their respective channels?",
                                               "4.How many comments were made on each video, and what are their corresponding video names?",
                                               "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                               "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8.What are the names of all the channels that have published videos in the year 2022?",
                                               "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                               "10.Which videos have the highest number of comments, and what are their corresponding channel names?"),
                                                index=None,
                                                placeholder="Select Your Question...",)

if question=="1.What are the names of all the videos and their corresponding channels?":

    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    q1=cursor.fetchall()
    df=pd.DataFrame(q1,columns=["video title","channel name"])
    st.write(df)


elif question== "2.Which channels have the most number of videos and how many videos do they have?":

    query2='''select channel_name  as channelname,total_videos as no_videos from channels
            order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    q2=cursor.fetchall()
    df2=pd.DataFrame(q2,columns=["channel name","No of videos"])
    st.write(df2)


elif question=="3.What are the top 10 most viewed videos and their respective channels?":
   query3='''select views as views ,channel_name as channelname,title as videotitle 
           from videos where views is not null order by views desc limit 10'''
   cursor.execute(query3)
   mydb.commit()
   q3=cursor.fetchall()
   df3=pd.DataFrame(q3,columns=[" views","channel name","videotitle"])
   st.write(df3)

elif question== "4.How many comments were made on each video, and what are their corresponding video names?":
        query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
        cursor.execute(query4)
        mydb.commit()
        q4=cursor.fetchall()
        df4=pd.DataFrame(q4,columns=["no of comments","videotitle"])
        st.write(df4)

elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?" :
        query5='''select title as videotitle,channel_name as channelname,likes as likecount from
                videos where likes is not null order by likes  desc '''
        cursor.execute(query5)
        mydb.commit()
        q5=cursor.fetchall()
        df5=pd.DataFrame(q5,columns=["videotitle","channelname","likecount"])
        st.write(df5)

elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query6='''select likes as likecount, title as videotitle from videos'''
        cursor.execute(query6)
        mydb.commit()
        q6=cursor.fetchall()
        df6=pd.DataFrame(q6,columns=["likecount","videotitle"])
        st.write(df6)

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
        query7='''select channel_name as channelname,views as totalviews from channels '''
        cursor.execute(query7)
        mydb.commit()
        q7=cursor.fetchall()
        df7=pd.DataFrame(q7,columns=["channel name","totalviews"])
        st.write(df7)

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
        query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                        where extract (year from published_date) =2022'''
        cursor.execute(query8)
        mydb.commit()
        q8=cursor.fetchall()
        df8=pd.DataFrame(q8,columns=["videotitle","published_date","channelname"])
        st.write(df8)

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
        cursor.execute(query9)
        mydb.commit()
        q9=cursor.fetchall()
        df9=pd.DataFrame(q9,columns=["channelname",("avergeduration")])
        df9
        
        q9=[]
        for index,row  in df9.iterrows():
                channel_title=row["channelname"]
                average_duration=row["avergeduration"]
                average_duration_str=str(average_duration)
                q9.append(dict(channeltitle=channel_title,averageduration=average_duration_str))
        df1=pd.DataFrame
        st.write(df1)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    q10=cursor.fetchall()
    df10=pd.DataFrame(q10,columns=["video title","channel name","comments"])
    st.write(df10)







    

   
        



                                  




     

