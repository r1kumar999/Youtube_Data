from googleapiclient.discovery import build
import psycopg2
import pymongo
import pandas as pd
import streamlit as st

youtube = build("youtube", "v3", developerKey="AIzaSyBTwiyGAw1sBdaWxZrG8hGSXANNofl4LUA")

# GET CHANNEL INFO:
def get_channel_info(channel_id):
    ch_info=[]
    # Request to get channel data
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )

    # Execute the request
    response = request.execute()

    for i in response['items']:
            data01 = dict(
                channel_Name=i['snippet']['title'],
                channel_id=i['id'],
                subcribers_count=i['statistics']['subscriberCount'],
                viewer_count=i['statistics']['viewCount'],
                total_vedios=i['statistics']['videoCount'],
                channel_description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    ch_info.append(data01)
                
    return data01   
            
#get vedio ids:


def get_vedio_ids(channel_id):
        video_idS=[]

        response= youtube.channels().list(
                                        part='contentDetails',
                                        id=channel_id).execute()
        playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token=None
        while True:
                response1=youtube.playlistItems().list(part='snippet',
                                                        playlistId=playlist_id,
                                                        maxResults=50,
                                                        pageToken=next_page_token).execute()
                for i in range(len(response1['items'])):                                              # vedio_ids.append(response1['items'][i]['snippet']['resourceId']['videoId']) 
                        video_idS.append(response1['items'][i]['snippet']['resourceId']['videoId'])       # 'i' change routian to 50 times to check your id's
                next_page_token=response1.get('nextPageToken') 
                
                if next_page_token is None:
                        break  
        return video_idS
    
#get vedio information :
def get_vedio_info(video_ids):
        vedio_data=[]

        for vedio_id in video_ids: 
                request=youtube.videos().list(
                                part='snippet,statistics,contentDetails',
                                id=vedio_id)
                response=request.execute() 
                
                

                for item in response["items"]:
                        data=dict(channel_name=item['snippet']['channelTitle'],
                                channel_Id=item['snippet']['channelId'],
                                vedio_id=item['id'],
                                title=item['snippet']['title'],
                                Tags=item['snippet'].get('tags'),
                                Thumbnails=item['snippet']['thumbnails']['default']['url'],
                                Description=item['snippet'].get('description'),
                                Publiced_date=item['snippet']['publishedAt'],
                                Durations=item['contentDetails']['duration'],
                                View=item['statistics'].get('viewCount'),
                                likes=item['statistics'].get('likeCount'),
                                comments=item['statistics'].get('commentCount'),
                                favorite_count=item['statistics']['favoriteCount'],
                                definition=item['contentDetails']['definition'],
                                caption_status=item['contentDetails']['caption'])
                        vedio_data.append(data)
        return vedio_data
    

def get_comment_details(vedio_ids):
    comment_data = []

    try:
        for video_id in vedio_ids:
            try:
                request = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=50
                )
                response = request.execute()

                # Loop through comments and append to comment_data
                for item in response['items']:
                    data = {
                        'Comment_id': item['snippet']['topLevelComment']['id'],
                        'Vedio_id': item['snippet']['topLevelComment']['snippet']['videoId'],
                        'Comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'Comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_publiced_date': item['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    comment_data.append(data)

            # Catch specific errors
            except Exception as e:
                if "commentsDisabled" in str(e):
                    print(f"Comments are disabled for video ID: {video_id}")
                else:
                    print(f"An error occurred for video ID {video_id}: {e}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return comment_data


# get playlists_details:
def get_playlist_details(channel_id):   
    all_data=[]

    next_page_token=None
    

    while True:
        request=youtube.playlists().list(
                                            part='snippet,contentDetails',
                                            channelId=channel_id,
                                            maxResults=50,
                                            pageToken=next_page_token
                                            )
        response=request.execute()
       
        for item in response['items']:
                    data02=dict(playlist_id=item['id'],
                                title=item['snippet']['title'],
                                channel_id=item['snippet']['channelId'],
                                channel_name=item['snippet']['channelTitle'],
                                publised_at=item['snippet']['publishedAt'],
                                vedio_count=item['contentDetails']['itemCount'])
                    all_data.append(data02)   
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
                break  
    return all_data    


#connect to mongo DB:

client = pymongo.MongoClient('mongodb+srv://r1kumar143123:arun9999@cluster01.0cbdm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster01')     # Set MONGO_URI in your environment
db = client['youtube_data']

# upload to mango DB:

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    ply_details=get_playlist_details(channel_id)
    ve_ids=get_vedio_ids(channel_id)
    ve_details=get_vedio_info(ve_ids)
    co_details=get_comment_details(ve_ids)

    collection=db['channels_details']
    collection.insert_one({'channels_informations':ch_details,'playlist_informations':ply_details,
                           'vedios_details':ve_details,'comments_informatios':co_details})
    
    
    return 'upload successfully informations'


def channels_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="arun9999",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()


    
    create_query='''create table if not exists channels(channel_Name varchar(100),
                                                            channel_id varchar(80) primary key,
                                                            subcribers_count bigint,
                                                            viewer_count bigint,
                                                            total_vedios int,
                                                            channel_description text,
                                                            playlist_id varchar(80)
                                                            )'''
                                                            
    cursor.execute(create_query)
    mydb.commit()
    
    
        
    single_channels_detail=[]
    db1=client['youtube_data']
    coll=db['channels_details']
    for ch1_data in coll.find({"channels_informations.channel_Name": channel_name_s},{"_id":0}):
        single_channels_detail.append(ch1_data['channels_informations'])
    df_single_channels_detail=pd.DataFrame(single_channels_detail)



    for index,row in df_single_channels_detail.iterrows():
        insert_query='''insert into channels(channel_Name,
                                            channel_id,
                                            subcribers_count,
                                            viewer_count,
                                            total_vedios,
                                            channel_description,
                                            playlist_id)
                                            
                                            
                                            Values(%s,%s,%s,%s,%s,%s,%s)  '''
        values=(row['channel_Name'],
                row['channel_id'],
                row['subcribers_count'],
                row['viewer_count'],
                row['total_vedios'],
                row['channel_description'],
                row['playlist_id'])
        
    
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            news=f"Your Provided Channel Name{channel_name_s} Is Already Exists"
            
            return news
       
            
            
def playlist_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="arun9999",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists playlists(playlist_id varchar(100)primary key,
                                                            title varchar(100),
                                                            channel_id varchar(100),
                                                            channel_name varchar(100),
                                                            publised_at timestamp,
                                                            vedio_count int
                                                            )'''
                                                                                                                
    cursor.execute(create_query)
    mydb.commit()


    single_playlist_details=[]
    db1=client['youtube_data']
    coll=db['channels_details']
    for ch1_data in coll.find({"channels_informations.channel_Name":channel_name_s},{"_id":0}):
        single_playlist_details.append(ch1_data['playlist_informations'])
    df_single_playlist_detail=pd.DataFrame(single_playlist_details[0])

    for index,row in df_single_playlist_detail.iterrows():
        insert_query='''insert into playlists(playlist_id,
                                            title,
                                            channel_id,
                                            channel_name,
                                            publised_at,
                                            vedio_count)
                                            
                                            
                                            Values(%s,%s,%s,%s,%s,%s)  '''
                                            
                                            
        values=(row['playlist_id'],
                row['title'],
                row['channel_id'],
                row['channel_name'],
                row['publised_at'],
                row['vedio_count'])
        cursor.execute(insert_query,values)
        mydb.commit()
        

def video_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="arun9999",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()


    create_query='''create table if not exists videos(channel_name varchar(100),
                                                        channel_Id varchar(100),                          
                                                        vedio_id varchar(100) primary key,
                                                        title varchar(150),
                                                        Tags text,
                                                        Thumbnails varchar(200),
                                                        Description text,
                                                        Publiced_date timestamp,
                                                        Durations interval,
                                                        View bigint,
                                                        likes bigint,
                                                        comments int,
                                                        favorite_count int,
                                                        definition varchar(10),
                                                        caption_status boolean
                                                                                ) '''
                                                                    
                                                                                                                
    cursor.execute(create_query)
    mydb.commit()

    single_videos_details=[]
    db1=client['youtube_data']
    coll=db['channels_details']
    for ch1_data in coll.find({"channels_informations.channel_Name":channel_name_s},{"_id":0}):
        single_videos_details.append(ch1_data['vedios_details'])
    df_single_videos_detail=pd.DataFrame(single_videos_details[0])

    for index,row in df_single_videos_detail.iterrows():
        insert_query='''insert into videos(channel_name,
                                            channel_Id,                          
                                            vedio_id,
                                            title,
                                            Tags,
                                            Thumbnails,
                                            Description,
                                            Publiced_date,
                                            Durations,
                                            View,
                                            likes,
                                            comments,
                                            favorite_count,
                                            definition,
                                            caption_status)
                                            
                                            Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                    row['channel_Id'],
                    row['vedio_id'],
                    row['title'],
                    row['Tags'],
                    row['Thumbnails'],
                    row['Description'],
                    row['Publiced_date'],
                    row['Durations'],
                    row['View'],
                    row['likes'],
                    row['comments'],
                    row['favorite_count'],
                    row['definition'],
                    row['caption_status'])
        
        cursor.execute(insert_query,values)
        mydb.commit()


def comments_table(channel_name_s):

    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="arun9999",
                            database="youtube_data",
                            port="5432")
    cursor=mydb.cursor()


    create_query='''create table if not exists comments(Comment_id varchar(150)primary key,
                                                        Vedio_id varchar(100),
                                                        Comment_text text,
                                                        Comment_author varchar(150),
                                                        Comment_publiced_date timestamp
                                                            )'''
                                                                                                                
    cursor.execute(create_query)
    mydb.commit()

    single_comments_details=[]
    db1=client['youtube_data']
    coll=db['channels_details']
    for ch1_data in coll.find({"channels_informations.channel_Name":channel_name_s},{"_id":0}):
        single_comments_details.append(ch1_data['comments_informatios'])
    df_single_comments_detail=pd.DataFrame(single_comments_details[0])



    for index,row in df_single_comments_detail.iterrows():
        insert_query='''insert into comments(Comment_id,
                                                Vedio_id,
                                                Comment_text,
                                                Comment_author,
                                                Comment_publiced_date)
                                            
                                            Values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_id'],
                    row['Vedio_id'],
                    row['Comment_text'],
                    row['Comment_author'],
                    row['Comment_publiced_date']
                    )
        
        cursor.execute(insert_query,values)
        mydb.commit()


def tables(single_channel):
    news=channels_table(single_channel)
    if news:
        return news
    
    else:
        playlist_table(single_channel)
        video_table(single_channel)
        comments_table(single_channel)
    
        return "Tables created sucessfully !!!"



def show_channels_table():
    ch_list=[]
    db1=client['youtube_data']
    coll=db['channels_details']
    for ch1_data in coll.find({},{"_id":0,"channels_informations":1}):
        ch_list.append(ch1_data['channels_informations'])
    df=st.dataframe(ch_list)
    
    return df



def show_playlists_table():
    pl_list=[]
    db1=client['youtube_data']
    coll=db['channels_details']
    for pl_data in coll.find({},{"_id":0,"playlist_informations":1}):
        for i in range(len(pl_data['playlist_informations'])):
            pl_list.append(pl_data['playlist_informations'][i])
    df1=st.dataframe(pl_list)
    
    return df1


def show_videos_table():
    vi_list=[]
    db1=client['youtube_data']
    coll=db['channels_details']
    for vi_data in coll.find({},{"_id":0,"vedios_details":1}):
        
        for i in range(len(vi_data['vedios_details'])):
            vi_list.append(vi_data['vedios_details'][i])
    df2=st.dataframe(vi_list)
    
    return df2


def show_comments_tables():
    cmt_list=[]
    db1=client['youtube_data']
    coll=db['channels_details']
    for cmt_data in coll.find({},{"_id":0,"comments_informatios":1}):
        
        for i in range(len(cmt_data['comments_informatios'])):
            cmt_list.append(cmt_data['comments_informatios'][i])
    df3=st.dataframe(cmt_list)
    
    return df3



with st.sidebar:
    st.title(":red['YOUTUBE DATA HAVERSTING AND WAREHOUSEING']")
    st.header('Skill Take Away')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption("Mongo DB")
    st.caption('API Integration')
    st.caption('Data Management Using MongoDB and SQL')
    
Channels_id=st.text_input('Enter The Channells ID')


if st.button('Collect and Store Data'):
    ch_ids=[]
    db=client['youtube_data']
    coll=db['channels_details']

    for ch_data in coll.find({},{'_id':0,"channels_informations":1}):
        ch_ids.append(ch_data['channels_informations']['channel_id'])
    
    if Channels_id in ch_ids:
        st.success('Channels Details Of The Given Channel Id Already Exists')
        
        
    else:
        insert=channel_details(Channels_id)
        st.success(insert)
        
        
all_channels=[]
db1=client['youtube_data']
coll=db['channels_details']
for ch1_data in coll.find({},{"_id":0,"channels_informations":1}):
    all_channels.append(ch1_data['channels_informations']['channel_Name'])
    

unique_channel=st.selectbox("Select The Channels",all_channels)

if st.button('Migrate To SQL'):
    Table=tables(unique_channel)
    st.success(Table)
    
show_table=st.radio('SELECT THE TABLE FOR VIEW',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))

if show_table=='CHANNELS':
    show_channels_table()

elif show_table=='PLAYLISTS':
    show_playlists_table()
    
elif show_table=='VIDEOS':
    show_videos_table()
    
elif show_table== 'COMMENTS':
    show_comments_tables()
        


#SQL CONNECTIONS:

mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="arun9999",
                            database="youtube_data",
                            port="5432")
cursor=mydb.cursor()
mydb.commit()
questions=st.selectbox("Select Your Question",("1. All The Video snd The Channels Name",
                                               "2. Channels With Most Number Of Videos",
                                               "3. 10 Most Viewed Videos",
                                               "4. Comments In Each Videos",
                                               '5. Videos With Highest Likes',
                                               '6. Likes Of All Videos',
                                               '7. Views Of All Videos',
                                               '8. Videos Published In The Year Of 2022',
                                               '9. Average Duration Of all Videos In Each Channel',
                                               '10. Videos With Hightest Number Of Comments'))

if questions=="1. All The Video snd The Channels Name":
    query1="""select title as videos,channel_name as channelname from Videos"""
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=['video title','channel title'])
    st.write(df)
    
    
elif questions=="2. Channels With Most Number Of Videos":
    query2="""select channel_name as channalname,total_vedios as no_vedio from channels
                    order by total_vedios desc"""
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=['channels name','No of Videos'])
    st.write(df2)
    

elif questions=="3. 10 Most Viewed Videos":
    query3="""select view as view,channel_name as channelname, title as videotitle from Videos
                    where view is not null order by view desc limit 10"""
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=['Views','Channels Name','Videotitle'])
    st.write(df3)


elif questions=="4. Comments In Each Videos":
    query4="""select comments as no_comments,title as videotitle from Videos where comments is not null"""
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=['No Of Comments','Videos'])
    st.write(df4)
    
elif questions=="5. Videos With Highest Likes":
    query5="""select title as videotitle,channel_name as channelname,likes as likecount from videos 
            where Videos is not null order by likes desc """
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=['Vediotitle','Channels name','Likes'])
    st.write(df5)
    
    

elif questions=="6. Likes Of All Videos":
    query6="""select likes as likecount,title as videotitle from videos """
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=['Likes Count','Video Title'])
    st.write(df6)
    

elif questions=='7.views for each channel,':
    query7="""select channel_name as channelname,viewer_count as viewersCount from channels """
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=['Channels Name','Viewers Count'])
    st.write(df7)
    
    
elif questions=='8. Videos Published In The Year Of 2022':
    query8="""select title as videotitle,publiced_date as releshed_date,channel_name as channelName from Videos
                where extract(year from publiced_date)=2022"""
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=['Video Title','Releshed Date','Channels Name'])
    st.write(df8)
    
    
elif questions=='9. Average Duration Of all Videos In Each Channel':
    query9="""select channel_name as channelname,AVG(durations) as avgdurations from videos group by channel_name"""
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=['Channel Name','AVG(Durations)'])
    #st.write(df8)

    T9=[]
    for index,row in df9.iterrows():
        channelstitle=row['Channel Name']
        avgduration=row['AVG(Durations)']
        AvgDuration_str=str(avgduration)
        T9.append(dict(Channel_Title=channelstitle,AVG_Duration=AvgDuration_str))

    df09=pd.DataFrame(T9)
    st.write(df09)
    
    
if questions=='10. Videos With Hightest Number Of Comments':
    query10="""select title as videotitle,channel_name as channelname,comments as comments from videos where comments is not null
                order by comments desc"""
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=['Videos Title','Channel Name','Comments'])
    st.write(df10)
