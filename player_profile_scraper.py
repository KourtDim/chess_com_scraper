

#Start time
import pickle
import requests as rq
import time 
import pandas as pd
from IPython.display import display
from datetime import datetime
import chess.pgn
import io
import os

time_start = time.time()
print(time_start)
# base url from chess.com api
base_url = 'https://api.chess.com/pub/'

# Chess player titles
titles = ['GM','WGM']

#Create empty list for titled player usernames
titled_players =[]

# Create a for loop to get all titled players
for t in titles:
    #Cretae the title player url
    url = base_url+'titled/{}'.format(t)
    print(url)
    
    # Create a temporary dataframe
    temp_df =pd.DataFrame()
    temp_df['Player']=rq.get(url).json()['players']
    temp_df['Title']=t
    titled_players .append(temp_df)
    time.sleep(0.5)

# Concatenate all titled_player dfs in one dataframe
titled_players_df = pd.concat(titled_players)
# Save it to a pickle file
titled_players_df.to_pickle('Titled_Players.pkl')

#Create empty lists for player profile,player stats and player games data
player_profile_dfs=[]
player_stats_df = []
player_games = []

#Create a for loop to scrape the data
for i,t in zip(titled_players_df['Player'],titled_players_df['Title']):
    
    player_url = 'https://api.chess.com/pub/player/{}'.format(i)
    print(i,rq.get(player_url))
    
    player_json =rq.get(player_url).json()
    player_json['last_online']=datetime.fromtimestamp(player_json['last_online']).date()
    player_json['joined']=datetime.fromtimestamp(player_json['joined']).date()
    #player_json['title']=t
    player_profile_dfs.append(pd.DataFrame(player_json, index=[i]))
    time.sleep(0.25)
    
    player_stats_url ='https://api.chess.com/pub/player/{}/stats'.format(i)
    player_stats_json = rq.get(player_stats_url).json()

    for stat in player_stats_json.keys():
        l=['tactics','lessons','puzzle_rush']
        if stat not in l:
            try:
                data ={'Type':stat,
                       'Current_Rating':player_stats_json[stat]['last']['rating'],
                      'Current_Rating_Date':datetime.fromtimestamp(player_stats_json[stat]['last']['date']).date(),
                      'RD':player_stats_json[stat]['last']['rd'],
                      'Best_Rating':player_stats_json[stat]['best']['rating'],
                      'Best_Rating_Date':datetime.fromtimestamp(player_stats_json[stat]['best']['date']).date(),
                      'Win':player_stats_json[stat]['record']['win'],
                      'Loss':player_stats_json[stat]['record']['loss'],
                      'Draw':player_stats_json[stat]['record']['draw']}
                temp_df = pd.DataFrame(data,index=[i])
                player_stats_df.append(temp_df)
            except:
                pass

    time.sleep(0.25)
    
    player_game_archives_url = 'https://api.chess.com/pub/player/{}/games/archives'.format(i)
    player_game_archives_json = rq.get(player_game_archives_url).json()
    
    temp_df=pd.DataFrame()
    temp_df['Games']=player_game_archives_json['archives']
    temp_df['Player']=i
    player_games.append(temp_df)
    time.sleep(0.25)

player_profile_df=pd.concat(player_profile_dfs)
player_profile_df.to_pickle('Player_Profile.pkl')

player_stat_df=pd.concat(player_stats_df)
player_stat_df.to_pickle('Player_Stats.pkl')

player_games_df=pd.concat(player_games)
player_games_df.to_pickle('Player_Games.pkl')

time_stop =time.time()
print('Time Elapsed:{} seconds'.format(round(time_stop-time_start,2)))
