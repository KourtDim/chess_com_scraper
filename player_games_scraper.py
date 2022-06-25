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
print('Time started: {}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_start))))

try:
    os.mkdir('Player_Games')
except:
    pass

path = 'Player_Games'

df= pd.read_pickle('Player_Games.pkl')
player_profile_df = pd.read_pickle('Player_Profile.pkl')
games_df=[]

for player in df['Player'].unique()[0:1400]:
    reqs = []
    n_games =[]
    p_time_start = time.time()
    if '{}.csv'.format(player) in os.listdir(path):
        print('{} player data have already been collected.'.format(player))
    else:
        player_df = df.loc[df['Player']==player]
        
        player_games=[]
        
        for game in player_df['Games']:
            try:
                games = rq.get(game).json()['games']
                reqs.append(1)
                for g in games:
                    try:
                        n_games.append(1)
                        pgn = io.StringIO(g['pgn'])
                        pgn = chess.pgn.read_game(pgn)
                        if len(pgn.headers.keys())==21:
                            data={}
                            data['player']=player
                            data['player_name']=player_profile_df.loc[player]['name']
                            data['url']=g['url']
                            try: 
                                data['white_Accuracy']=g['accuracies']['white']
                                data['black_Accuracy']=g['accuracies']['black']
                            except:
                                data['white_Accuracy']='-'
                                data['black_Accuracy']='-'

                            for h in pgn.headers.keys():
                                data[h]=pgn.headers[h]
                            data['pgn']=g['pgn']
                            data['ECOUrl']= data['ECOUrl'].replace('https://www.chess.com/openings/','').replace('-',' ')
                            if data['White']== player:
                                data['player_rating']=data['WhiteElo']
                            else:
                                data['player_rating']=data['BlackElo']
                            temp_df = pd.DataFrame(data,index=[data['url']])

                            player_games.append(temp_df)

                        else:
                            pass
                    except:
                        pass

                time.sleep(0.25)

            except:
                time.sleep(150)
        if len(player_games)>0:
            temp_df = pd.concat(player_games)
            temp_df.to_csv('Player_Games/{}.csv'.format(player))
        else:
            print('No games found for player: {}'.format(player))
        p_time_stop =time.time()
        print('{} player data collection completed in {} seconds, total requests: {}, total_games:{}'.format(player,
                                                                                                             round(p_time_stop-p_time_start,2),
                                                                                                            len(reqs),
                                                                                                            len(n_games)))
        
        time.sleep(0.5)
    
time_stop =time.time()

print('Time Finished: {}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_stop))))
