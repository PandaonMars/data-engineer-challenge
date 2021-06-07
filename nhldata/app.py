'''
	This is the NHL crawler.  

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run 
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.  
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''

import os
import argparse

import logging
from pathlib import Path
from datetime import datetime
from datetime import date
from datetime import timedelta
from dataclasses import dataclass
import boto3
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f'{self.SCHEMA_HOST}/{self.VERSION_PREFIX}'


    def schedule(self, start_date: date, end_date: date) -> dict:
        ''' 
        returns a dict tree structure that is like
            "dates": [ 
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        '''
        # error handling
        # Answer
        # Is the schedule is to pick up all the history data and contiune to run?

        try:
            return self._get(self._url('schedule'), {'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')})
        
        except Exception as ex:
            logging.error(f'Failed to retrive. exception: {ex}')

    def boxscore(self, game_id):
        '''
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            "person": {
                                "id": $int,
                                "fullName": $string,
                                #-- other info
                                "currentTeam": {
                                    "name": $string,
                                    #-- other info
                                },
                                "stats": {
                                    "skaterStats": {
                                        "assists": $int,
                                        "goals": $int,
                                        #-- other status
                                    }
                                    #-- ignore "goalieStats"
                                }
                            }
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home" 
                }
            }

            See tests/resources/boxscore.json for a real example response
        '''
        url = self._url(f'game/{game_id}/boxscore')
        logging.info(f'requesting url: {url}')
        return self._get(url)
    # error handling 
    # Answer
    def _get(self, url, params=None):
        # retry if status code is in the list
        try:
            s = requests.Session()
            retries = Retry(total=5, backoff_factor=1, status_forcelist=[413, 429, 500, 502, 503, 504])
            s.mount('https//', HTTPAdapter(max_retries=retries))
            response = s.get(url)
        
        except requests.exceptions.RequestException as ex:
            logging.error ("API Request Error: ", ex)
            raise SystemExit(ex)
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'

@dataclass
class StorageKey:
    # TODO what propertie are needed to partition? 
    # ANSOWER: Partitioning by API processing time
    gameid: str
    game_datetime: datetime

    def key(self):
        ''' renders the s3 key for the given set of properties '''
        # TODO use the properties to return the s3 key
        # ANSOWER: use year/month/game_datetime_gameid as file name
        return f'{self.game_datetime.year}/{self.game_datetime.month}/{self.game_datetime}_{self.gameid}.csv'

class Storage():
    def __init__(self, dest_bucket, s3_client):
        self._s3_client = s3_client
        self.bucket = dest_bucket

    def store_game(self, key: StorageKey, game_data) -> bool:
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=game_data)
        return True

class Crawler():
    def __init__(self, api: NHLApi, storage: Storage):
        self.api = api
        self.storage = storage

    def crawl(self, startDate: datetime, endDate: datetime) -> None:
	# NOTE the data direct from the API is not quite what we want. Its nested in a way we don't want
	#      so here we are looking for your ability to gently massage a data set.

        #TODO error handling. Finished in schedule and _get
        
        #TODO get games for dates

        schedule = self.api.schedule(startDate, endDate)
        
        # Answer
        # iterate over game dates to properly partition
        if schedule is not None:
            for date in schedule.get("dates"):
                game_datetime = datetime.strptime(date.get("date"),'%Y-%m-%d')

                logging.info(f'Loading data for {game_datetime}')
                games_df = pd.DataFrame()
                games_df = games_df.append(pd.json_normalize(date.get("games")), ignore_index = True)
            
                column_names = ["player_person_id", "player_person_currentTeam_name", "player_person_fullName", "player_stats_skaterStats_assists", "player_stats_skaterStats_goals", "side"]

                for index, row in games_df.iterrows():
                    gameid = row["gamePk"]
                    stats = self.api.boxscore(gameid)
                    stats_df = pd.DataFrame()
                    logging.info(f'Loading {gameid}')
                    
                    #TODO for each game get all player stats: schedule -> date -> teams.[home|away] -> $playerId: player_object (see boxscore above)
                    # Answer
                    for side in ('home','away'):
                        teamname = stats.get("teams").get(side).get("team")["name"]
                        players = stats.get("teams").get(side).get("players").keys()

                        logging.info(f'Loading data for team {teamname}')
                    # Store players stats into df

                    for player in players:
                        #TODO ignore goalies (players with "goalieStats")
                        # Ansower do not select stats from goalieStats
                        player_str = stats.get("teams").get(side).get("players").get(f'{player}')

                        if player_str.get("stats").get("skaterStats") is not None:
                            player_id = str(int(player_str.get("person").get("id")))
                            playername = player_str.get("person").get("fullName")
                            goals,assists = [player_str.get("stats").get("skaterStats").get(k) for k in ["goals","assists"]]
                            
                            playerstats = pd.Series([player_id, teamname, playername, assists, goals, side], index=column_names)
                            stats_df = stats_df.append(playerstats, ignore_index=True)

                
                    
                    #TODO output to S3 should be a csv that matches the schema of utils/create_games_stats 
                    s3Key = StorageKey(gameid, game_datetime)
                    
                    logging.info(f'Writing file: {s3Key.key()}')

                    self.storage.store_game(s3Key, stats_df[column_names].to_csv(index=False))
        else:
            logging.info(f'No game found between {startDate} to {endDate}')
                 
def main():

    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    # TODO what arguments are needed to make this thing run,  if any?
    # Answer

    # add default date
    parser.add_argument("--start_date", default='20210604', type=str)
    
    parser.add_argument("--end_date", default='20210605', type=str)
    
    args = vars(parser.parse_args())

    startDate = dateparse(args['start_date'])
    endDate = dateparse(args['end_date'])

    logging.info(f"the start date is {startDate}")
    logging.info(f"the end date is {endDate}")

    dest_bucket = os.environ.get('DEST_BUCKET', 'output')

    api = NHLApi()

    s3client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    storage = Storage(dest_bucket, s3client)
    crawler = Crawler(api, storage)

    crawler.crawl(startDate, endDate)

    
if __name__ == '__main__':
    main()