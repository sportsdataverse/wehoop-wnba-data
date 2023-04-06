import os, json
import re
import http
import pyreadr
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import sportsdataverse as sdv
import xgboost as xgb
import multiprocessing
import time
import urllib.request
import argparse
from tqdm import tqdm
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from pathlib import Path

path_to_raw = "wnba/json/raw"
path_to_final = "wnba/json/final"
path_to_errors = "wnba/errors"
run_processing = True
rescrape_all = False
def main():
    if args.start_year < 2002:
        start_year = 2002
    else:
        start_year = args.start_year
    if args.end_year is None:
        end_year = start_year
    else:
        end_year = args.end_year
    years_arr = range(start_year, end_year + 1)
    schedule = pd.read_parquet('wnba_schedule_master.parquet', engine='auto', columns=None)
    schedule = schedule.sort_values(by=['season','season_type'], ascending = True)
    schedule["game_id"] = schedule["game_id"].astype(int)
    schedule = schedule[schedule['status_type_completed']==True]
    if args.rescrape == False:
        schedule_in_repo = pd.read_parquet('wnba/wnba_games_in_data_repo.parquet', engine='auto', columns=None)
        schedule_in_repo["game_id"] = schedule_in_repo["game_id"].astype(int)
        done_already = schedule_in_repo['game_id']
        schedule = schedule[~schedule['game_id'].isin(done_already)]
    schedule_with_pbp = schedule[schedule['season']>=2002]

    for year in years_arr:
        print("Scraping year {}...".format(year))
        games = schedule[(schedule['season']==year)].reset_index()['game_id'].tolist()
        print(f"Number of Games: {len(games)}")
        bad_schedule_keys = pd.DataFrame()
        # this finds our json files
        path_to_raw_json = "{}/".format(path_to_raw)
        path_to_final_json = "{}/".format(path_to_final)
        Path(path_to_raw_json).mkdir(parents=True, exist_ok=True)
        Path(path_to_final_json).mkdir(parents=True, exist_ok=True)
        # json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith('.json')]

        for game in tqdm(games):
            try:
                g = sdv.wnba.espn_wnba_pbp(game_id = game, raw=True)
            except (TypeError) as e:
                print("TypeError: game_id = {}\n {}".format(game, e))
                continue
            except (IndexError) as e:
                print("IndexError: game_id = {}\n {}".format(game, e))
                continue
            except (KeyError) as e:
                print("KeyError: game_id = {}\n {}".format(game, e))
                continue
            except (ValueError) as e:
                print("DecodeError: game_id = {}\n {}".format(game, e))
                continue
            except (AttributeError) as e:
                print("AttributeError: game_id = {}\n {}".format(game, e))
                continue
            with open("{}{}.json".format(path_to_raw_json, game),'w') as f:
                json.dump(g, f, indent=2, sort_keys=False)
            if args.process == True:
                try:
                    processed_data = sdv.wnba.wnba_pbp_disk(
                        game_id = game,
                        path_to_json = path_to_raw
                    )

                    result = sdv.wnba.helper_wnba_pbp(
                        game_id = game,
                        pbp_txt = processed_data
                    )
                    fp = "{}{}.json".format(path_to_final_json, game)
                    with open(fp,'w') as f:
                        json.dump(result, f, indent=2, sort_keys=False)
                except (IndexError) as e:
                    print("IndexError: game_id = {}\n {}".format(game, e))
                except (KeyError) as e:
                    print("KeyError: game_id = {}\n {}".format(game, e))
                    continue
                except (ValueError) as e:
                    print("DecodeError: game_id = {}\n {}".format(game, e))
                    continue
                except (AttributeError) as e:
                    print("AttributeError: game_id = {}\n {}".format(game, e))
                    continue

        print("Finished Scraping year {}...".format(year))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_year', '-s', type=int, required=True, help='Start year of WNBA Schedule period (YYYY), eg. 2023 for 2022-23 season')
    parser.add_argument('--end_year', '-e', type=int, help='End year of WNBA Schedule period (YYYY), eg. 2023 for 2022-23 season')
    parser.add_argument('--rescrape', '-r', type=bool, default=True, help='Rescrape all games in the schedule period')
    parser.add_argument('--process', '-p', type=bool, default=True, help='Run processing pipeline for games in the schedule period')
    args = parser.parse_args()

    main()
