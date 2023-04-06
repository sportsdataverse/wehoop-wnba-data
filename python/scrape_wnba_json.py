import os
import json
import re
import http
import time
import urllib.request
import pyreadr
import pyarrow as pa
import pandas as pd
import sportsdataverse as sdv
import argparse
import time
import urllib.request
import argparse
import concurrent.futures
import gc
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap, repeat
from pathlib import Path
from tqdm import tqdm

path_to_raw = "wnba/json/raw"
path_to_final = "wnba/json/final"
path_to_errors = "wnba/errors"
run_processing = True
rescrape_all = False
MAX_THREADS = 30

def download_game_pbps(games, process, path_to_raw, path_to_final):
    threads = min(MAX_THREADS, len(games))

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        result = list(tqdm(executor.map(download_game, games, repeat(process), repeat(path_to_raw), repeat(path_to_final)), total = len(games)))
        return result

def download_game(game, process, path_to_raw, path_to_final):

    # this finds our json files
    path_to_raw_json = f"{path_to_raw}/"
    path_to_final_json = f"{path_to_final}/"
    Path(path_to_raw_json).mkdir(parents = True, exist_ok = True)
    Path(path_to_final_json).mkdir(parents = True, exist_ok = True)
    try:
        g = sdv.wnba.espn_wnba_pbp(game_id = game, raw = True)
        with open(f"{path_to_raw_json}{game}.json","w") as f:
            json.dump(g, f, indent = 0, sort_keys = False)
    except (TypeError) as e:
        print(f"TypeError: game_id = {game}\n {e}")
        pass
    except (IndexError) as e:
        print(f"IndexError:  game_id = {game}\n {e}")
        pass
    except (KeyError) as e:
        print(f"KeyError: game_id =  game_id = {game}\n {e}")
        pass
    except (ValueError) as e:
        print(f"DecodeError: game_id = {game}\n {e}")
        pass
    except (AttributeError) as e:
        print(f"AttributeError: game_id = {game}\n {e}")
        pass
    if process == True:
        try:
            processed_data = sdv.wnba.wnba_pbp_disk(
                game_id = game,
                path_to_json = path_to_raw
            )

            result = sdv.wnba.helper_wnba_pbp(
                game_id = game,
                pbp_txt = processed_data
            )
            fp = f"{path_to_final_json}{game}.json"
            with open(fp,"w") as f:
                json.dump(result, f, indent = 0, sort_keys = False)
        except (FileNotFoundError) as e:
            print(f"FileNotFoundError: game_id = {game}\n {e}")
            pass
        except (TypeError) as e:
            print(f"TypeError: game_id = {game}\n {e}")
            pass
        except (IndexError) as e:
            print(f"IndexError:  game_id = {game}\n {e}")
            pass
        except (KeyError) as e:
            print(f"KeyError: game_id =  game_id = {game}\n {e}")
            pass
        except (ValueError) as e:
            print(f"DecodeError: game_id = {game}\n {e}")
            pass
        except (AttributeError) as e:
            print(f"AttributeError: game_id = {game}\n {e}")
            pass
    time.sleep(0.5)
def main():

    if args.start_year < 2002:
        start_year = 2002
    else:
        start_year = args.start_year
    if args.end_year is None:
        end_year = start_year
    else:
        end_year = args.end_year
    process = args.process
    years_arr = range(start_year, end_year + 1)
    schedule = pd.read_parquet("wnba/wnba_schedule_master.parquet", engine = "auto", columns = None)
    schedule = schedule.sort_values(by = ["season", "season_type"], ascending = True)
    schedule["game_id"] = schedule["game_id"].astype(int)
    schedule = schedule[schedule["status_type_completed"] == True]
    if args.rescrape == False:
        schedule_in_repo = pd.read_parquet("wnba/wnba_games_in_data_repo.parquet", engine = "auto", columns = None)
        schedule_in_repo["game_id"] = schedule_in_repo["game_id"].astype(int)
        done_already = schedule_in_repo["game_id"]
        schedule = schedule[~schedule["game_id"].isin(done_already)]
    schedule_with_pbp = schedule[schedule["season"] >= 2002]

    for year in years_arr:
        print(f"Scraping WNBA PBP for {year}...")
        games = schedule[(schedule["season"] == year)].reset_index()["game_id"].tolist()
        print(f"Number of Games: {len(games)}")
        bad_schedule_keys = pd.DataFrame()

        # json_files = [pos_json.replace(".json", "") for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith(".json")]


        t0 = time.time()
        download_game_pbps(games, process, path_to_raw, path_to_final)
        t1 = time.time()
        print(f"{(t1-t0)/60} minutes to download {len(games)} game play-by-plays.")


        print(f"Finished WNBA PBP for {year}...")
    gc.collect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", "-s", type = int, required = True, help = "Start year of WNBA Schedule period (YYYY), eg. 2023 for 2022-23 season")
    parser.add_argument("--end_year", "-e", type = int, help = "End year of WNBA Schedule period (YYYY), eg. 2023 for 2022-23 season")
    parser.add_argument("--rescrape", "-r", type = bool, default = True, help = "Rescrape all games in the schedule period")
    parser.add_argument("--process", "-p", type = bool, default = True, help = "Run processing pipeline for games in the schedule period")
    args = parser.parse_args()

    main()
