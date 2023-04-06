import os, json
import re
import http
import time
import urllib.request
import pyreadr
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import sportsdataverse as sdv
import xgboost as xgb
import tidypolars as tp
from tidypolars import col, desc
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from pathlib import Path

path_to_final = "wnba/json/final"
path_to_schedules = "wnba/schedules"
path_to_pbp = 'wnba/pbp'
path_to_team_box = 'wnba/team_box'
path_to_player_box = 'wnba/player_box'
def wnba_team_box_score_extract(js, result):
    team_box_score = pd.json_normalize(result.get("boxscore",{}).get("teams",[]),
                    record_path="statistics",
                    meta = [["team","id"],
                            ["team","uid"],
                            ["team","slug"],
                            ["team","location"],
                            ["team","name"],
                            ["team","abbreviation"],
                            ["team","displayName"],
                            ["team","shortDisplayName"],
                            ["team","color"],
                            ["team","alternateColor"],
                            ["team","logo"]],
                    sep="_",
                    record_prefix="stat_",
                    errors='ignore')
    team_box_score = team_box_score.fillna(value=np.nan)
    team_box_score['game_id'] = int(js)
    return team_box_score

def wnba_player_box_score_extract(js, result):
    num_teams = range(len(result.get("boxscore",{}).get("players",[])))
    player_box_score = pd.DataFrame()
    for team in num_teams:
        player_box = pd.json_normalize(
                        data = result.get("boxscore",{}).get("players",[])[team],
                        record_path = "statistics",
                        meta = [["team","id"],
                                ["team","uid"],
                                ["team","slug"],
                                ["team","location"],
                                ["team","name"],
                                ["team","abbreviation"],
                                ["team","displayName"],
                                ["team","shortDisplayName"],
                                ["team","color"],
                                ["team","alternateColor"],
                                ["team","logo"]],
                        sep = "_",
                        record_prefix = "stat_",
                        errors = 'ignore')
        team_info = pd.json_normalize(data = result.get("boxscore").get("players",[])[team].get("team"),
                                                  errors = 'ignore')
        team_info.columns = 'team_' + team_info.columns
        stat_names = player_box.get('stat_name',[])

        aths_column = player_box.get("stat_athletes",[])
        team_player_boxscore_df = pd.DataFrame()
        for i in range(len(stat_names)):
            if len(aths_column[i]) > 0:
                for k in range(len(aths_column[i])):
                    try:
                        athlete = aths_column[i][k]
                        athlete.get("athlete",{}).pop("links", None)
                        athlete_df = pd.json_normalize(athlete.get("athlete",{}),
                                                                errors = 'ignore')
                        athlete_df.columns = 'athlete_' + athlete_df.columns
                        stat_keys = np.array(player_box.get("stat_keys",[]))[i]
                        ath_stat_vals = np.array(aths_column[i][k].get('stats',[]))
                        athlete_stats_df = pd.DataFrame(ath_stat_vals.reshape(-1,len(ath_stat_vals)), columns=stat_keys)

                        athlete_df = pd.concat([team_info, athlete_df, athlete_stats_df], axis=1)
                        team_player_boxscore_df = pd.concat([team_player_boxscore_df,athlete_df], ignore_index=True)
                    except (IndexError) as e:
                        print("IndexError: game_id = {}\n {}".format(js, e))
                        continue
                    except (KeyError) as e:
                        print("KeyError: game_id = {}\n {}".format(js, e))
                        continue
                    except (ValueError) as e:
                        print("DecodeError: game_id = {}\n {}".format(js, e))
                        continue
                    except (AttributeError) as e:
                        print("AttributeError: game_id = {}\n {}".format(js, e))
                        continue
        player_box_score = pd.concat([player_box_score, team_player_boxscore_df], ignore_index=True)

    player_box_score = player_box_score.fillna(value=np.nan)
    player_box_score.insert(0,'game_id', int(js))
    return player_box_score


def main():
    years_arr = range(2021,2022)
    for year in years_arr:
        print("Processing year {}...".format(year))
        season_schedule = pd.read_parquet(f"{path_to_schedules}/parquet/wnba_schedule_{year}.parquet", engine='auto', columns=None)
        season_schedule["game_id"] = season_schedule["game_id"].astype(str)

        season_schedule_game_ids= season_schedule[season_schedule['season'] == year]['game_id'].tolist()
        path_to_final_json = "{}/".format(path_to_final)
        Path(path_to_final_json).mkdir(parents=True, exist_ok=True)
        json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_final_json) if pos_json.endswith('.json')]
        json_files = set(json_files).intersection(set(season_schedule_game_ids))
        json_files = list(json_files)
        json_files = sorted(json_files)
        print("Number of Games: {}".format(len(json_files)))
        pbp_season = pd.DataFrame()
        team_box_season = pd.DataFrame()
        player_box_season = pd.DataFrame()
        # we need both the json and an index number so use enumerate()
        for index, js in enumerate(json_files):
            try:

                result = sdv.wnba.wnba_pbp_disk(game_id = js, path_to_json = path_to_final_json)
                plays = pd.DataFrame(result.get("plays",[]))
                plays = plays.fillna(value=np.nan)
                team_box_score = wnba_team_box_score_extract(js, result)

                player_box_score = wnba_player_box_score_extract(js, result)
                pbp_season = pd.concat([pbp_season, plays],
                                       ignore_index=True)
                team_box_season = pd.concat([team_box_season, team_box_score], ignore_index=True)
                player_box_season = pd.concat([player_box_season, player_box_score], ignore_index=True)
            except (KeyError) as e:
                print("KeyError: yo", js)
                continue
            except (ValueError) as e:
                print("DecodeError: yo", js)
                continue
            except (AttributeError) as e:
                print("AttributeError: yo", js)
                continue
        if path_to_schedules is not None:
            path_to_csv = "{}/{}/".format(path_to_pbp, 'csv')
            path_to_parquet = "{}/{}/".format(path_to_pbp, 'parquet')
            path_to_rds = "{}/{}/".format(path_to_pbp, 'rds')
            Path(path_to_csv).mkdir(parents=True, exist_ok=True)
            Path(path_to_parquet).mkdir(parents=True, exist_ok=True)
            Path(path_to_rds).mkdir(parents=True, exist_ok=True)
            pbp_season = pbp_season.fillna(value=np.nan)

            # pbp_season.to_csv(f"{path_to_pbp}/csv/play_by_play_{year}.csv", index = False)
            pbp_season.to_parquet(f"{path_to_pbp}/parquet/play_by_play_{year}.parquet", index = False)
            pyreadr.write_rds(f"{path_to_pbp}/rds/play_by_play_{year}.rds", pbp_season, compress = "gzip")

            path_to_csv = "{}/{}/".format(path_to_team_box, 'csv')
            path_to_parquet = "{}/{}/".format(path_to_team_box, 'parquet')
            path_to_rds = "{}/{}/".format(path_to_team_box, 'rds')
            Path(path_to_csv).mkdir(parents=True, exist_ok=True)
            Path(path_to_parquet).mkdir(parents=True, exist_ok=True)
            Path(path_to_rds).mkdir(parents=True, exist_ok=True)
            team_box_season = team_box_season.fillna(value=np.nan)
            team_box_season.to_csv(f"{path_to_team_box}/csv/team_box_{year}.csv", index = False)
            team_box_season.to_parquet(f"{path_to_team_box}/parquet/team_box_{year}.parquet", index = False)
            pyreadr.write_rds(f"{path_to_team_box}/rds/team_box_{year}.rds", team_box_season, compress = "gzip")

            path_to_csv = "{}/{}/".format(path_to_player_box, 'csv')
            path_to_parquet = "{}/{}/".format(path_to_player_box, 'parquet')
            path_to_rds = "{}/{}/".format(path_to_player_box, 'rds')
            Path(path_to_csv).mkdir(parents=True, exist_ok=True)
            Path(path_to_parquet).mkdir(parents=True, exist_ok=True)
            Path(path_to_rds).mkdir(parents=True, exist_ok=True)
            player_box_season = player_box_season.fillna(value=np.nan)
            player_box_season.to_csv(f"{path_to_player_box}/csv/player_box_{year}.csv", index = False)
            player_box_season.to_parquet(f"{path_to_player_box}/parquet/player_box_{year}.parquet", index = False)
            pyreadr.write_rds(f"{path_to_player_box}/rds/player_box_{year}.rds", player_box_season, compress = "gzip")

            pbp_game_ids = pbp_season.game_id.unique()
            team_box_game_ids = team_box_season.game_id.unique()
            player_box_game_ids = player_box_season.game_id.unique()

            season_schedule['PBP'] = season_schedule['game_id'].isin(pbp_game_ids)
            season_schedule['team_box'] = season_schedule['game_id'].isin(team_box_game_ids)
            season_schedule['player_box'] = season_schedule['game_id'].isin(player_box_game_ids)
            season_schedule.to_csv(f"{path_to_schedules}/csv/wnba_schedule_{year}.csv", index = False)
            season_schedule.to_parquet(f"{path_to_schedules}/parquet/wnba_schedule_{year}.parquet", index = False)
            pyreadr.write_rds(f"{path_to_schedules}/rds/wnba_schedule_{year}.rds", season_schedule, compress = "gzip")

        print("Finished processing year {}...".format(year))



if __name__ == "__main__":
    main()