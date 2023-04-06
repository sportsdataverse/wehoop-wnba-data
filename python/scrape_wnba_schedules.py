import os, json
import re
import http
import time
import urllib.request
import pyreadr
import pyarrow as pa
import pandas as pd
import sportsdataverse as sdv
import argparse
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from pathlib import Path
from tqdm import tqdm

path_to_schedules = "wnba/schedules"
final_file_name = "wnba_schedule_master.csv"

def download_schedule(season, path_to_schedules=None):
    df = sdv.wnba.espn_wnba_calendar(season, ondays=True)
    calendar = df['dateURL'].tolist()
    ev = pd.DataFrame()
    for d in tqdm(calendar):
        date_schedule = sdv.wnba.espn_wnba_schedule(dates=d)
        ev = pd.concat([ev,date_schedule],axis=0, ignore_index=True)
    ev = ev[ev['season_type'].isin([2,3])]
    ev = ev.drop('competitors', axis=1)
    ev = ev.drop_duplicates(subset=['game_id'], ignore_index=True)
    if path_to_schedules is not None:
        ev.to_csv(f"{path_to_schedules}/csv/wnba_schedule_{season}.csv", index = False)
        ev.to_parquet(f"{path_to_schedules}/parquet/wnba_schedule_{season}.parquet", index = False)
        pyreadr.write_rds(f"{path_to_schedules}/rds/wnba_schedule_{season}.rds", ev, compress = "gzip")
    return ev

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
    schedule_table = pd.DataFrame()
    for year in years_arr:
        print("Scraping WNBA schedules for year {}...".format(year))
        year_schedule = download_schedule(year, path_to_schedules)
        schedule_table = pd.concat([schedule_table, year_schedule], axis=0)
    csv_files = [pos_csv.replace('.csv', '') for pos_csv in os.listdir(path_to_schedules+'/csv') if pos_csv.endswith('.csv')]
    glued_data = pd.DataFrame()
    for index, js in enumerate(csv_files):
        x = pd.read_csv(f"{path_to_schedules}/csv/{js}.csv", low_memory=False)
        glued_data = pd.concat([glued_data,x],axis=0)
    glued_data['status_display_clock'] = glued_data['status_display_clock'].astype(str)
    glued_data.to_csv(final_file_name, index=False)
    glued_data.to_parquet(final_file_name.replace('.csv', '.parquet'), index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_year', '-s', type=int, required=True, help='Start year of WNBA Schedule period (YYYY)')
    parser.add_argument('--end_year', '-e', type=int, help='End year of WNBA Schedule period (YYYY)')
    args = parser.parse_args()

    main()