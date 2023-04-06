#!/bin/bash
while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done
for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    echo "$i"
    git pull
    python3 python/scrape_wnba_schedules.py -s $i -e $i
    python3 python/scrape_wnba_json.py -s $i -e $i -r $RESCRAPE
    git pull
    git add .
    bash scripts/daily_wnba_R_processor.sh -s $i -e $i
done