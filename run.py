from typing import List
from typing import Union

from datetime import datetime
import time
import pytz
from zoneinfo import ZoneInfo

import asyncio
from aiohttp import ClientSession

import requests

from bs4 import BeautifulSoup

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import orjson

# Change to your own settings
API_KEY = ""

# Please set your own timezone!!!
# It will be converted to UTC timezone
# cuz osu!api return scores time in UTC
TIME_START = datetime( # FROM
    2022, 8, 22, 0, 0, 0, 0,
    tzinfo=ZoneInfo("Europe/Minsk")
) 
TIME_END = datetime(
    2022, 8, 28, 0, 0, 0, 0, # TO
    tzinfo=ZoneInfo("Europe/Minsk")
) 

SHEET_NAME = ""
COUNTRY_CODE = ""
NUM_PAGES = 2 # number of ranking pages e.g. 2 is 100 users
CREDS_FILE = "./creds.json"

# Consts
DT_FORMAT = "%Y-%m-%d %H:%M:%S"
SCORES_TO_INSERT = []
SCOPE = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

def format_mods(mods: int) -> str:
    m = ""
    if int(mods) == 0:
        return "NM"
    if int(mods) & 1: 
        m += "NF"
    if int(mods) & 2:
        m += "EZ"
    if int(mods) & 4:
        m += "TD"
    if int(mods) & 8:
        m += "HD"
    if int(mods) & 16:
        m += "HR"
    if int(mods) & 32:
        m += "SD"
    if int(mods) & 64:
        m += "DT"
    if int(mods) & 128:
        m += "RX"
    if int(mods) & 256:
        m += "HT"
    if int(mods) & 512:
        m += "NC"
    if int(mods) & 1024:
        m += "FL"
    return m

def get_users(n: int, country_code: str) -> List[str]:
    arr = []
    for i in range(n):
        link = (
            "https://osu.ppy.sh/rankings/osu/performance"
            f"?country={country_code}"
            f"&page={i+1}#scores"
        )

        r = requests.get(link)
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table', ['class', 'ranking-page-table'])

        for row in table.tbody.find_all(
            'a',
            [
                'class',
                'ranking-page-table__user-link-text js-usercard'
            ]
        ):
            u: str = row.text
            u = u.strip()
            u = u.replace(' ', '_')
            arr.append(u)
    return arr

async def get_best_scores(session, username: str):
    link = (
        "https://osu.ppy.sh/api/get_user_best"
         f"?k={API_KEY}"
         f"&u={username}"
         "&limit=100"
    )
    async with session.get(link) as r:
        return await r.json()

async def get_user(session, uid: Union[int, str]):
    link = (
        "https://osu.ppy.sh/api/get_user"
         f"?k={API_KEY}"
         f"&u={uid}"
    )
    async with session.get(link) as r:
        j = await r.json()
        return j[0]

async def get_beatmap(session, bid: int):
    link = (
        "https://osu.ppy.sh/api/get_beatmaps"
         f"?k={API_KEY}"
         f"&b={bid}"
    )
    async with session.get(link) as r:
        j = await r.json()
        return j[0]

async def task(username: str):
    session = ClientSession(json_serialize=orjson.dumps)
    scores = await get_best_scores(session, username)
    user_data = await get_user(session, username)
    for score in scores:
        dt = datetime.strptime(score['date'], DT_FORMAT)
        dt = pytz.UTC.localize(dt)
        if (dt > TIME_START.astimezone(pytz.UTC)) and (dt < TIME_END.astimezone(pytz.UTC)):
            info = []
            b = await get_beatmap(session, score['beatmap_id'])

            info.append(username)
            info.append(score['pp'])
            info.append(score['date'])
            info.append(score['replay_available'])
            info.append(
                    f"=HYPERLINK(\""
                    "https://osu.ppy.sh/scores/osu/"
                    f"{score['score_id']}\","
                    "\"SCORE\")"
            )
            info.append(f"{b['artist']} - {b['title']}")
            info.append(b['version'])
            info.append(format_mods(score['enabled_mods']))
            info.append(user_data['pp_country_rank'])
            info.append(user_data['pp_rank'])
            info.append(user_data['pp_raw'])

            SCORES_TO_INSERT.append(info)
    await session.close()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    print("Fetching users from osu website....")
    users = get_users(NUM_PAGES, COUNTRY_CODE)
    
    print("Scanning top scores....")
    tasks = []
    for index, user in enumerate(users):
        tasks.append(loop.create_task(task(user)))
        if (index % 20 == 0):
            loop.run_until_complete(asyncio.wait(tasks))
            time.sleep(1)

    print(f"Found total of {len(SCORES_TO_INSERT)} scores!")
    creds = ServiceAccountCredentials.from_json_keyfile_name(
            "creds.json",
            SCOPE
    )
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).sheet1

    print("Inserting to google sheets...")
    sheet.insert_rows(SCORES_TO_INSERT, 2, "USER_ENTERED")
    print("Done...")
