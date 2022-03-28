import csv
import json
import os
import threading
import time
import traceback
from datetime import datetime, tzinfo

import gspread
import requests
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from pytz import timezone

markets = {
    151: "Assists",
    152: "Blocks",
    156: "Points",
    157: "Rebounds",
    160: "Steals",
    # 162: "Threes",
    # 147: "Most points",
    # 142: "first-basket",
    # 136: "double-double"
}
nba_map = {
    # "PLAYER_NAME": "Player",
    # "TEAM_ABBREVIATION": "Team",
    "PTS": "Points",
    "AST": "Assists",
    "STL": "Steals",
    "BLK": "Blocks",
    "REB": "Rebounds",
    # "3PM": "Threes"
}

outCSV = "Out.csv"
nba_data = {}
bp = "https://www.bettingpros.com/nba/odds/player-props/"
threadcount = 10
semaphore = threading.Semaphore(threadcount)
lock = threading.Lock()
headers = ["PLAYER NAME", "PROP OVER", "PROP UNDER", "AVG", "AVG L5", "AVG L10", "HOME AVG", "HOME L5", "HOME L10",
           "AWAY AVG", "AWAY L5", "AWAY L10", "HIT %"]
all_data = []
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
sheet = "https://docs.google.com/spreadsheets/d/"
html = f"<b>Last refreshed: {datetime.now(timezone('US/Central')).strftime('%d:%m:%Y %H:%M %p')} </b><br>"
# input(html)
red = "#EA4335"
yellow = "#FFFF00"
green = "#00FF00"


def getProps(player, events):
    with semaphore:
        try:
            params = (
                ('player_slug', player['player']['slug']),
                ("market_id", ":".join([str(x) for x in markets.keys()])),
                ("event_id", events),
                ('location', 'ALL'),
            )
            js = requests.get('https://api.bettingpros.com/v3/offers',
                              headers={'x-api-key': 'CHi8Hy5CEE4khd46XNYL23dCFX96oUdw6qOt1Dnh'},
                              params=params).json()
            data = {}
            for offer in js['offers']:
                key = f"{markets[offer['market_id']]}"
                data[key] = {}
                for selection in offer['selections']:
                    for book in selection['books']:
                        for line in book['lines']:
                            if line['best']:
                                data[key][selection['label']] = f"{line['line']} ({line['cost']})"
                    for book in selection['books']:
                        for line in book['lines']:
                            data[key][selection['label']] = f"{line['line']} ({line['cost']})"
                            break
            # pprint("Bettingpros", json.dumps({player['name']: data}, indent=4))
            name = player['name']
            for key in nba_data.keys():
                if name in key or key in name:
                    name = key
                    break
            if data == {}:
                pprint(f"No line found for {player['name']} on bettingpros.")
            if name not in nba_data.keys():
                pprint(f"No line found for {name} on NBA.com")
                nba = {}
            else:
                nba = nba_data[name]
            newdata = {player['name']: nba}
            for key in data.keys():
                for ou in data[key]:
                    if key not in newdata[player['name']].keys():
                        newdata[player['name']][key] = {}
                    newdata[player['name']][key][ou] = data[key][ou]
            pprint(json.dumps(newdata, indent=4))
            if "Team" not in newdata[player['name']].keys():
                newdata[player['name']]["Team"] = player['player']['team']
            # append(newdata)
            all_data.append(newdata)
        except:
            traceback.print_exc()
            pprint(f"Error {events} {player}")
            with open("Error.csv", 'a') as errorfile:
                csv.writer(errorfile).writerow([events, player])


def append(row):
    with lock:
        with open(outCSV, 'a', newline='', encoding='utf8', errors='ignore') as outfile:
            csv.writer(outfile).writerows(getRows(row))


def getPlayers():
    soup = BeautifulSoup(requests.get(bp).content, 'lxml')
    js = {}
    for script in soup.find_all('script'):
        if "offer-counts" in str(script):
            js = json.loads(script.text.replace("var odds =", "").strip()[:-1])
    events = ":".join([str(event['id']) for event in js['events']['events']])
    # pprint("Events", events)
    players = []
    for market in js['offer-counts']['player-props']:
        for player in market['participants']:
            if player not in players:
                players.append(player)
    pprint(f"Got {len(players)} players.")
    threads = []
    for player in players:
        thread = threading.Thread(target=getProps, args=(player, events,))
        thread.start()
        threads.append(thread)
        time.sleep(0.1)
    for thread in threads:
        thread.join()


def pprint(msg):
    m = f'{str(datetime.now()).split(".")[0]} | {msg}'
    print(m)
    # with open("logs.txt", 'a') as logfile:
    #     logfile.write(m + "\n")


def getNBA(n=0, location=""):
    pprint(f"Working on {location} NBA#{n}".replace("  ", " "))
    params = (
        ('College', ''),
        ('Conference', ''),
        ('Country', ''),
        ('DateFrom', ''),
        ('DateTo', ''),
        ('Division', ''),
        ('DraftPick', ''),
        ('DraftYear', ''),
        ('GameScope', ''),
        ('GameSegment', ''),
        ('Height', ''),
        ('LastNGames', n),
        ('LeagueID', '00'),
        ('Location', location),
        ('MeasureType', 'Base'),
        ('Month', '0'),
        ('OpponentTeamID', '0'),
        ('Outcome', ''),
        ('PORound', '0'),
        ('PaceAdjust', 'N'),
        ('PerMode', 'PerGame'),
        ('Period', '0'),
        ('PlayerExperience', ''),
        ('PlayerPosition', ''),
        ('PlusMinus', 'N'),
        ('Rank', 'N'),
        ('Season', '2021-22'),
        ('SeasonSegment', ''),
        ('SeasonType', 'Regular Season'),
        ('ShotClockRange', ''),
        ('StarterBench', ''),
        ('TeamID', '0'),
        ('TwoWay', '0'),
        ('VsConference', ''),
        ('VsDivision', ''),
        ('Weight', ''),
    )
    response = requests.get('https://stats.nba.com/stats/leaguedashplayerstats',
                            headers={'User-Agent': 'Mozilla/5.0', 'x-nba-stats-origin': 'stats'},
                            params=params)
    js = response.json()
    header = [x for x in js['resultSets'][0]['headers']]
    for row in js['resultSets'][0]['rowSet']:
        player = row[1]
        if player not in nba_data.keys():
            nba_data[player] = {"Team": row[4]}
        for i in range(len(header)):
            if header[i] in nba_map.keys():
                league = f"{nba_map[header[i]]}"
                if league not in nba_data[player].keys():
                    nba_data[player][league] = {}
                nba_data[player][league][f"{location} {n}".strip()] = row[i]


def main():
    global nba_data,html,all_data
    while True:
        logo()
        all_data = []
        if not os.path.isfile(outCSV):
            with open(outCSV, 'w', newline='', encoding='utf8', errors='ignore') as tfile:
                x = csv.writer(tfile)
                x.writerow(headers)
        pprint("Fetching data from NBA.com...")
        threads = []
        for location in ["", "Home", "Road"]:
            for i in [0, 5, 10]:
                thread = threading.Thread(target=getNBA, args=(i, location,))
                thread.start()
                threads.append(thread)
                time.sleep(0.1)
        for thread in threads:
            thread.join()
        pprint("Fetching from NBA finished. Now fetching from bettingpros...")
        getPlayers()
        all_data.sort(key=lambda js: js[[k for k in js.keys()][0]]['Team'], reverse=False)
        with open("result.json", 'w') as resfile:
            json.dump(all_data, resfile, indent=4)
        html = f"<b>Last refreshed: {datetime.now(timezone('US/Central')).strftime('%d:%m:%Y %H:%M %p')} </b><br>"
        print(html)
        for data in all_data:
            append(data)
        pprint(f"All done! Results saved in {outCSV}, Generating HTML page...")
        writeHtml()
        uploadCSV(outCSV, sheet)
        pprint("Waiting for 30 mins...")
        time.sleep(30)


def writeHtml():
    with open("page.html") as pfile:
        table = pfile.read()
    with open("index.html", 'w') as ifile:
        ifile.write(str(BeautifulSoup(table.replace("<tr></tr>", html), 'lxml')))
    with open('index.html') as ifile:
        res = requests.post('url', data={"abc": ifile.read()})
        print(res.content)


def uploadCSV(ucsv, usheet):
    pprint("Uploading CSV..")
    credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', SCOPES)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_url(usheet)
    with open(ucsv, 'r') as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet.id, data=content)
    pprint(f"{ucsv} uploaded to {usheet}")


def create():
    spreadsheet_details = {
        'properties': {
            'title': 'URLs'
        }
    }
    credentials = service_account.Credentials.from_service_account_file('client_secret.json', scopes=SCOPES)
    spreadsheet_service = build('sheets', 'v4', credentials=credentials)
    drive_service = build('drive', 'v3', credentials=credentials)
    sht = spreadsheet_service.spreadsheets().create(body=spreadsheet_details, fields='spreadsheetId').execute()
    sheetId = sht.get('spreadsheetId')
    pprint('Spreadsheet ID: {0}'.format(sheetId))
    permission1 = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': '786hassan777@gmail.com'
    }
    drive_service.permissions().create(fileId=sheetId, body=permission1).execute()


def logo():
    os.system("color 0a")
    os.system("cls")
    print(fr"""
     ____         _    _    _                _____                    _   _   _  ____           
    |  _ \       | |  | |  (_)              |  __ \                  | | | \ | ||  _ \    /\    
    | |_) |  ___ | |_ | |_  _  _ __    __ _ | |__) |_ __  ___   ___  | | |  \| || |_) |  /  \   
    |  _ <  / _ \| __|| __|| || '_ \  / _` ||  ___/| '__|/ _ \ / __| | | | . ` ||  _ <  / /\ \  
    | |_) ||  __/| |_ | |_ | || | | || (_| || |    | |  | (_) |\__ \ | | | |\  || |_) |/ ____ \ 
    |____/  \___| \__| \__||_||_| |_| \__, ||_|    |_|   \___/ |___/ | | |_| \_||____//_/    \_\
                                       __/ |                         | |                        
                                      |___/                          |_|      
======================================================================================================= 
                          Scrapes data from bettingpros.com and nba.com.
                          Developed by: https://github.com/evilgenius786                 
=======================================================================================================
[+] Multithreaded (Thread count = {threadcount})
[+] Without browser!
[+] CSV/JSON output
[+] Super fast
[+] Error handling
[+] Proper logging
_______________________________________________________________________________________________________                  
""")


def getRows(js):
    global html
    # pprint(json.dumps(js, indent=4))
    name = [k for k in js.keys()][0]
    team = js[name]['Team']
    rows = [[], [f"{name} ({team})",
                 # "PROP OVER", "PROP UNDER", "AVG", "AVG L5", "AVG L10", "HOME AVG", "HOME L5",
                 # "HOME L10", "AWAY AVG", "AWAY L5", "AWAY L10", "HIT %"
                 ]]
    html += "<tr></tr>"
    for row in rows:
        html += '<tr>'
        for word in row:
            html += f'<td colspan="{len(headers)}"><b>{word}<b></td>'
        html += "</tr>"
    for key in ["Points", "Rebounds", "Assists", "Steals", "Blocks"]:
        row = [key]
        html += f"<tr><td><b>{key}</b></td>"
        try:
            odd = float(js[name][key]['Over'].split(" ")[0])
        except:
            odd = 1000
        hit = 0.0
        total = 0.0
        for w in ['Over', 'Under', '0', '5', '10', 'Home 0', 'Home 5', 'Home 10', 'Road 0', 'Road 5', 'Road 10']:
            try:
                style = ""
                try:
                    if w in js[name][key].keys() and isinstance(js[name][key][w], float):
                        total += 1
                        if js[name][key][w] >= odd:
                            hit += 1
                            if js[name][key][w] > odd:
                                style = f' style="background-color: {green};" '
                            else:
                                style = f' style="background-color: {yellow};" '
                        else:
                            style = f' style="background-color: {red};" '
                except:
                    traceback.print_exc()
                row.append(js[name][key][w])
                html += f"<td {style}>{js[name][key][w]}</td>"
            except:
                html += f"<td>-</td>"
                row.append('-')
        try:
            perc = round((hit / total) * 100.0, 2)

        except:
            perc = 0.0
        row.append(f"{perc}%")
        html += f"<td>{perc}%</td>"
        rows.append(row)
    html += "</tr><tr></tr>"
    rows.append([])
    return rows


if __name__ == '__main__':
    main()
