# https://us.etrade.com/knowledge/thematic-investing
# https://www.ishares.com/ch/individual/en/themes/megatrends/climate-change-and-resource-scarcity
# https://research2.fidelity.com/pi/stock-screener#themes

import requests
from bs4 import BeautifulSoup
import finnhubutils as utils
from datetime import datetime
import json
import finnhub
import os

finnhub_client = finnhub.Client(api_key=os.environ['FINNHUB_API_KEY'])

### Fidelity
themes = [{'theme': '3dPrinting', 'ThemeId': '200', 'OptionId': '204'},
          {'theme': 'ai', 'ThemeId': '200', 'OptionId': '213'},
          {'theme': 'bigData', 'ThemeId': '200', 'OptionId': '210'},
          {'theme': 'cloud', 'ThemeId': '200', 'OptionId': '203'},
          {'theme': 'drones', 'ThemeId': '200', 'OptionId': '202'},
          {'theme': 'fintech', 'ThemeId': '200', 'OptionId': '206'},
          {'theme': 'founder', 'ThemeId': '200', 'OptionId': '209'},
          {'theme': 'organicFood', 'ThemeId': '200', 'OptionId': '207'},
          {'theme': 'robotics', 'ThemeId': '200', 'OptionId': '201'},
          {'theme': 'mobilePayment', 'ThemeId': '200', 'OptionId': '211'},
          {'theme': 'hr', 'ThemeId': '200', 'OptionId': '208'},
          {'theme': 'tobaccoAlcoholGambling', 'ThemeId': '200', 'OptionId': '212'},
          {'theme': 'windEnergy', 'ThemeId': '200', 'OptionId': '205'},
          ]

headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'https://research2.fidelity.com',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://research2.fidelity.com/pi/stock-screener',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8,de;q=0.7',
}

sql_data = [['theme', 'updated', 'data']]
for theme in themes:
    data = [
        ('ResultFields[]', 'ticker'),
        ('ResultFields[]', 'FI.CompanyName'),
        ('ResultFields[]', 'FI.IssueType'),
        ('ResultFields[]', 'FI.IntradayPrice'),
        ('ResultFields[]', 'FI.MarketCap'),
        ('ResultFields[]', 'FI.Vol90dAvg'),
        ('ResultFields[]', 'FI.SubIndustryGicsCode'),
        ('AjaxId', '1'),
        ('Criteria',
         '[{"ArgsOperator":null,"Arguments":[],"Clauses":[{"Operator":"Equals","Values":[1]}],"ClauseGroups":[],"Field":' +
         f'"FIST.IsOption{theme["OptionId"]}"' + ',"Identifiers":[]}]'),
        ('ResultView', 'SearchCriteria'),
        ('FirstRow', '0'),
        ('RowCount', '100'),
        ('SortDir', ''),
        ('SortField', 'FI.MarketCap'),
        ('SortResults', 'true'),
        ('InitialLoad', 'true'),
        ('ScreenerId', '128'),
        ('ThemeId', theme['ThemeId']),
        ('OptionId', theme['OptionId']),
    ]

    r = requests.post('https://research2.fidelity.com/pi/stock-screener/LoadResults', headers=headers, data=data)
    print(theme)

    j = r.json()
    soup = BeautifulSoup(j['html'])
    data = []
    for s in soup.find_all('a'):
        if s.get('data-symbol') is not None:
            data.append({'symbol': s.get('data-symbol')})

    print('Found', len(data))
    if len(data) > 1:
        sql_data.append([theme['theme'], datetime.utcnow().strftime("%Y-%m-%d"), json.dumps(data)])

### ETFs
etfs = [['ageingPopulation', ['AGED.L']], ['digitalisation', ['DGTL.L']], ['futureVehicles', ['IDRV']],
        ['globalWater', ['IH2O.L']], ['cyberSecurity', ['HACK', 'CIBR']], ['futureEntertainment', ['IEME', 'NERD']],
        ['healthcareInnovation', ['IEIH', 'SBIO']], ['gaming', ['GAMR', 'HERO']], ['globalCleanEnergy', ['ICLN']]
        ]

for theme in etfs:
    tickers = []
    for etf in theme[1]:
        holdings = finnhub_client.etfs_holdings(etf)['holdings'][:30]
        for h in holdings:
            if h['symbol'] is not None and h['symbol'] != '':
                if h['symbol'] not in tickers:
                    tickers.append(h['symbol'])

    data = []
    for ticker in tickers:
        data.append({'symbol': ticker})

    sql_data.append([theme[0], datetime.utcnow().strftime("%Y-%m-%d"), json.dumps(data)])

###### Manual
arr = [['financialExchangesData',
        ['ICE', 'NDAQ', 'CBOE', 'FDS', 'SPGI', 'TW', 'TRI', 'CME', 'COIN', 'MSCI', 'MKTX', 'MORN', 'OTCM', 'MCO']],
       ['futureFood', ['BYND', 'K', 'HRL', 'OTLY', 'INGR', 'TTCF', 'STKL', 'LSF', 'NESN.SW', 'KR']],
       ]

for theme in arr:
    data = []
    for s in theme[1]:
        data.append({'symbol': s})
    sql_data.append([theme[0], datetime.utcnow().strftime("%Y-%m-%d"), json.dumps(data)])

mysql_client = utils.MysqlClient()
mysql_client.add_db('investment_theme', sql_data)
