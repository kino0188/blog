import pandas as pd
import requests as rq

url = 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&download=true'
headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}

data = rq.get(url, headers=headers)
data_json = data.json()['data']
all_data = pd.DataFrame(data = data_json['rows'], columns = data_json['headers'])

all_data.head()

len(all_data)

print(len(all_data))

all_data = all_data.loc[~all_data.symbol.duplicated(keep = 'first'), :]
all_data = all_data.loc[~all_data.name.duplicated(keep = 'first'), :]

all_data['marketCap'] = pd.to_numeric(all_data['marketCap'])
all_data = all_data.loc[all_data['marketCap'] != 0]
all_data = all_data.loc[~all_data['symbol'].str.contains('\\^')]
all_data = all_data.loc[~all_data['name'].str.contains('Preferred')]
all_data = all_data.loc[~all_data['name'].str.contains('preferred')]
all_data = all_data.loc[~all_data['name'].str.contains('ETF')]
all_data = all_data.loc[~all_data['name'].str.contains('Fund')]
all_data = all_data.loc[~all_data['name'].str.contains('%')]
all_data['symbol'] = all_data['symbol'].str.replace('\\/', '-')
all_data = all_data.sort_values(by = 'symbol')
all_data = all_data.reset_index(drop=True)

len(all_data)

print(len(all_data))

all_data.to_csv('data/US_ticker.csv')