import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime, timedelta
from decouple import config
from sqlalchemy import create_engine

def main():
    url = 'https://kworb.net/spotify/artist/06HL4z0CvFAxyc27GXpf02_songs.html'
    response = requests.get(url)

    if response.status_code != 200:
        print(f'Failed to retrieve the webpage with status code {response.status_code}')
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    line_break = soup.find('br')

    if not line_break:
        print('<br> tag not found ')
    else:
        parent_element = line_break.find_parent()
        text = ''.join(str(content) for content in parent_element.contents if content.name != 'table')
        
        # parse for the date
        date_pattern = re.compile(r'Last updated: (\d{4}/\d{1,2}/\d{1,2})')
        match = date_pattern.search(text)
        # Ideally there should always be a date found if not just use current date -1
        if match:
            try:
                extracted_date = (datetime.strptime(match.group(1), '%Y/%m/%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            except ValueError:
                extracted_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            extracted_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    table = soup.find('table', class_='addpos sortable')
    if not table:
        print('Table not found -- inspect the tags')
        return

    rows = table.find_all('tr')
    data = [[cell.text.strip() for cell in row.find_all(['td', 'th'])] for row in rows]

    if len(data) <= 1:
        print('No data found in the table -- inspect the tags')
        return

    df = pd.DataFrame(data[1:], columns=[col.lower().replace(' ', '_') for col in data[0]])
    df = (df
          # There's some encoding issues with the name and spacing issues
            .assign(song_title=lambda x: x.song_title.str.replace("â", "’", regex = True)
                                                        .str.replace("*", '', regex=False)
                                                        .str.replace('\s+', ' ', regex=True)
                                                        .str.strip())
                # change all columns to lower case
            .rename(columns=lambda col_name: col_name.lower().replace(' ', '_'))
            .assign(streams=lambda x: x.streams.str.replace(',', '').astype(int),
                    date = extracted_date, 
                    daily = lambda x: x.daily.str.replace(',', '').astype(int))
            .rename(columns = {'song_title':'track_title', 'daily': "daily_streams", 'streams' :"total_streams"})
            .loc[:,['date', 'track_title', 'total_streams', 'daily_streams']]
            # There shouldn't be duplicate track titles but somethings it slips through
            .drop_duplicates(subset=['track_title'])
        )

    print(f"Data processed for {extracted_date}")

     # Load information from .env
    DB_USERNAME = config('POSTGRES_USER')
    DB_PASSWORD = config('POSTGRES_PASSWORD')
    DB_NAME = config('POSTGRES_DB')
    DB_HOST = config('DB_HOST')
    DB_PORT = config('DB_PORT')

    # url for database connection
    DATABASE_URL = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Create connection 
    engine = create_engine(DATABASE_URL)

    df.to_sql('daily_streams', con=engine, if_exists='append', index=False)

if __name__ == "__main__":
    main()
