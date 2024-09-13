import logging
import concurrent.futures
import json
import os
import re
import numpy as np
import pandas as pd 
from sqlalchemy import create_engine
from decouple import config
from datetime import timedelta
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# --------------- Global Variables just for logging purposes------------------------
# Logging - Track information on data parsing status
def configure_logger(logger_name, log_file, level = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(filename)s - line: %(lineno)d - %(levelname)s - %(message)s')

    handler = logging.FileHandler(log_file, mode='w')
    # Modify this to filter the message displayed in log
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    # which messages are directed to log
    logger.setLevel(level)
    return logger

# Create handler for csv and webscraping process
# csvLogger = configure_logger('csvLogger', 'log/csv.log')
scrapingLogger = configure_logger('spotifyLogger', '../log/spotify.log')

def createDriver(): 
    """ 
        Web-drivers are not thread safe -- they sort of resemable a thread themselve
        Therefore for each thread, we need to create a seperate webdriver
    """
    options = webdriver.ChromeOptions()
    # headless option use with caution can have authentication issues when multithreading and can be blocked
    # options.add_argument('--headless')
    # options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=options)
    return driver

def addInput(driver: webdriver, by: By, value: str, text: str):
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((by, value))
        )
        element.clear()
        element.send_keys(text)
    except: 
        # This usually occurs when login is already processed so youre in the login portal 
        scrapingLogger.debug('Already Logged In')
        return


def clickButton(driver: webdriver, by: By, value: str):
    try: 
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((by, value))
        )
        button.click()
    except: 
        # same logic as above
        scrapingLogger.debug('Button clicked: Already loggin in')
        return


def loginSpotify(driver: webdriver, maxRetries = 2): 
    """ 
        Spotify will redirect all attempts to webscraping to default login page. 
        Use Selenium to dynamically login and continue webscraping all the Global charts INFO
        loginSpotify() sends by username and password and clicks submit when 
        fields are filled. 
    """
    username = config("SPOTIFY_USERNAME")
    password = config("SPOTIFY_PASSWORD")

    # Go to login page, wait for page to load, config and simulate login
    for iter in range(maxRetries+1):
        driver.get("https://accounts.spotify.com/en/login")
        try:
            addInput(driver, by=By.ID, value='login-username', text=username)
            addInput(driver, by=By.ID, value='login-password', text=password)
            clickButton(driver, by=By.ID, value='login-button')
            # Delay for button click to be processed and login processed - adjust as needed
            login = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="status-logged-in"]'))
            )
            if login: 
                scrapingLogger.debug("Successfully Login to spotify")
                return 
        except TimeoutException as e:
            if iter < maxRetries:
                 scrapingLogger.warning(f"Attempt {iter + 1}: Login webpage did not load. Retrying...")
            else:
                scrapingLogger.fatal("Maximum retries reached. Could not authenticate login.")
                return  
            

def createDate(startDate: str, endDate: str, dateFormat: str = '%Y-%m-%d') -> list:
    """ 
        Purpose: Returns a list of dates between the 
        specificed two inputs startDate and endDate 
        -- Used for webscraping to obtain data between during time interval
    """
    currentDate = datetime.strptime(startDate, dateFormat)
    endDate = datetime.strptime(endDate, dateFormat)

    dates = list()
    incrementTime  = timedelta(days=1)

    while(currentDate <= endDate):
        dates.append(currentDate.strftime(dateFormat))
        currentDate = currentDate + incrementTime

    return dates

def scrapeTop200Songs(input: str, date: str, region: str, type: str):
    '''
    Parses the input string from a webpage and returns data as a list of dictionaries.
    Parameters: 
        date: The date of the chart
        region: The region of the chart
        type: The type of chart ('song' or 'artist')
    
    Returns:
        List of dictionaries with parsed data
    '''
    try:
        match = re.search("\n1\n", input)
        if(match):
            # Parsing format follows: position, position_change, track_title, artist_name, 
            # and one line string containing space seperated values peak, prev, streak, streams
            # Every 5 lines is a new song which should be an a new list -- varies based on chart page
            data = list()
            entryIndex = -1
            modBy = 4
            if type == 'song': 
                modBy = 5

            for i, entry in enumerate(input[match.start()+1:].split("\n")): 
                if i % modBy == 0: 
                    data.append(list())
                    entryIndex +=1 
                    data[entryIndex].extend([date, region])
                elif (i+1) % modBy == 0: 
                    data[entryIndex].extend(entry.split(' '))
                    continue
                data[entryIndex].append(entry)

        scrapingLogger.info(f"Data parsing completed for {date} - {region} with type '{type}'")
        return data

    except Exception as e:
        scrapingLogger.fatal(f'An error occurred: {str(e)}')
        return []
    
def getChartElement(driver) -> str: 
    '''
        Get the top 200 chart elements as a string 
        Regarding elements: There are various ways to parse the ouput 
        including using various html/css elements to break it down such as tr
        Here, using brute force string manipulating to avoid html jargon
    '''
    try: 
        # Wait for the charts table to be present
        charts_table_selector = '[data-testid="charts-table"]'
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, charts_table_selector))
        )
        elements = driver.find_elements(By.CSS_SELECTOR, charts_table_selector)
        stringInput = elements[0].text
        scrapingLogger.info("Top 200 songs found and returned")
        return stringInput
    except TimeoutException as e: 
        scrapingLogger.fatal("chart element not found -- page not loaded properly and or invalid navigation links")
        
def writeRegions():
    ''' 
        Write each region/country available on spotify charts to regions.json 
        Format example follows the example; Argentina: [ar, 2017/01/2017]
        First item in array is the country abbreviation and second line a date
    '''
    # Note the starting date here is used to check if the region had Spotify at that time
    # ie if it exists the date would match else the first available date is returned
    startDate = '2017-01-01'
    url = f'https://charts.spotify.com/charts/view/regional-global-daily/{startDate}'
    driver = createDriver()
    loginSpotify(driver)
    driver.get(url)
    try: 
        # Click the dropdown menu for Regions
        clickButton(driver, by=By.ID, value='entity-search')

        # Wait for popUp container to appear
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@data-popover-root="true"]'))
        )
        popUpContainer = driver.find_element(By.XPATH, '//*[@data-popover-root="true"]')
        scrapingLogger.info('Pop Up Container located')

        listBox = popUpContainer.find_elements(By.XPATH, '//ul[@role="listbox"]/li')
        #Create list to help populate the dictionary for json file later on
        regionName = list()
        regionLink = list()
        # Loop through each li element and extract text and data-key attribute
        for liElement in listBox:
            text = liElement.find_element(By.XPATH, './/*[contains(@class, "EntityOptionDisplay__TextContainer")]').text
            link = liElement.get_attribute('data-key')
            regionLink.append(link)
            regionName.append(text)
        scrapingLogger.info(f'All region Names and links retrieved \nregionNames:{regionName} \nregionLink{regionLink}')

        scrapingLogger.info("Begin parsing links for region.json")
        regionDict = dict()
        abbrPtrn = re.compile(r'(?<=-)\D+(?=-)')
        datePtrn = re.compile(r'\d{4}-\d{2}-\d{2}')
        for i, region in enumerate(regionName): 
            regionDict[region] = list()
            #Parse every link to get the region abbreviation and valid date
            abbrResult  = abbrPtrn.search(regionLink[i])
            dateResult = datePtrn.search(regionLink[i])
            regionDict[region].append(abbrResult.group(0))
            regionDict[region].append(dateResult.group(0))

        with open('region.json', 'w')as json_file:
            json.dump(regionDict, json_file, indent=2) 
        scrapingLogger.info('Wrote Dictionary to region.json')
    except TimeoutException as e: 
        scrapingLogger.fatal(f'Pop Up container not found {str(e)}')
    except NoSuchElementException as e :
        scrapingLogger.fatal(str(e))

def scrapeCharts(driver: webdriver, dates: list, regionDict: dict):
    '''
    Scrapes both artist rankings and global charts for the specified dates and regions.
    
    Parameters:
        driver (webdriver): Selenium WebDriver instance.
        dates (list): List of dates to scrape.
        regionDict (dict): Dictionary of regions and their abbreviations.
        
    Returns:
        tuple: (artistRankData, globalChartsData)
            - artistRankData: List of dictionaries containing artist rank data.
            - globalChartsData: List of dictionaries containing global charts data.
    '''
    artistRankData = []
    globalChartsData = []

    for date in dates:
        for region, (abbr, _) in regionDict.items():
            # Artist Rankings
            artistUrl = f'https://charts.spotify.com/charts/view/artist-{abbr}-daily/{date}'
            driver.get(artistUrl)
            scrapingLogger.info(f'Begin webscraping Artist Chart for {date} {region}')
            top200Artist = getChartElement(driver)
            artistData = scrapeTop200Songs(top200Artist, date, region, 'artist')
            artistRankData.extend(artistData)
            scrapingLogger.info(f'Finished webscraping Artist Chart for {date} {region}')
            
            # Song Streams, essentially the same process but different URL
            songUrl = f'https://charts.spotify.com/charts/view/regional-{abbr}-daily/{date}'
            driver.get(songUrl)
            scrapingLogger.info(f'Begin webscraping Global Chart for {date} {region}')
            top200Global = getChartElement(driver)
            globalData = scrapeTop200Songs(top200Global, date, region, 'song')
            globalChartsData.extend(globalData)
            scrapingLogger.info(f'Finished webscraping Track Chart for {date} {region}')

    return artistRankData, globalChartsData

def parallelSpotifyScrapeHelper(dateChunks, regionChunk):
    ''' 
        Helper function for scripts to run in parallel. Each process needs its own Driver.
    '''
    driver = createDriver()
    loginSpotify(driver)  # Perform login
    artistData, songData = scrapeCharts(driver, dateChunks, regionChunk)  # Perform scraping
    return artistData, songData

def main(): 
    jsonPath = 'region.json'
    if not os.path.exists(jsonPath): 
        writeRegions(jsonPath)
    with open(jsonPath, 'r') as jsonFile:
        regionDict = json.load(jsonFile)
    
    # Initialize needed variables and split dates into n-thread-partitions 
    startDate = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    endDate = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    dates = createDate(startDate, endDate)
    nProcesses = 4

    # Every process should process all the dates
    dateSplit = [dates for _ in range(nProcesses)]
    
    # Split up regions amongst processes -> this is exhaustive compared to splitting across dates
    regionSplit = [dict(list(regionDict.items())[i * len(regionDict) // nProcesses: (i + 1) * len(regionDict) // nProcesses])
        for i in range(nProcesses)]

    songResults = []
    artistResults = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=nProcesses) as executor:
        # Submit scraping tasks for global charts 
        globalDataFutures = [executor.submit(parallelSpotifyScrapeHelper, dateChunk, regionChunk) 
                             for dateChunk, regionChunk in zip(dateSplit, regionSplit)]
        # Collect and parse those results
        for future in concurrent.futures.as_completed(globalDataFutures):
            artistData, songData = future.result()
            artistResults.extend(artistData)
            songResults.extend(songData)

    # ----- Some data processing, specifically fixing the change column --------
    song_df = pd.DataFrame(songResults)
    song_df.columns = ['date', 'region', 'position', 'change', 'track_title', 'artist_name',
                            'peak', 'prev', 'streak', 'streams']
    artist_df = pd.DataFrame(artistResults)
    artist_df.columns = ['date', 'region', 'position', 'change', 'artist_name',
                            'peak', 'prev', 'streak']

    def mutate_change(df):
        # the track change and rank change in spotify is indicated visually and is a mixed type column 
        # specifically need to append a '-' to indicate a negative change everything else can stay the same
        df = df.assign(change=np.select(
            condlist=[
                df['prev'] == '—',
                df['prev'].str.len() > df['position'].astype(str).apply(len),
                df['prev'].str.len() < df['position'].astype(str).apply(len),
                df['prev'] < df['position'].astype(str),
                True
            ],
            choicelist=[
                df['change'], 
                df['change'], 
                "-" + df['change'], 
                "-" + df['change'], 
                df['change']
            ]
        ))
        return df

    song_df = mutate_change(song_df)
    song_df['streams'] = song_df['streams'].str.replace(',', '').astype(int)
    artist_df = mutate_change(artist_df)

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

    song_df.to_sql('song_streams', con=engine, if_exists='append', index=False)
    artist_df.to_sql('artist_rank', con=engine, if_exists='append', index=False)


if __name__ == "__main__":
        main()
