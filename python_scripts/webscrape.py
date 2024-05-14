from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import time
import threading
import random
import asyncio
import httpx
import unidecode
import datetime
import os
from dotenv import load_dotenv, dotenv_values
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

# DEFINE METHODS
# Async helper function
async def helper(client, url):
        page = await client.get(url, timeout = None)
        soup = BeautifulSoup(page.text, 'html.parser')
        #print("hello")
        return soup

# At this point, we have ['city', 'year', 'type', 'sport', event, medal, country code]
def get_team_members(thingy, list_of_values):
    # Check if we are in the correct case, i.e. it's a team sport where all the members are listed by name
    if (thingy.parent is not None) & (thingy.parent.next_sibling is not None) & (thingy.parent.next_sibling.next_sibling is not None):
        if (thingy.parent.next_sibling.next_sibling.td is not None) & (thingy.parent.next_sibling.next_sibling.td.next_sibling is not None):
            if len(str(thingy.parent.next_sibling.next_sibling.td.next_sibling.get_text()))>3:
                    # Add athlete's name and time
                    list_of_values.append(thingy.parent.next_sibling.next_sibling.td.next_sibling.get_text())
                    list_of_values.append(str(datetime.datetime.now()))
                    # Add row to table
                    maintable.loc[len(maintable)] = list_of_values
                    # Go recursively until no more team members left
                    get_team_members(thingy.parent.next_sibling.next_sibling.td, list_of_values[:-2])

# At this point, we have ['city', 'year', 'type', 'sport', event]
def get_people(soup, list_of_values):
    # This will be used to determine whether it is a team event where individual names aren't given
    discriminant = True
    for thing in soup.find_all("td", height = "10"):
        if thing.img is not None:
            # Create a copy of the value array that can be refreshed with each event
            list_of_values_copy = list_of_values.copy()
            # If we have any such match, this means that it isn't a team event without individual names
            discriminant = discriminant & False
            # Medal
            if str(thing)[68:74] == "Gold_m":
                list_of_values_copy.append("Gold")
            else:
                list_of_values_copy.append(str(thing)[68:74])
            # Country Code
            list_of_values_copy.append(thing.next_sibling.next_sibling.next_sibling.get_text())
            # (first) athlete's full name and time
            list_of_values_copy.append(thing.next_sibling.get_text())
            list_of_values_copy.append(str(datetime.datetime.now()))
            # Add row to table
            maintable.loc[len(maintable)] = list_of_values_copy
            # will be used to find more teammates in case it's a team event where individual names are given
            get_team_members(thing, list_of_values_copy[:-2])
    # This is the case where it's a team event where don't have individual names
    if discriminant:
        for thing in soup.find_all("td", height = "15"):
            if thing.img is not None:
                # Create a copy of the value array that can be refreshed with each event
                list_of_values_copy = list_of_values.copy()
                # Medal
                if str(thing.img)[68:74] == "Gold_m":
                    list_of_values_copy.append("Gold")
                else:
                    list_of_values_copy.append(str(thing.img)[68:74])
                # This doesn't exist, so we give NA, which not an actual country code
                list_of_values_copy.append("NA")
                # The athlete's name is actually just the name of the country and then the time
                list_of_values_copy.append(thing.next_sibling.get_text())
                list_of_values_copy.append(str(datetime.datetime.now()))
                # Add row to table
                maintable.loc[len(maintable)] = list_of_values_copy

# At this point, we have ['city', 'year', 'type', 'sport']
def get_events(soup, list_of_values):
    # Create empty list
    list_of_events_urls = []
    for thing in soup.find_all("td", height = "25"):
        # Create a copy of the value array that can be refreshed with each event
        list_of_values_copy = list_of_values.copy()
        if thing.img is not None:
            if thing.a is None:
                if thing.previous_sibling is not None and thing.previous_sibling.previous_sibling is not None and thing.previous_sibling.previous_sibling.previous_sibling is not None and thing.previous_sibling.previous_sibling.previous_sibling.previous_sibling is not None:
                    # Event
                    list_of_values_copy.append(unidecode.unidecode(thing.previous_sibling.previous_sibling.previous_sibling.previous_sibling.get_text()))
                    print(list_of_values_copy)
                    # Create next url
                    nexturl = urlroot + str(thing.previous_sibling.previous_sibling.previous_sibling.previous_sibling.a['href'])
                    # Store next url and values array
                    list_of_events_urls.append([nexturl, list_of_values_copy])
    number_of_events = len(list_of_events_urls)
    
    async def helper_events():
        async with httpx.AsyncClient() as client:

            tasks = []
            for number in range(0,number_of_events):
                
                url = list_of_events_urls[number][0]
                tasks.append(asyncio.ensure_future(helper(client, url)))

            ans = await asyncio.gather(*tasks)

            print(type(ans))
            for soup in ans:
                print(type(soup))

            for number in range(0,number_of_events):
                list_of_events_urls[number].append(ans[number])
                
    asyncio.run(helper_events())   
      
    # list_of_urls = [url, ["text", Event], soup]
    for i in list_of_events_urls:
        # Get athlete, send values html code and the values array
        get_people(i[2], i[1])
    
# At this point the list will be [City, Year]
def get_sports(url, list_of_values):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    # Type, i.e. Summer or Winter
    list_of_values.append(str(soup.find_all("h1")[0].get_text())[5:11])
    list_of_sports_urls = []
    for thing in soup.find_all("td", height = "45", align="left"):
        list_of_values_copy = list_of_values.copy()
        # Sport
        list_of_values_copy.append(thing.next_sibling.next_sibling.get_text())
        print(len(list_of_values_copy))
        nexturl = urlroot + str(thing.next_sibling.next_sibling.a['href'])
        list_of_sports_urls.append([nexturl, list_of_values_copy])
    number_of_sports = len(list_of_sports_urls)
    
    async def helper_sports():
        async with httpx.AsyncClient() as client:

            tasks = []
            for number in range(0,number_of_sports):
                url = list_of_sports_urls[number][0]
                tasks.append(asyncio.ensure_future(helper(client, url)))

            ans = await asyncio.gather(*tasks)

            print(type(ans))
            for soup in ans:
                print(type(soup))
                
            for number in range(0,number_of_sports):
                list_of_sports_urls[number].append(ans[number])
 
    asyncio.run(helper_sports())   
      
    # list_of_urls = [url, ["text", Event], soup]
    for i in list_of_sports_urls:
        # Get athlete, send values url and the values array
        get_events(i[2], i[1])

def get_olympics(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    counter = 0
    for thing in soup.find_all("td", height = "35"):
        if len(thing.next_sibling.next_sibling.next_sibling.get_text())>0 and counter < 10:
            list_of_values = []
            list_of_values.append(str(thing.next_sibling.next_sibling.get_text())[0:-5])
            list_of_values.append(str(thing.next_sibling.next_sibling.get_text())[-4:])
            nexturl = urlroot + str(thing.next_sibling.next_sibling.a['href'])
            counter += 1
            get_sports(nexturl, list_of_values)

# DEFINE CODE TO RUN
# start timer
start_time = time.time()

# root url string
urlroot = 'https://www.olympiandatabase.com'

# create dataframe
columnnames = ['CITY', 'YR', 'TYPE', 'SPORT', 'EVENT', 'MEDAL', 'COUNTRY_CODE', 'ATHLETE_FULL_NAME', 'TIME_ADDED']
dummy = [['Askaban', '1234', 'Winter', 'Quidditch', 'Team M', 'Gold', 'GRY', 'Harry Potter', '2024-05-14 01:24:27.978238'], ['Askaban', '1134', 'Winter', 'Quidditch', 'Team M', 'Gold', 'SLY', 'Tom Riddle', '2024-05-14 01:24:26.978238']]
maintable = pd.DataFrame(dummy, columns = columnnames)

# Three times faster than in sequence
get_olympics('https://www.olympiandatabase.com/index.php?id=278979&L=1')

# Open the Snowflake connection
load_dotenv()
conn = snowflake.connector.connect(
    account = os.getenv("SF_ACCOUNT"),
    user = "eiffel",
    password = os.getenv("SF_PASSWORD"),
    role = "ACCOUNTADMIN",  # optional
    warehouse = "COMPUTE_WH",  # optional
    database = "TF_DEMO",  # optional
    schema = "RAW",  # optional 
)
success, nchunks, nrows, _ = write_pandas(conn, maintable, 'RAW_FCT_TABLE')
print("--- %s seconds ---" % (time.time() - start_time))