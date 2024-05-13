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


start_time = time.time()
urlroot = 'https://www.olympiandatabase.com'

# create dataframe
columnnames = ['city', 'year', 'type', 'sport', 'event', 'medal', 'country_code', 'athlete_full_name']
dummy = [['Askaban', 1234, 'Winter', 'Quidditch', 'Team M', 'Gold', 'GRY', 'Harry Potter'], ['Askaban', 1134, 'Winter', 'Quidditch', 'Team M', 'Gold', 'SLY', 'Tom Riddle']]
maintable = pd.DataFrame(dummy, columns = columnnames)

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
                    # Add athlete's name
                    list_of_values.append(thingy.parent.next_sibling.next_sibling.td.next_sibling.get_text())
                    # Add row to table
                    maintable.loc[len(maintable)] = list_of_values
                    # Go recursively until no more team members left
                    get_team_members(thingy.parent.next_sibling.next_sibling.td, list_of_values[:-1])

# At this point, we have ['city', 'year', 'type', 'sport', event]
def get_people(soup, list_of_values):
    # This will be used to determine whether it is a team event where individual names aren't given
    discriminant = True
    for thing in soup.css.select("td > img")[1:]:
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
        list_of_values_copy.append(thing.parent.next_sibling.next_sibling.next_sibling.get_text())
        # (first) athlete's full name
        list_of_values_copy.append(thing.parent.next_sibling.get_text())
        # Add row to table
        maintable.loc[len(maintable)] = list_of_values_copy
        # will be used to find more teammates in case it's a team event where individual names are given
        get_team_members(thing.parent, list_of_values_copy[:-1])
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
                # The athlete's name is actually just the name of the country
                list_of_values_copy.append(thing.next_sibling.get_text())
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
                    #print("Event: ", thing.previous_sibling.previous_sibling.previous_sibling.previous_sibling.get_text())
                    list_of_values_copy.append(unidecode.unidecode(thing.previous_sibling.previous_sibling.previous_sibling.previous_sibling.get_text()))
                    print(list_of_values_copy)
                    # Create next url
                    nexturl = urlroot + str(thing.previous_sibling.previous_sibling.previous_sibling.previous_sibling.a['href'])
                    # Store next url and values array
                    list_of_events_urls.append([nexturl, list_of_values_copy])
    number_of_events = len(list_of_events_urls)
    soups = []
    async def helper_events():
        async with httpx.AsyncClient() as client:

            tasks = []
            for number in range(0,number_of_events):
                #print(list_of_events_urls[number][0])
                url = list_of_events_urls[number][0]
                tasks.append(asyncio.ensure_future(helper(client, url)))

            ans = await asyncio.gather(*tasks)

            print(type(ans))
            for soup in ans:
                print(type(soup))
                #print(soup)
            for number in range(0,number_of_events):
                #print(list_of_events_urls[number][0])
                list_of_events_urls[number].append(ans[number])
                #tasks[number].cancel()
    #loop = asyncio.get_running_loop()
    #loop.create_task(helper_events2())
    #asyncio.run(helper_events2)
    #loop = asyncio.get_event_loop()
    #asyncio.run_coroutine_threadsafe(helper_events2(), loop)
    asyncio.run(helper_events())   
      
    # list_of_urls = [url, ["text", Event], soup]
    for i in list_of_events_urls:
        # Get athlete, send values url and the values array
        get_people(i[2], i[1])
        
        #print("this is i: ", i[0])
    # return soups
    
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
    soups = []
    async def helper_sports():
        async with httpx.AsyncClient() as client:

            tasks = []
            for number in range(0,number_of_sports):
                #print(list_of_events_urls[number][0])
                url = list_of_sports_urls[number][0]
                tasks.append(asyncio.ensure_future(helper(client, url)))

            ans = await asyncio.gather(*tasks)

            print(type(ans))
            for soup in ans:
                print(type(soup))
                #print(soup)
            for number in range(0,number_of_sports):
                #print(list_of_events_urls[number][0])
                list_of_sports_urls[number].append(ans[number])
                #tasks[number].cancel()
    #loop = asyncio.get_running_loop()
    #loop.create_task(helper_events2())
    #asyncio.run(helper_events2)
    #loop = asyncio.get_event_loop()
    #asyncio.run_coroutine_threadsafe(helper_events2(), loop)
    asyncio.run(helper_sports())   
      
    # list_of_urls = [url, ["text", Event], soup]
    for i in list_of_sports_urls:
        # Get athlete, send values url and the values array
        get_events(i[2], i[1])

def get_olympics(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html')
    for thing in soup.find_all("td", height = "35"):
        if len(thing.next_sibling.next_sibling.next_sibling.get_text())>0:
            print("City: ", str(thing.next_sibling.next_sibling.get_text())[0:-5])
            print("Year: ", str(thing.next_sibling.next_sibling.get_text())[-4:])
            nexturl = urlroot + str(thing.next_sibling.next_sibling.a['href'])
            get_sports(nexturl)

# Three times faster than in sequence
get_sports('https://www.olympiandatabase.com/index.php?id=109426&L=1', ['Timbuktu', '0'])

#print(objtocheck)
print(maintable)
print("--- %s seconds ---" % (time.time() - start_time))