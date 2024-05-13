from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import time
import threading
import random
import asyncio
import httpx


start_time = time.time()
urlroot = 'https://www.olympiandatabase.com'
            
# Async helper function
async def helper_events1(client, url):
        page = await client.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        #print("hello")
        return soup

# At this point, we have ['city', 'year', 'type', 'sport']
def get_events(url, list_of_values):
    page = httpx.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    # Create empty list
    list_of_events_urls = []
    for thing in soup.find_all("td", height = "25"):
        # Create a copy of the value array that can be refreshed with each event
        list_of_values_copy = list_of_values.copy()
        if thing.img is not None:
            if thing.a is None:
                if thing.previous_sibling is not None and thing.previous_sibling.previous_sibling is not None and thing.previous_sibling.previous_sibling.previous_sibling is not None and thing.previous_sibling.previous_sibling.previous_sibling.previous_sibling is not None:
                    # Event
                    list_of_values_copy.append(thing.previous_sibling.previous_sibling.previous_sibling.previous_sibling.get_text())
                    # Create next url
                    nexturl = urlroot + str(thing.previous_sibling.previous_sibling.previous_sibling.previous_sibling.a['href'])
                    # Store next url and values array
                    list_of_events_urls.append((nexturl, list_of_values_copy))
    number_of_events = len(list_of_events_urls)
    soups = []
    async def helper_events2():
        async with httpx.AsyncClient() as client:

            tasks = []
            for number in range(0,number_of_events):
                #print(list_of_events_urls[number][0])
                url = list_of_events_urls[number][0]
                tasks.append(asyncio.ensure_future(helper_events1(client, url)))

            ans = await asyncio.gather(*tasks)

            print(type(ans))
            for soup in ans:
                print(type(soup))
                #print(soup)
            for number in range(0,number_of_events):
                #print(list_of_events_urls[number][0])
                soups.append(ans[number])
                #tasks[number].cancel()
    #loop = asyncio.get_running_loop()
    #loop.create_task(helper_events2())
    #asyncio.run(helper_events2)
    #loop = asyncio.get_event_loop()
    #asyncio.run_coroutine_threadsafe(helper_events2(), loop)
    asyncio.run(helper_events2())   
    return soups    
    for i in list_of_events_urls:
        # Get athlete, send values url and the values array
        #get_people(i[2], i[1])
        
        #print(i)
        pass
    
objtocheck = get_events('https://www.olympiandatabase.com/index.php?id=109438&L=1', ['test'])
for thing in objtocheck:
    print(thing.find_all("td"))

#print(objtocheck)
print("--- %s seconds ---" % (time.time() - start_time))