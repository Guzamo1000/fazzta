import asyncio
import aiohttp
from bs4 import BeautifulSoup
import mysql.connector
import pandas as pd
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def process_noun(session, noun, gender_ls):
    async with session.get(f"https://der-artikel.de/die/{noun}.html") as response:
        if response.status == 404:
            async with session.get(f"https://der-artikel.de/der/{noun}.html") as response:
                if response.status == 404:
                    print(f"{noun} does not exist")
                    return
        html = await response.text()
        soup = BeautifulSoup(html, 'html.parser')
        gender = soup.find("h3", class_="mb-5")
        if gender is not None:
            gender_label = gender.text.split(",")
            gender_ls[noun] = gender_label[1]
            print(f"success {gender_label[1]}")
        else:
            print(f"{noun} does not exist")

async def main(nouns):
    async with aiohttp.ClientSession() as session:
        tasks = []
        gender_ls = {}
        for noun in nouns:
            task = asyncio.ensure_future(process_noun(session, noun, gender_ls))
            tasks.append(task)
        await asyncio.gather(*tasks)
    return gender_ls

if __name__ == "__main__":
    cnx = mysql.connector.connect(user='root', password='258000',
                              host='localhost',
                              database='fazzta')
    df=pd.read_sql("select * from nomen", cnx)
    nouns = df['form']
    loop = asyncio.get_event_loop()
    gender_ls = loop.run_until_complete(main(nouns))
    loop.close()
    print(gender_ls)
