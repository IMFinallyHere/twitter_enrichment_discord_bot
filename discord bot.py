import sys

import discord as dis
import json
import urllib.error
import pandas as pd
import sqlite3
import os
from threading import Thread
import gspread
import gspread_dataframe as gd
from gspread.exceptions import APIError
from pandas.errors import EmptyDataError

intents = dis.Intents.default()
intents.message_content = True
client = dis.Client(intents=intents)


class DbConnector:
    """To be used only by FindDb. Never use this to write data"""

    def __init__(self, database_path: str):
        self.db = sqlite3.connect(database_path, check_same_thread=False)
        self.cursor = self.db.cursor()

    def get(self):
        return self.cursor


class FindDb:
    """Use to find url from databases"""

    def __init__(self, database_dir: str):
        self.cursors = []
        for i in os.listdir(database_dir):
            self.cursors.append(DbConnector(f'{database_dir}/{i}').get())
        self.result = ''

    def find(self, url: str):
        ls = []
        sn = url.split('/')[-1]
        for cursor in self.cursors:
            ls.append(Thread(target=self._find, args=(cursor, sn)))
        for i in ls:
            i.start()
        for i in ls:
            i.join()

        return self.result

    def _find(self, cursor, screen_name):
        query = f"""select * from twitter where ScreenName='{screen_name}'"""
        cursor.execute(query)
        res = cursor.fetchone()
        if res:
            self.result = res


class Finder:
    def __init__(self, url: str):
        self.df = pd.DataFrame()
        self.url = url

    def sheet_reader(self):
        """reads g-sheet and return pandas df"""
        try:
            self.df = pd.read_csv(f'https://docs.google.com/spreadsheets/d/{self.url.split("/")[5]}/gviz/tq?tqx=out:csv&sheet=Sheet1')
        except EmptyDataError:
            return {'error': 'Empty Sheet'}
        except urllib.error.HTTPError:
            return {'error': 'Invalid url'}
        return {'message': 'success'}

    def find(self, database_dir):
        ls = []
        for i in self.df.get('url'):
            result = FindDb(database_dir).find(i)
            if not result:
                result = ('', '', '', '', '', i)
            ls.append(
                {
                    'url': result[5],
                    'ScreenName': result[0],
                    'Name': result[1],
                    'Email': result[2],
                    'Followers': result[3],
                    'Created': result[4]
                }
            )
        self.df = pd.DataFrame(ls)
        return {'message': 'success'}

    def sheet_writer(self):
        sa = gspread.service_account(filename='service.json')
        sh = sa.open_by_url(self.url).get_worksheet(0)
        try:
            gd.set_with_dataframe(sh, self.df)
        except APIError:
            return {'error': 'access denied'}
        else:
            return {'message': 'success'}


def set_token():
    token = input('Enter your bot token: ')
    with open('token.txt', 'w') as f:
        f.write(token)
    return token


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    username = message.author.name
    msg = message.content
    channel = message.channel

    if msg.lower() == 'hello':
        await channel.send(f'Hello {username}!!')

    # commands
    avail_comm = ['!leads']
    if msg.startswith('!'):
        if msg.startswith('!leads'):
            url = msg.split(' ')[-1]
            if not url.startswith('https://docs.google.com/spreadsheets'):
                await channel.send(f'Invalid format.')
                await channel.send(f'Correct format: !leads https://docs.google.com/spreadsheets....')
            else:
                obj = Finder(url)
                _ = obj.sheet_reader()
                if 'error' in _:
                    await channel.send(_['error'])
                else:
                    await channel.send(f'Preparing leads...')
                    obj.find(d)
                    _ = obj.sheet_writer()
                    if 'error' in _:
                        if _['error'] == 'access denied':
                            with open('service.json') as f:
                                email = json.load(f).get('client_email')
                            await channel.send('Can\'t write to google sheet')
                            await channel.send(f'Make sure to share your google sheet with **{email}**')
                    else:
                        await channel.send(f'Done, Check your file!!')

        else:
            await channel.send(f'Command not found.')
            await channel.send(f'Available command: {avail_comm}')

        await message.channel.send('Command Completed!!')


if __name__ == '__main__':
    try:
        with open('token.txt') as f:
            token = f.readline()
        if not token:
            raise FileNotFoundError
    except FileNotFoundError:
        token = set_token()

    d = input('Drag database folder: ').replace('"', '')

    try:
        a = open('service.json')
    except FileNotFoundError:
        print('service.json not found')
        input()
        sys.exit()
    else:
        client.run(token)



