import urllib.error
import pandas as pd
import sqlite3
import os
from threading import Thread
import gspread
import gspread_dataframe as gd
from gspread.exceptions import APIError
from pandas.errors import EmptyDataError


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
    def __init__(self, url: str, database_dir: str):
        self.df = pd.DataFrame()
        self.url = url
        self.dir = database_dir

    def sheet_reader(self):
        """reads g-sheet and return pandas df"""
        try:
            self.df = pd.read_csv(f'https://docs.google.com/spreadsheets/d/{self.url.split("/")[5]}/gviz/tq?tqx=out:csv&sheet=Sheet1')
        except EmptyDataError:
            return {'error': 'Empty Sheet'}
        except urllib.error.HTTPError:
            return {'error': 'Invalid url'}
        return {'message': 'success'}

    def find(self):
        ls = []
        for i in self.df.get('url'):
            result = FindDb(self.dir).find(i)
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


def main():
    url = input('Enter sheet url: ')
    d = input('Drag database folder: ').replace('"', '')

    obj = Finder(url, d)
    obj.sheet_reader()
    obj.find()
    obj.sheet_writer()


if __name__ == '__main__':
    main()

    # token = 'MTA3NjA0MDE2NDk5MzkyOTI5Ng.GwvAu4.WuDVsY9weGr2x-QKezoHFw40LmuskO0xtzE2XI'

