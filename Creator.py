import os
import sys
import time
import sqlite3
from threading import Thread
from tqdm import tqdm


class DbConnector:
    """To be used only by FindDb. Never use this to write data"""

    def __init__(self, database_path: str):
        self.db = sqlite3.connect(database_path, check_same_thread=False)
        self.cursor = self.db.cursor()

    def get(self):
        return self.cursor


class InsertDb:
    """Use only for insert_many"""

    def __init__(self, database_dir: str):
        self.dir = database_dir
        self.db = sqlite3.connect(f'{self.dir}/{self._get_database_name()}')
        self.cursor = self.db.cursor()
        self._create()

    def _get_database_name(self):
        return f'Twitter{len(os.listdir(self.dir)) + 1}.db'

    def _create(self):
        self.cursor.execute(
            'CREATE TABLE IF NOT EXISTS twitter(ScreenName varchar(100) PRIMARY KEY, Name varchar(100), Email NVARCHAR(255), '
            'Followers INT, Created text, url text)')
        self.db.commit()

    def insert_many(self, values):
        query1 = f"""INSERT OR IGNORE into twitter (ScreenName, Name, Email, Followers, Created, url) values (?, ?, ?, ?, ?, ?)"""
        s = time.time()
        self.cursor.executemany(query1, values)
        self.db.commit()
        print(f'query executed...in {round(time.time() - s, 2)}s.')


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


def check_and_tuple(dic: dict):
    ls = []
    keys = ('ScreenName', 'Name', 'Email', 'Followers', 'Created At', 'url')
    dic['url'] = f'https://twitter.com/{dic["ScreenName"]}'
    for key in keys:
        if key not in dic:
            ls.append(None)
        else:
            ls.append(dic[key])
    return tuple(ls)


def get_file_size(file: str):
    with open(file, 'rb') as f:
        c = 0
        for _ in f:
            c += 1
    return c


def main(file: str, error_file: str, rows_to_insert_at_a_time: int, databases_dir: str):
    print('Analysing file...')
    ls = []
    index = 0
    # try:
    #     os.remove(error_file)
    # except FileNotFoundError:
    #     pass

    with open(file, 'rb') as f:
        for i in tqdm(f, desc='Progress:', unit=' lines', total=get_file_size(file), colour='green'):
            index += 1
            i = i.decode('utf-8')
            x = {}
            flag = False
            for j in i.split(' - '):
                j = j.split(':', 1)
                try:
                    x[j[0].strip()] = j[1].strip()
                except Exception:
                    flag = True
                    break

            if len(x) < 4 or flag or 'ScreenName' not in x:
                # with open(error_file, 'ab') as er_file:
                #     er_file.write(i.encode('utf-8'))
                continue

            if index > rows_to_insert_at_a_time:
                db_obj = InsertDb(databases_dir)
                db_obj.insert_many(ls)
                print(f'{index} inserted...')
                ls = []
                index = 0
            else:
                ls.append(check_and_tuple(x))
        if ls:
            db_obj.insert_many(ls)
            print(f'{len(ls)} also inserted...')


if __name__ == '__main__':
    d = input('Drag database folder: ').replace('"', '')
    i = input('Drag data file: ').replace('"', '')
    main(i, 'error-test.txt', 10000000, d)
    input('Press any key to exit. ')
    sys.exit()
    # cursor = DbConnector('Databases/Twitter1.db').get()
    # cursor.execute('select * from twitter where ScreenName="andrew_dreww"')
    # s = time.time()
    # Find
    # a = FindDb('Databases').find('https://twitter.com/andrew_dreww')
    # print(a)
    # print(f'Total time taken: {time.time()-s} s.')
