import sqlite3
import csv
import time
import wikipediaapi
import wikipedia
import pandas as pd
import numpy as np

from os.path import dirname, abspath

parent_path = dirname(dirname(abspath(__file__)))


class SQLite:

    def __init__(self, db_path):
        self.conn = self._create_connection(db_path)

    @staticmethod
    def _create_connection(db_path):
        """ create a database connection to the SQLite database specified by db_file
        :param db_path: database file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(db_path)
            return conn
        except sqlite3.Error as e:
            print("[sqlite-log]: " + str(e))

        return None

    def insert_wiki_page(self, page_id, title, context, categories):
        """ insert a new wikipedia page
            :param page_id: id of wikipedia page
            :param title: title of the page
            :param content: text of the page
            :param categories: list of page's categories
            """
        sql = ''' INSERT INTO pages(id, title, context, category) VALUES(?, ?, ?, ?) '''
        cur = self.conn.cursor()
        cur.execute(sql, (page_id, title, context, categories))
        self.conn.commit()

    def execute_query(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        return rows


class WikiAggregator:
    def __init__(self):
        self.titles_path = parent_path + "/data/wikipedia/enwiki-latest-all-titles-in-ns0"
        self.db_path = parent_path + "/data/wikipedia/wiki.db"
        self.table_exist_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='pages';"
        self.create_table_query = """CREATE TABLE pages (
                                                id       STRING UNIQUE, 
                                                title    TEXT, 
                                                context  TEXT, 
                                                category TEXT);"""

        # check if 'pages' table exists
        db = SQLite(self.db_path)
        if len(db.execute_query(self.table_exist_query)) == 0:
            db.execute_query(self.create_table_query)

    @staticmethod
    def get_categories():
        """
        getting major categories in wikipedia's hierarchy
        :return:
        """
        return ['Culture', 'Geography', 'Health',
                'History', 'Mathematics', 'People',
                'Philosophy', 'Religions', 'Society', 'Technology']

    def create_wiki_db_v1(self, sleep_step=2000, sleep_time=65):
        """
        creating wikipedia pages database using the titles list and wikipedia python api
        api: https://pypi.org/project/wikipedia/
        :return:
        """
        success = 0
        # reading list of titles
        with open(self.titles_path) as f:
            titles = f.readlines()

        # inserting pages info into pages table
        db = SQLite(self.db_path)
        for title in titles:
            try:
                title = title.replace('\n', '')
                page = wikipedia.WikipediaPage(title)
                db.insert_wiki_page(page.pageid, title, page.content, str(page.categories))
                success += 1
            except Exception as e:
                print("[aggregator-log] {}".format(e))
            if success > 0 and success % sleep_step == 0:
                time.sleep(sleep_time)

    def create_wiki_db_v2(self, sleep_step=2000, sleep_time=65):
        """
        creating wikipedia pages table using wikipedia-api
        api: https://pypi.org/project/Wikipedia-API/
        :return:
        """

        def get_categories(page):
            cats = []
            categories = page.categories
            for category in categories:
                cats.append(category.replace("Category:", ''))
            return cats

        success = 0
        # reading list of titles
        with open(self.titles_path) as f:
            titles = f.readlines()

        wiki = wikipediaapi.Wikipedia('en')

        # inserting pages info into pages table
        db = SQLite(self.db_path)

        # creating dictionary of existing titles to avoid downloading existing pages
        db_titles = {}
        title_rows = db.execute_query("SELECT title FROM pages")
        for row in title_rows:
            db_titles[row[0]] = 1

        for title in titles:
            # check if we have already extracted title info
            title = title.replace('\n', '')
            if title not in db_titles.keys():
                try:
                    page = wiki.page("" + title + "")
                    if page.title.strip() != "" and page.text.strip() != "":
                        db.insert_wiki_page(page.pageid, page.title, page.text, str(get_categories(page)))
                        success += 1
                except Exception as e:
                    print("[aggregator-log] {}".format(e))
                if success > 0 and success % sleep_step == 0:
                    time.sleep(sleep_time)

    def create_wiki_db_v3(self):
        """
        writing Wikipedia articles into a TSV file--which is the input format for snorkel
        :return:
        """

        with open(parent_path + "/data/wikipedia/wiki.txt") as wiki_file:
            current_page = []
            counter = 0
            db = SQLite(self.db_path)
            with open(parent_path + '/data/wiki.tsv', 'wt') as out_file:
                tsv_writer = csv.writer(out_file, delimiter='\t')
                for line in wiki_file:
                    if line == "\n" and len(current_page) != 0:
                        title = current_page[0].replace('\n', '').replace('\t', '').replace(':', '')
                        url = current_page[1].replace('\n', '').replace('\t', '')
                        page_id = current_page[2].replace('\n', '').replace('\t', '')
                        context = ' '.join(current_page[3:]).replace('\n', '')
                        if title != "" and context != "" and url != "" and page_id != "":
                            tsv_writer.writerow([page_id, title, url, context])
                            # uncomment if we want to write articles in the SQLite db
                            db.insert_wiki_page(page_id, title, context, '')
                        current_page = []
                        counter += 1
                    else:
                        if line != "\n":
                            current_page.append(line)

    def read_pages_table(self, n_category_samples=30):
        # ========================================================
        # randomly selecting pages from wikipedia categories
        db = SQLite(self.db_path)
        categories = self.get_categories()
        page_ids = []
        for category in categories:
            ids_ = db.db_select("SELECT id FROM pages WHERE category LIKE '%" + category + "%'")
            ids = [_id[0] for _id in ids_]
            if len(ids) > n_category_samples:
                ids_sample = np.random.choice(ids, size=n_category_samples, replace=False)
                count = 0
                for i in range(len(ids_sample)):
                    if ids_sample[i] not in page_ids:
                        page_ids.append(ids_sample[i])
                        count += 1
                        if count == n_category_samples:
                            break
                print("category: " + category + ", count: " + str(count))

        # TODO [0] we might need another step to filter out those pages we know not going to
        # give us much info, this pre processing will be based on 'text' field

        columns = ['id', 'arg1', 'arg2', 'text', 'label']
        q = "SELECT id, title, context, category FROM pages WHERE id IN (" + ", ".join(
            [str(_id) for _id in page_ids]).strip(", ") + ")"
        data = db.execute_query(q)
        data_df = pd.DataFrame(columns=columns)
        for item in data:
            data_df = data_df.append({'id': item[0], 'arg1': '', 'arg2': '', 'text': item[2], 'label': ''},
                                     ignore_index=True)

        return data_df
