#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on  2017/7/31 10:49
@author: IMYin
@File: scrapy_from_gd.py
"""

import datetime
import os
import sys
import time
import re
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pymysql.cursors

import Utils as utils
import Constants as cons

LOGGING_PATH = cons.LOGGING_PATH
sys.path.append(LOGGING_PATH)
from JobLogging import JobLogging


class Scarpy_Da_zhong:
    # initial log
    def __init__(self, log_lev='INFO'):
        date_today = datetime.datetime.now().date()
        self.log_name = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
        log_dir = cons.TASK_LOG_PATH
        self.today = date_today.strftime("%Y%m%d")
        # log_dir += '/' + self.today
        if not os.path.isdir(log_dir):
            try:
                os.makedirs(log_dir)
            except:
                pass
        mylog = JobLogging(self.log_name, log_dir)
        mylog.set_level(log_lev)
        self.log = mylog.get_logger()
        self.log.info("Scrapy Da zhong's log create success.")

    def shangqu(self):
        with open('sq.txt', 'r', encoding="utf-8") as lines:
            for item in lines.readlines():
                yield item.split(":")

    # Get information on website
    def connect_internet(self, target_url, retry=3):
        for i in range(retry):
            time.sleep(np.random.rand(1))
            try:
                bsObj = utils.conn_get(target_url)
                if len(bsObj) > 2:
                    return bsObj
                else:
                    pass
            except Exception as e:
                self.log.info(u"Something wrong: {}".format(e))
                self.log.info(u"Will be retried later......")
                pass
        self.log.info(u"There is nothing in {}".format(target_url))

    # Put url together
    def get_page_url(self, bsObj):
        try:
            last_page = (bsObj.find("div", {"class": "page"}).find_all("a", {"class": "PageLink"}))[-1]
            page_num = last_page.text
            url_lst = (last_page.attrs["href"]).split(cons.SPLIT_ITEM7)
            if len(url_lst) == 2:
                if "p" not in url_lst[0][-2:]:
                    for i in range(1, int(page_num)):
                        yield cons.URL_OF_DA_ZHONG + url_lst[0][:-2] + str(i) + cons.SPLIT_ITEM7 + url_lst[-1]
                else:
                    for i in range(1, int(page_num)):
                        yield cons.URL_OF_DA_ZHONG + url_lst[0][:-1] + str(i) + cons.SPLIT_ITEM7 + url_lst[-1]
            else:
                if "p" not in url_lst[0][-2:]:
                    for i in range(1, int(page_num)):
                        yield cons.URL_OF_DA_ZHONG + url_lst[0][:-2] + str(i)
                else:
                    for i in range(1, int(page_num)):
                        yield cons.URL_OF_DA_ZHONG + url_lst[0][:-1] + str(i)
        except Exception as e:
            self.log.info(u"Something wrong: {}".format(e))
            self.log.info(u"No pages...")

    # Two dictionaries in cons.
    def file_url_items(self):
        for category, num in cons.BEIJING_CATEGORY.items():
            for area, code in cons.BEIJING_COMMERCIAL_AREA.items():
                yield category, num, area, code

    # Get information on website.
    def info(self, bsObj):
        information = {u'name': [], u'price': [], u'star': [], u'comment': [],
                       u'tag': [], u"domain": [], u"address": [], u"taste": [],
                       u"env": [], u"service": [], u"summary": []}
        try:
            content = bsObj.find("div",
                                 {"class": re.compile("^.*shop-all-list$")}).find_all("div", {"class": "txt"})
            for mess in content:
                try:
                    price = mess.find("a", {"class": "mean-price"}).text
                    price = re.sub("\n+", "", price)
                    price = re.sub(" +", "", price)
                    information[u'price'].append(price)
                    information[u'domain'].append(mess.find_all("span", {"class": "tag"})[1].text)
                    store_name = mess.find("h4").text
                    information[u'name'].append(store_name)
                    information[u'tag'].append(mess.find_all("span", {"class": "tag"})[0].text)
                    information[u'star'].append(mess.find("span", {"class": re.compile("^.*stars.*")}).attrs["title"])
                    information[u"address"].append(mess.find("span", {"class": "addr"}).text)
                    try:
                        comment = mess.find("a", {"module": "list-readreview"}).text
                        comment = re.sub("\n+", "", comment)
                        comment = re.sub(" +", "", comment)
                        information[u'comment'].append(comment)
                    except Exception as e:
                        information[u'comment'].append(None)
                    try:
                        comment_p = mess.find("span", {"class": "comment-list"}).find_all("b")
                        taste = comment_p[0].text
                        env = comment_p[1].text
                        service = comment_p[2].text
                        # add to dict
                        information[u"taste"].append(taste)
                        information[u"env"].append(env)
                        information[u"service"].append(service)
                        information[u"summary"].append((float(taste) + float(env) + float(service)) / 3)
                    except Exception as e:
                        information[u"taste"].append(None)
                        information[u"env"].append(None)
                        information[u"service"].append(None)
                        information[u"summary"].append(None)
                    self.log.info("Get information of {}".format(store_name))
                except Exception as e:
                    pass
        except Exception as e:
            pass
        return pd.DataFrame(information,
                            columns=[u'name', u'price', u'star', u'comment',
                                     u'tag', u'domain', u'address', u'taste',
                                     u'env', u'service', u'summary'])

    # Merge DataFrame
    def merge_df(self, df):
        new_df = pd.DataFrame()
        new_df = new_df.append(df, ignore_index=True)
        return new_df

    # Write content to file.csv
    def write_to_file(self, df, commercial_area, category):
        commercial_area = re.sub("/", "_", commercial_area)
        archive_path = cons.BEIJING_ALL_DATA + "/" + category
        if not os.path.isdir(archive_path):
            try:
                os.makedirs(archive_path)
            except:
                pass
        data_path = os.path.join(archive_path, commercial_area + ".csv")
        df.drop_duplicates(inplace=True)
        df.to_csv(data_path)
        self.log.info(u"Save data to {} successful.".format(data_path))

    # Use mysql to store information of stock which is crawled by some websites.
    def insert_to_table(self, df):
        connection = pymysql.connect(host=cons.mysql_host,
                                     user=cons.mysql_user,
                                     password=cons.mysql_passwd,
                                     db=cons.stock_db,
                                     charset='utf8',  # set the mysql character is utf-8 !!!
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                for index, row in df.iterrows():
                    sql = cons.insert_announ_table_sql.format(cons.announ_table_name)
                    cursor.execute(sql, (
                        row[u'publishtime'], row[u'code'], row[u'title'], row[u'pdfurl']))
                    self.log.info(
                        u"Got the '{}, {}, {}, {}' into table: {}".format(row[u'publishtime'], row[u'code'],
                                                                          row[u'title'].decode('utf-8'),
                                                                          row[u'pdfurl'], cons.announ_table_name))
            connection.commit()
            self.log.info(u"Great job, you got {} rows information　today.".format(len(df)))
        finally:
            connection.close()


if __name__ == '__main__':
    URL = cons.URL_OF_DA_ZHONG + cons.DA_ZHONG_OF_BEEJING
    URL_Net = urlparse(URL).netloc
    URL_SCHEME = urlparse(URL).scheme
    time1 = datetime.datetime.now()
    run = Scarpy_Da_zhong()
    all_nums = len(cons.BEIJING_COMMERCIAL_AREA) * len(cons.BEIJING_CATEGORY)
    run.log.info("There are {} files to write..".format(all_nums))
    for u_tuple in run.file_url_items():
        url_whole = URL + u_tuple[1] + "/" + u_tuple[-1]
        df_result = pd.DataFrame()
        for page in run.get_page_url(run.connect_internet(url_whole, retry=10)):
            df_result = df_result.append(run.info(run.connect_internet(page, retry=5)))
        if len(df_result) != 0:
            run.write_to_file(df_result, commercial_area=u_tuple[2], category=u_tuple[0])
        else:
            run.log.info(u"There is no information in {}".format(u_tuple[2]))
        all_nums -= 1
        run.log.info("Tere are {} files to write...".format(all_nums))
    time2 = datetime.datetime.now()
    run.log.info(u"It costs {} sec to run it.".format((time2 - time1).total_seconds()))
    run.log.info(u"-" * 100)
