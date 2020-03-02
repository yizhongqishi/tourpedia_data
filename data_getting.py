# -*- coding:utf-8 -*-
# created by CYMX on 01/03/2020

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from urllib import parse, request
import os
import numpy as np
import threading


def read_csv(filepath):
    result = pd.DataFrame(columns=('id', 'text'))
    df = pd.read_csv(filepath)
    for index, row in df.iterrows():
        print(index)
        url = row[row.last_valid_index()]
        id = url.split('=')[1]
        text = get_text(url)
        dic = {'text': text}
        temp = pd.DataFrame(dic)
        for index in temp.index:
            temp.loc[index, 'id'] = id
        result = result.append(temp, sort=True)
    result.to_csv('./test.csv')


def get_text(url):
    # need to save the text
    result = []
    try:
        data = requests.get(url=url).json()
        if isinstance(data, list):
            for item in data:
                # get text
                result.append(item['text'])
        else:
            # get text
            result.append(data['text'])
        return result
    except Exception as ex:
        return ex


def get_csv_url(url='http://tour-pedia.org/about/datasets.html'):
    page = requests.get(url).text
    dom = bs(page, 'html.parser')
    table = dom.find_all('table')
    table = table[-1]
    trs = table.find_all('tr')[1: -1]
    result = []
    for tr in trs:
        tds = tr.find_all('td')[1: -1]
        for td in tds:
            result.append(parse.urljoin('http://tour-pedia.org/about', td.a['href']))
    return result


def get_csv(url=None, filename=None):
    request.urlretrieve(url=url, filename=filename)


if __name__ == "__main__":
    # t = get_text('http://tour-pedia.org/api/getReviewsByPlaceId?placeId=114593')

    # if not os.path.exists('./csv'):
    #     os.mkdir(os.path.join(os.getcwd(), 'csv'))
    # url_table = get_csv_url()
    # threads = []
    # files = []
    # # download csv files about place
    # for url in url_table:
    #     filepath = './csv/' + os.path.basename(url)
    #     files.append(filepath)
    #     thread = threading.Thread(target=get_csv, args=(url, filepath,))
    #     thread.start()

    # download text from csv files
    files = os.listdir('./csv')
    filepath = files[0]
    read_csv('./csv/' + filepath)

    # for filepath in files:
    #     read_csv(filepath)