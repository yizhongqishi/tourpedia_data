# -*- coding:utf-8 -*-
# created by CYMX on 01/03/2020

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from urllib import parse, request
import os
import threading


def read_csv(filename):
    filepath = './csv/' + filename
    result = pd.DataFrame(columns=('id', 'text'))
    df = pd.read_csv(filepath, error_bad_lines=False, low_memory=False)
    for index, row in df.iterrows():
        print(filename + ': No.' + str(index))
        url = row[row.last_valid_index()]
        id = url.split('=')[1]
        text = get_text(url)
        dic = {'text': text}
        temp = pd.DataFrame(dic)
        for i in temp.index:
            temp.loc[i, 'id'] = id
        result = result.append(temp, sort=True)
    result.to_csv('./text_output/text-'+filename, index=False)


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


def get_csv_threading():
    if not os.path.exists('./csv'):
        os.mkdir(os.path.join(os.getcwd(), 'csv'))
    url_table = get_csv_url()
    # download csv files about place
    files = []
    for url in url_table:
        filepath = './csv/' + os.path.basename(url)
        files.append(filepath)
        thread = threading.Thread(target=get_csv, args=(url, filepath,))
        thread.start()


def get_result():
    if not os.path.exists('./text_output'):
        os.mkdir(os.path.join(os.getcwd(), 'text_output'))
    files = os.listdir('./csv')
    for filename in files:
        # 多线程
        thread = threading.Thread(target=read_csv, args=(filename,))
        thread.start()
        # 单线程
        # read_csv(filename)


if __name__ == "__main__":
    # 下载地点的csv文件，保存在csv文件夹中
    # get_csv_threading()
    # 下载review的text，保存在text_output中
    get_result()


    # 原始csv不干净，用来检错来着
    # df = pd.read_csv('./csv/barcelona-restaurant.csv', error_bad_lines=False)
    # files = os.listdir('./csv')
    # for filename in files:
    #     filepath = './csv/' + filename
    #     result = pd.DataFrame(columns=('id', 'text'))
    #     print(filename)
    #     df = pd.read_csv(filepath, error_bad_lines=False)
