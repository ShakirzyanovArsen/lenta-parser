#!/usr/bin/python
import requests
from bs4 import BeautifulSoup
import datetime
from config import config
import psycopg2
import locale
import re


base_url = 'https://lenta.ru'
locale.setlocale(locale.LC_TIME, "ru_RU.utf8")

database_connect = config()
connection = psycopg2.connect(**database_connect)
connection.autocommit = True


def parse_news_page(news_url):
    url = base_url + news_url
    if news_exists(news_url):
        return

    resp = requests.get(url)
    news_soup = BeautifulSoup(resp.text, "lxml")

    posted_at = news_soup.select('.b-topic__info .g-date')
    posted_at = posted_at[0].text if len(posted_at) > 0 else None
    if posted_at != None:
        splitted_posted_at = posted_at.split(' ')
        posted_at = ''
        for part in splitted_posted_at:
            if re.match('^[а-яё]+$', part):
                part = part[:3]
            posted_at = posted_at + ' ' + part
        posted_at = posted_at.strip()
        posted_at = datetime.datetime.strptime(posted_at, '%H:%M, %d %b %Y')

    title = news_soup.select('.b-topic__title')
    title = title[0].text if len(title) > 0 else ''

    topic_rightcol = news_soup.select('.b-topic__info .b-topic__rightcol')
    topic_rightcol = topic_rightcol[0].text if len(topic_rightcol) > 0 else ''

    content = news_soup.select('.b-topic__content .b-text')
    content = content[0].text if len(content) > 0 else ''
    content = re.sub('\nМатериалы по теме.+\n', '', content)

    news = {'url': news_url, 'posted_at': posted_at, 'title': title, 'topic_rightcol': topic_rightcol, 'content': content}
    add_news_to_db(news)


def add_news_to_db(news):
    cursor = connection.cursor()
    cursor.execute("INSERT INTO news (url, posted_at, title, topic_rightcol, content) VALUES (%s, %s, %s, %s, %s)",
                   (news['url'], news['posted_at'], news['title'], news['topic_rightcol'], news['content']))


def news_exists(url):
    cursor = connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM news WHERE url = %s)", (url,))
    return cursor.fetchone()[0]

last_date = datetime.datetime.now()
curr_date = last_date

while (curr_date-last_date).days < 365:
    archive_url = base_url + '/' + '/'.join([str(last_date.year), str(last_date.month), str(last_date.day)])
    resp = requests.get(archive_url)
    archive_soup = BeautifulSoup(resp.text, "lxml")
    archive_items = archive_soup.select('div.item div.titles h3 a[href]')
    for item in archive_items:
        parse_news_page(item['href'])
    last_date = last_date - datetime.timedelta(1)