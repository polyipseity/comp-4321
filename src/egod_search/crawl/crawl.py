starting_page = 'http://www.cse.ust.hk'
number_of_pages = 30
database_path = 'crawled.json'

import httplib2, json, pandas as pd, numpy as np, re
from bs4 import BeautifulSoup, SoupStrainer
from queue import SimpleQueue
from typing import NewType, Dict, TypedDict
from datetime import datetime
from dateutil.parser import parse as parsedate
from itertools import islice
WordId = NewType('WordId', int)
PageId = NewType('PageId', int)
Word = NewType('Word', str)
Url = NewType('Url', str)
class Page(TypedDict):
    title: str
    url: Url
    links: list[Url]
    modified: datetime
    text: str

http = httplib2.Http('.cache')
pages_to_index = SimpleQueue[Url]()
pages_to_index.put(starting_page)
index = []

with open('database.json', 'r+') as database_file:
    database = json.load(database_file)
    word_id_to_word: Dict[WordId, Word] = database.get('word_id_to_word', {})
    word_to_word_id: Dict[Word, WordId] = database.get('word_to_word_id', {})
    url_to_page_id: Dict[Url, PageId] = database.get('url_to_page_id', {})
    page_id_to_url: Dict[PageId, Url] = database.get('page_id_to_url', {})
    forward_index_frequency: Dict[PageId, Dict[WordId, int]] = database.get('forward_index_frequency', {})
    inverted_index_position: Dict[WordId, Dict[PageId, list[int]]] = database.get('inverted_index_position', {})
    pages: Dict[PageId, Page] = database.get('pages', {})
    try:
        pages_indexed = 0
        while pages_indexed < number_of_pages:
            # Get page ID
            url = pages_to_index.get(block = False)
            page_id = url_to_page_id.get(url, PageId(len(url_to_page_id)))
            if page_id == len(url_to_page_id):
                page_id_to_url[page_id] = url
                pages[page_id] = { 'url': url, 'links': [], 'modified': datetime.min }
            # Open the page
            response, html_text = http.request(url)
            if response.status == 200:
                last_modified = response['last-modified']
                if last_modified is None or parsedate(last_modified) > pages[page_id].modified:
                    html = BeautifulSoup(html_text, 'html.parser')
                    pages[page_id].modified = parsedate(last_modified)
                    pages[page_id].title = html.title.string
                    pages[page_id].links.clear()
                    pages[page_id].text = html.text

                    forward_index_this_page = forward_index_frequency.get(page_id, {})
                    # Get words for indexing
                    for match in re.finditer(r'\S+', pages[page_id].text):
                        position, word = match.start(), Word(match.group())

                        word_id = word_to_word_id.get(word, WordId(len(word_to_word_id)))
                        if word_id == len(word_to_word_id): word_id_to_word[word_id] = word

                        forward_index_this_page[word_id] = forward_index_this_page.get(word_id, 0) + 1
                        inverted_index_position.get(word_id, {}).get(page_id, []).append(position)

                    # Append outward links for breadth first search
                    for link in BeautifulSoup(html_text, parse_only=SoupStrainer('a')):
                        if link.has_attr('href'):
                            pages[page_id].links.append(link['href'])
                            pages_to_index.put(link['href'])
                pages_indexed = pages_indexed + 1
    finally:
        json.dump({
            word_id_to_word: word_id_to_word,
            word_to_word_id: word_to_word_id,
            url_to_page_id: url_to_page_id,
            page_id_to_url: page_id_to_url,
            forward_index_frequency: forward_index_frequency,
            inverted_index_position: inverted_index_position,
            pages: pages
        }, database_file)


with open('database.json', 'r') as database_file:
    with open('spider_result.txt', 'w') as result_file:   
        database = json.load(database_file)
        for page_id, page in database['pages'].iteritems():
            result_file.write(page.title)
            result_file.write('\n')
            result_file.write(page.url)
            result_file.write('\n')
            result_file.write(page.modified)
            result_file.write(', ')
            result_file.write(len(page.text))
            result_file.write('\n')
            for word_id, frequency in islice(database['forward_index_frequency'][page_id].iteritems(), 10):
                result_file.write(database['word_id_to_word'][word_id])
                result_file.write(' ')
                result_file.write(frequency)
                result_file.write('; ')
            result_file.write('\n')
            for link in islice(page.links, 10):
                result_file.write(link)
                result_file.write('\n')
            result_file.write('\n')