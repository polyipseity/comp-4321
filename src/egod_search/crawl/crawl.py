starting_page = 'http://www.cse.ust.hk'
number_of_pages = 50
http_cache_path = '.cache'
database_path = 'crawled.json'
result_path = 'spider_result.txt'

import httplib2, json, re
from bs4 import BeautifulSoup, SoupStrainer
from queue import SimpleQueue
from typing import NewType, Dict, TypedDict
from datetime import datetime
from dateutil.parser import parse as parsedate
from itertools import islice
from os.path import isfile
from urllib.parse import urljoin
WordId = NewType('WordId', int)
PageId = NewType('PageId', int)
Word = NewType('Word', str)
Url = NewType('Url', str)
class Page(TypedDict):
    title: str
    url: Url
    links: list[Url]
    last_modified: str
    text: str

http = httplib2.Http(http_cache_path)
pages_to_index = SimpleQueue[Url]()
pages_to_index.put(starting_page)
index = []

if not isfile(database_path):
    with open(database_path, 'w') as database_file:
        database_file.write('{}')
with open(database_path, 'r+') as database_file:
    database = json.load(database_file)
    word_id_to_word: list[Word] = database.get('word_id_to_word', [])
    word_to_word_id: Dict[Word, WordId] = database.get('word_to_word_id', {})
    url_to_page_id: Dict[Url, PageId] = database.get('url_to_page_id', {})
    page_id_to_url: list[Url] = database.get('page_id_to_url', [])
    forward_index_frequency: Dict[PageId, Dict[WordId, int]] = database.get('forward_index_frequency', {})
    inverted_index_position: Dict[WordId, Dict[PageId, list[int]]] = database.get('inverted_index_position', {})
    pages: list[Page] = database.get('pages', [])
    try:
        pages_indexed = 0
        while pages_indexed < number_of_pages:
            # Get page ID
            url = pages_to_index.get(block = False)
            page_id = url_to_page_id.get(url, PageId(len(url_to_page_id)))
            if page_id == len(url_to_page_id):
                url_to_page_id[url] = page_id
                page_id_to_url.append(url)
                pages.append({ 'url': url, 'links': [], 'last_modified': "0001-01-01T00:00:00+00:00" })
            # Open the page
            response, html_text = http.request(url)
            if response.status == 200:
                last_modified = response.get('last-modified', None)
                if last_modified is None or parsedate(last_modified) > parsedate(pages[page_id]['last_modified']):
                    html = BeautifulSoup(html_text, 'html.parser')
                    pages[page_id]['last_modified'] = last_modified
                    pages[page_id]['title'] = html.title.string
                    pages[page_id]['links'].clear()
                    pages[page_id]['text'] = html.text

                    forward_index_this_page = forward_index_frequency.get(page_id, {})
                    forward_index_frequency[page_id] = forward_index_this_page
                    # Get words for indexing
                    for match in re.finditer(r'[a-zA-Z0-9\-_]+', pages[page_id]['text']):
                        position, word = match.start(), Word(match.group())

                        word_id = word_to_word_id.get(word, WordId(len(word_to_word_id)))
                        if word_id == len(word_to_word_id):
                            word_to_word_id[word] = word_id
                            word_id_to_word.append(word)

                        forward_index_this_page[word_id] = forward_index_this_page.get(word_id, 0) + 1
                        inverted_index_this_word = inverted_index_position.get(word_id, {})
                        inverted_index_position[word_id] = inverted_index_this_word
                        inverted_index_this_word_this_page = inverted_index_this_word.get(page_id, [])
                        inverted_index_this_word_this_page.append(position)
                        inverted_index_this_word[page_id] = inverted_index_this_word_this_page

                    # Append outward links for breadth first search
                    for link in BeautifulSoup(html_text, 'html.parser', parse_only=SoupStrainer('a')):
                        if link.has_attr('href'):
                            href = urljoin(url, link['href'])
                            pages[page_id]['links'].append(href)
                            pages_to_index.put(href)
                pages_indexed = pages_indexed + 1
    finally:
        database_file.seek(0)
        json.dump({
            'word_id_to_word': word_id_to_word,
            'word_to_word_id': word_to_word_id,
            'url_to_page_id': url_to_page_id,
            'page_id_to_url': page_id_to_url,
            'forward_index_frequency': forward_index_frequency,
            'inverted_index_position': inverted_index_position,
            'pages': pages
        }, database_file)


with open(database_path, 'r') as database_file:
    with open(result_path, 'w') as result_file:   
        database = json.load(database_file)
        for page_id in range(len(database['pages'])):
            page = database['pages'][page_id]
            if 'text' in page: # Only display pages that got us a 200 response
                result_file.write(page['title'] if page['title'] else '<No Title>')
                result_file.write('\n')
                result_file.write(page['url'])
                result_file.write('\n')
                result_file.write(page['last_modified'] if page['last_modified'] else '<No last-modified>')
                result_file.write(', ')
                result_file.write(str(len(page['text'])) if page['text'] else '<No text>')
                result_file.write('\n')
                for word_id, frequency in islice(database['forward_index_frequency'][str(page_id)].items(), 10):
                    result_file.write(database['word_id_to_word'][int(word_id)])
                    result_file.write(' ')
                    result_file.write(str(frequency))
                    result_file.write('; ')
                result_file.write('\n')
                for link in islice(page['links'], 10):
                    result_file.write(link)
                    result_file.write('\n')
                result_file.write('\n')