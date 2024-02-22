number_of_pages = 30

from urllib.request import urlopen
from bs4 import BeautifulSoup
import dbm
import httplib2
from queue import SimpleQueue
import pandas as pd
import numpy as np

http = httplib2.Http('.cache')
pages_to_index = SimpleQueue()
pages_to_index.put('http://www.cse.ust.hk')

for i in range(number_of_pages):
    # Open the page
    response, html_text = http.request(pages_to_index.get(block = False))
    assert response.status == 200
    html = BeautifulSoup(html_text, 'html.parser')

    # Get words for indexing
    unique, counts = np.unique(np.array(' '.split(html.text)), return_counts=True)
    # TODO: insert these into dbm

    # Append outward links for breadth first search
    for link in BeautifulSoup(html_text, parse_only=SoupStrainer('a')):
        if link.has_attr('href'):
            pages_to_index.put(link['href'])