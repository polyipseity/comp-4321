# Overall design of the System

We separated the system into different modules: crawl containing the crawler, database containing the database models and database saver, index containing the indexer that processes raw web pages into word occurrences and frequencies, query containing the query parser when retrieving, res containing the stop-words and words.txt for Porter's algorithm testing, retrieve containing the retrieval algorithm and web for the web interface. 

The program is written in Python and has two entry points, for the crawler and the web interface respectively. The crawler entry point runs the crawler and the indexer, saving to the database with a summary file. The web interface entry point provides a web interface for querying the database through the query parser and retrieval algorithm.

# File structures used in index database

![database.svg](attachments/database.svg)

## General Principles

For precise communication, we would need consistent names. We used these two names consistently:

* `id` indicates the integer ID of an item. It is the primary key of each table so cannot be null.
* `content` is the item

For example, in the URL-to-ID table, `id` is the URL ID, and `content` is the URL.

## URL-to-ID table `url`

Field name | Type | Can be null? | Indexed for efficient search? | Description
-----------|------|--------------|----------|-------------
id      | Integer (Primary key) | No | Yes (Automatic for primary key, for searching URL) | URL ID of each unique URL
content    | Text | No | Yes (for searching URL ID) | Unique URL
redirect_id | Integer (Foreign key - another URL ID) | Yes (Default value) | No | the URL redirected to by this page (unused for now)

## Word-to-ID table `word`

Field name | Type | Can be null? | Indexed for efficient search? | Description
-----------|------|--------------|----------|-------------
id      | Integer (Primary key) | No | Yes (Automatic for primary key, for searching Word) | Word ID of each unique Word
content    | Text | No | Yes (for searching Word ID) | Unique Word

## Page information table `page`

Field name | Type | Can be null? | Indexed for efficient search? | Description
-----------|------|--------------|----------|-------------
id      | Integer (Primary key) | No | Yes (Automatic for primary key - for searching page information) | URL ID of the page
mod_time   | Integer | No | No | UNIX timestamp in seconds: Last modified time in HTTP header if available, or the time of scraping otherwise.
size   | Integer | No | No | Size in bytes: Size of page in HTTP header if available, or the size of plain text (without HTML tags) otherwise. Validated on the Python side to be at least 0.
text   | Text | No | No | Complete page with HTML tags
plaintext   | Text | No | No | Plain human-readable text without HTML tags
title   | Text | No | No | Title of the page
url_id | Integer (Foreign key - URL ID) | No | No | The URL ID associated with this page. Each page must have an associated URL but a URL might not have an associated Page.

## Page-word combination table `pageword`

The Tortoise library does not support user-defined composite primary keys. As a result, we need to work around with a new table that provides a new set of singular keys for later use by the word occurrences for each page table.

Field name | Type | Can be null? | Indexed for efficient search? | Description
-----------|------|--------------|----------|-------------
id         | Integer (Primary key) | No | Yes (Automatic for primary key) | ID of the Page-word combination
page_id    | Integer (Foreign key - Page ID) | No | No | Page ID of the Page-word combination
word_id    | Integer (Foreign key - Word ID) | No | No | Word ID of the Page-word combination

## Word occurrences for each page `wordpositions` & `wordpositionstitle`

`wordpositionstitle` is used for storing word occurrences in the <title> tag of the Page.
`wordpositions` is used for storing word occurrences outside of the <title> tag of the Page.

Field name | Type | Can be null? | Indexed for efficient search? | Description
-----------|------|--------------|----------|-------------
id      | Integer (Primary key) | No | Yes (Automatic for primary key) | ID of the word occurrence.
word_id   | Integer (Composite Primary key and Foreign key - Word ID) | No | Yes (Automatic for primary key - for searching positions and frequencies) | Word ID of the word
positions   | Text | No | No | List of unique word positions. Validated on the Python side to be comma-separated and nonnegative.
frequency   | Integer | No | No | Computed frequency of the word in question in the page. Since lookup occurs much more frequently than scraping, this enables faster lookup, trading for a slower scraping. Validated on the Python side to be at least 1.
tf_normalized | Real number | No | No | Normalized term frequency which is term frequency over the maximum term frequency in the web page. Pre-computed from the frequency column for faster retrieval. Validated on the Python side to be at least 0 and at most 1.
key_id      | Integer (Foreign key - page-word combination ID) | No | No | ID of the page-word combination associated with this word occurrence.

## Outlinks table `page_url`

This table is generated from the many-to-many outlinks relation between Page and Url.

Field name | Type | Can be null? | Indexed for efficient search? | Description
-----------|------|--------------|----------|-------------
page_id     | Integer (Composite primary key, Foreign key - Page ID) | No | Yes (Automatic for primary key) | Page ID where this outlink points from.
url_id      | Integer (Composite primary key, Foreign key - Url ID) | No | Yes (Automatic for primary key) | Url ID where this outlink points to.

# Algorithms used

## Crawler - Breadth First Search
The first part of the search engine is the crawler. Upon initiation of the crawler, a `ConcurrentCrawler` defined in `src/egod_search/crawl/concurrency.py` is created. When `show_progress` is `true` (no `--no-progress`): If `summary_path` (`-s` argument) is provided, then two progress bars for crawling and summary writing are presented; otherwise one progress bar for crawling is shown. This is defined in `src/egod_search/crawl/main.py`. 

When each page is crawled, the `Crawler.crawl` method in `src/egod_search/crawl/__init__.py` is called. The HTTP response and content type of the page are validated, then we detect the character set to guard against non-UTF8 web pages. All `<a>` HTML tags that contain `http://` or `https://` outlinks are then extracted. 

The main algorithm for deciding which pages to crawl is the Breadth First Search in `ConcurrentCrawler.run` of `src/egod_search/crawl/concurrency.py`. From the first requested page, we enqueue all outlinks, then crawl each dequeued page, with all outlinks enqueued. This is done until `page_count` (`-n` argument) is reached. Each crawled page is stored as in-memory objects of class `UnindexedPage` defined at `src/egod_search/index/__init__.py` and saved to the database sequentially with locking since SQLite does not support concurrent writing. Most of the code in the crawler relate to concurrency to speed up crawling. 

## Indexer - Text transformation and collection of word occurrences

The indexer is a converter from `UnindexedPage` to `IndexedPage`, implemented as the `index_page` function of `src/egod_search/index/__init__.py`. First, we extract the `<title>` tag and the page size from the `Content-Length` attribute from the HTTP response.

Then, the text undergoes transformation with the following steps:
1. Tokenize with `TreebankWordTokenizer` from the `nltk.tokenize` module.
2. Normalize the word into Unicode Normalization Compatibility Form D (NFKD). This is for removing diacritics in the next step. Also, very similar looking characters are converted into the normal characters, such as `𝐀` to `A`.
3. Remove non-alphanumeric characters. This also removes diacritics.
4. Normalize the word into Unicode Normalization Compatibility Form C (NFKC).  This merges decomposed characters back into their normal form.
5. Convert to lowercase.
6. Remove stop-words defined in `src/egod_search/res/stop words.txt`.
7. Stem according to Porter's stemming algorithm. 
8. Remove empty words after stemming.

After that, the word occurrences are collected to derive the term frequency of each word and the normalized term frequency from dividing it by the maximum term frequency for later retrieval. 

Finally, the word occurrence, frequency and normalized frequency information are stored. 

## Retrieval function - Word embedding and cosine similarity

When a query is submitted for searching, it is first lexed for separating terms (outside of double quotes) and phrases (inside double quotes) and parsed into a list of terms and a list of phrases. 

For all 3 embedding models (TFxIDF, TFxIDF with title weighted 3.9 times more, vector space model), the terms are converted into word embeddings by following steps 2 to 7 as mentioned in the indexer part then looking up using the stemmed terms. If there are stemmed terms, we exclude any page not containing stemmed terms in content or title. Then, we also exclude any page not containing all phrases in content or title. 

Finally, the term frequency and inverse document frequencies are calculated for cosine similarity ranking. 

## Web Interface - NiceGUI

The web interface is based on the NiceGUI library which provides easy definitions of controls for a nice interface. When the GUI application starts, `layout` of `src/egod_search/web/main.py` is called. There are 3 pages in the left drawer: Home, Search, Debug. The Home page lists usage instructions.

The Search page is the main function - a search bar and a Submit button for querying the search engine. 3 additional buttons provide the calculations used for retrieving results: TFxIDF/max(TF), TFxIDF/max(TF) (title) and Vector space for the use of 3 different page embedding models. The user can then view the calculation details for each result.

The Debug page accepts Python code and outputs its result for debug use. 

# Installation procedure

## Step 1

Set up a Python environment: Ensure that you have at least Python >= 3.11 installed on your system. You can download the latest version of Python from the official Python website (<https://www.python.org>) and follow the installation instructions for your operating system.

_Note: For Windows, you may want to install the Python launcher, enabling you to use `py` in place of `python` for consistently running the latest version of Python, avoiding any conflicts with third-party software and outdated Python versions._

_**After doing so, replace all instances of `python` with `py` in the following commands.**_

## Step 2

Unzip the submission file and navigate to the extracted folder.

Then, open a terminal at the folder.

## Step 3

**Create a virtual environment (highly recommended): Given how other teams may also use Python, and the dependencies used between projects may have conflicts, it is highly recommended to create a virtual environment for running our project.** To create a virtual environment, run the following command:

```shell
python -m venv venv
```

This command creates a new virtual environment named "venv" in the "comp-4321" directory.

The virtual environment can effectively avoid issues such as:
_ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
fastapi 0.104.1 requires anyio<4.0.0,>=3.7.1, but you have anyio 4.3.0 which is incompatible._

## Step 4

Activate the virtual environment: Activate the virtual environment using the appropriate command based on your operating system:

On Windows:

```shell
venv\Scripts\activate
```

On Linux or macOS:

```shell
source venv/bin/activate
```

## Step 5

Install the required packages: In the root directory of the project (i.e., the "comp-4321" directory), there should be a file named "requirements.txt". To install the required packages, run the following command:

_Note: Check again to see if `(venv)` appears in the command prompt for using the virtual environment._

```shell
pip install -r requirements.txt
```

This command will install all the necessary packages specified in the "requirements.txt" file.

## Step 6

Run the crawler using the command for Phase 1.

_Note: Check again to see if `(venv)` appears in the command prompt for using the virtual environment._

```shell
python -m egod_search.crawl -n 30 -d database.db -s spider_result.txt https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm
```

In case of re-run, and the database needs to be cleared, use the appropriate command based on your operating system:

On Windows:

```shell
del database.db
```

On Linux or macOS:

```shell
rm database.db
```

## Important notices

The program says it is `Finished` but does not end, just gets stuck:
On Windows, after the program has finished, the CLI may freeze if the program finishes too quickly. This is a [CPython bug](https://github.com/python/cpython/issues/111604) and is out of our control. Just Ctrl+C to get out of it and ignore the errors as they are harmless.  

If there is an error mentioning `requires a different Python`, for example `ERROR: Package 'egod-search' requires a different Python: 3.10.11 not in '>=3.11.0'`:
Your Python version is outdated and does not support [features the code relies on](https://stackoverflow.com/a/77247460). Please go to <https://www.python.org/downloads/> and download the newest version of Python.

## FAQ

Q: Install does not work

A1: Check again that `(venv)` appears in the command prompt for using the virtual environment. The virtual environment is not entered by default.

Q: `venv\Scripts\activate` does not work for my Windows machine

A: For Windows machines with MinGW-w64, `python.exe` may refer to the MinGW-w64 executable. It does not work because it generates Linux version of virtual environment script, and likely does not come with Python >= 3.11. Use `py` which can guarantee running the Windows Python executable.

## Tested working platforms

Linux: Debian 12 on Python 3.11.2 (older Linux distros do not have >= Python 3.11, either install yourself or switch machines)
Windows: Windows 10 and 11, Python 3.11.2 and 3.12.2

# Highlight of features beyond the required specification

We picked "Exceedingly good speed by using special implementation techinques". Specifically, aside from one lock for the database due to SQLite limitations, we have concurrency for all other parts including the downloader (6 threads by default), database content generation (4 threads by default) and database retrieval (automatically threaded by the Uvicorn web server library). If we used another database that supports concurrent writes, the lock wouldn't be needed.

Exceedingly good speed in the crawler is achieved by asynchronous tasks from the `asyncio` module in Python. When the crawler starts, a `TaskGroup` from `asyncio` is created and populated with asynchronous `Task`s, each waiting on an OS thread to finish network requests. The Python program can process other tasks during this time, speeding up the crawler.

After that, in the index computation, the Python program is overloaded with work to do. Asynchronous tasks will not work here, instead we made a `a_pool_imap` function which creates new Python processes with `Pool` from the `multiprocessing.pool` module. Through multiprocessing, we distribute computation-heavy work across CPU cores to speed up the indexer.

Inside the indexing function, we further speed up the computation using the Numpy library which provides parallel operations on arrays (called "vectorization") instead of coding our own for-loops. The `amax` function is used to obtain the maximum term frequency in the document for pre-computation of normalised term frequency stored as `tf_normalized` in the database for faster retrieval.

Numpy vectorization is further used in retrieval for cosine similarity, and TF-IDF calculation. Instead of calculating element by element, we used numpy to calculate cosine similarity, TF, IDF and TF multiplied by IDF in parallel across many elements to achieve exceedingly good speed in the retrieval. Title similarity for ranking also uses numpy for speed.

## For GUI: Enhanced Real-Time Search Engine Response through WebSocket Connection

WebSocket is a communication protocol that enables bidirectional communication channels over a single TCP connection. Unlike traditional methods that rely on HTML forms and follow a request-response pattern, WebSocket allows for real-time, two-way communication between a client and a server. This persistent connection eliminates the need for repeated requests and excessive HTTP communication overhead, resulting in lower latency and improved responsiveness.

In conventional search engines, search queries are submitted using HTML forms as GET parameters in the HTTP request. However, this approach requires an additional HTTP request, causing the client to clear the current document's head and body in anticipation of receiving new content. As a result, the page momentarily flashes white, creating a visually disruptive experience.

In our search engine, we leverage the NiceGUI framework, which utilizes WebSocket to establish bidirectional communication with the web client. Through this communication channel, the client can submit search queries and receive query results when the server is ready, all within the same connection. During the waiting period, on-screen elements such as the title and buttons remain in the document tree, eliminating any flashing effects.

# Testing of the functions implemented; include screenshots if applicable in the report

## Crawler

The testing of crawler is located in `src/egod_search/crawl/test___init__.py`, `src/egod_search/crawl/test_concurrency.py` and `src/egod_search/crawl/test_main.py`. You run it via................................................................

## Indexer

............................................................................................

## Retrieval function

............................................................................................

## Web Interface

............................................................................................

# Conclusion: What are the strengths and weaknesses of your systems; what you would have done differently if you could re-implement the whole system; what would be the interesting features to add to your system, etc

The system is very fast due to the use of optimisation techniques. If we could re-implement the system, a concurrent database would have been chosen to further speed up the system. An extension could be to consider links in page ranking as well.

# Contribution

33.333% for every member