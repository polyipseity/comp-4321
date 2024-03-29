# COMP4321 Phase 1 – Database Design Report <!-- markdownlint-disable MD024 -->

## General Principles

* `rowid` is the ID of the corresponding item to be kept track of
* `content` is the item itself

> In the URL-to-ID table, for instance, `rowid` is the URL-ID, and `content` is the URL itself

## URL-to-ID table `main.urls`

### Fields

* `rowid` is the URL-ID for each URL
  * Primary key of the table
  * Integer
  * Cannot be null
* `content` is the URL
  * Unique (no duplicates)
  * Text (string)
  * Cannot be null
* `redirect_id` is the URL that the page attempts to redirect to (unused for now)
  * Defaults to null
  * Integer
  * Enforces referential integrity to a valid URL-ID

### Indexing

* `rowid` is a primary key and automatically indexed, speeding up getting the URL for a particular URL-ID
* `content` is indexed to speed up searching for the URL-ID for a particular URL

### Triggers
>
> Since `main.pages(links)` store multiple URL-IDs per row using JSON lists, we cannot simply use `ON UPDATE CASCADE ON DELETE RESTRICT` to enforce referential integrity. Triggers are needed as a result.

#### After any possible change in URL-ID for a particular URL, update accordingly
>
> Essentially `ON UPDATE CASCADE`

The `links` column of the `pages` table is updated appropriately to screen for any references to the old `rowid`, and replace them with new `rowid` if found.

#### Before any deletion of a referenced URL, abort and raise error
>
> Essentially `ON DELETE RESTRICT`

The `links` column of the `pages` table is screened for any references to the `rowid` to be deleted, and explicitly aborts if found.

## Word-to-ID table `main.words`

### Fields

* `rowid` is the Word-ID for each word
  * Primary key of the table
  * Integer
  * Cannot be null
* `content` is the word
  * Unique (no duplicates)
  * Text (string)
  * Cannot be null

### Indexing

* `rowid` is a primary key and automatically indexed, speeding up getting the word for a particular Word-ID
* `content` is indexed to speed up searching for the Word-ID for a particular word

## Page information table `main.pages`

### Fields

* `rowid` is the URL-ID of the page
  * Primary key of the table
  * Integer
  * Cannot be null
  * Enforces referential integrity to a valid URL-ID
* `mod_time` is the UNIX timestamp (in seconds) of the last modified time as reported by HTTP header (if not available, then it will be the time of scraping, as prescribed)
  * Integer
  * Cannot be null
* `size` is the size (in bytes) of the page as reported by HTTP header (if not available, then it will be the size of the plaintext, as prescribed)
  * Integer
  * Cannot be null
  * Must be greater than or equal to 0
* `text` is the complete markup of the page
  * Text (string)
  * Cannot be null
* `plaintext` is the plain human-readable text of the page, free of any markup
  * Text (string)
  * Cannot be null
* `title` is the title of the page
  * Text (string)
  * Cannot be null
* `links` are the URL-IDs of the links in the page, stored as JSON
  * Text (string), but *internally we consider it as JSON by using JSON-handling functions on this field*
    * We consider it to be a sorted list of unique URI-IDs, and this is enforced by triggers
  * Cannot be null

### Indexing

* `rowid` is a primary key and automatically indexed, speeding up getting the page information for a particular page specified by URL-ID

### Triggers
>
> Since `links` store multiple URL-IDs per row using JSON lists, we need to enforce validity manually.

#### On insert and delete

* Check that it is valid JSON
* Check that all elements are integers
* Check that there are no duplicate values
* Check that it is a sorted list
* Check that each element is a valid URL-ID

## Word occurrences for each page `main.word_occurrences` & `main.word_occurrences_title`
>
> `_title` variant is used for storing word occurrences in the title, and the other variant is used for storing word occurrences outside of the title

* `page_id` is the URL-ID of the page
  * Part of composite primary key
  * Integer
  * Cannot be null
  * Enforces referential integrity to a valid URL-ID
* `word_id` is the Word-ID of the word
  * Part of composite primary key
  * Integer
  * Cannot be null
  * Enforces referential integrity to a valid Word-ID
* `positions` are the positions of the word in question in the page, stored as JSON
  * Text (string), but *internally we consider it as JSON by using JSON-handling functions on this field*
    * We consider it to be a sorted list of unique nonnegative integers, and this is enforced by triggers
  * Cannot be null
* `frequency` is the frequency of the word in question in the page
  * Generated field, stored in database
    * Since lookup occurs much more frequently than scraping, this enables faster lookup, trading for a slower scraping
  * Integer
  * Cannot be null

### Indexing

* `page_id` and `positions` form a composite primary key and automatically indexed, speeding up getting the positions and frequencies of a particular word in a page specified by Word-ID and URL-ID respectively

### Triggers
>
> Since `positions` store multiple word positions per row using JSON lists, we need to enforce validity manually.

#### On insert and delete

* Check that it is valid JSON
* Check that all elements are integers
* Check that there are no duplicate values
* Check that there are no negative values
* Check that it is a sorted list
