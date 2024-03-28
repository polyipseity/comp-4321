-- main.urls
CREATE TABLE IF NOT EXISTS main.urls (
  rowid INTEGER NOT NULL PRIMARY KEY,
  content TEXT NOT NULL UNIQUE,
  redirect_id INTEGER DEFAULT NULL REFERENCES urls(rowid) ON UPDATE CASCADE ON DELETE RESTRICT
) STRICT;
CREATE INDEX IF NOT EXISTS main.urls_content_index ON urls (content ASC);
DROP TRIGGER IF EXISTS main.update_urls;
CREATE TRIGGER main.update_urls
AFTER
UPDATE OF rowid ON main.urls FOR EACH ROW BEGIN
UPDATE pages
SET links = (
    SELECT json_group_array(value)
    FROM (
        SELECT iif(value = OLD.rowid, NEW.rowid, OLD.rowid) AS value
        FROM json_each(links)
        ORDER BY value
      )
  )
WHERE EXISTS (
    SELECT NULL
    FROM json_each(links)
    WHERE value = OLD.rowid
  );
END;
DROP TRIGGER IF EXISTS main.delete_urls;
CREATE TRIGGER main.delete_urls BEFORE DELETE ON main.urls FOR EACH ROW BEGIN
SELECT raise(ABORT, 'cannot delete referenced URLs')
FROM main.pages
WHERE EXISTS (
    SELECT NULL
    FROM json_each(links)
    WHERE value = OLD.rowid
  );
END;
-- main.words
CREATE TABLE IF NOT EXISTS main.words (
  rowid INTEGER NOT NULL PRIMARY KEY,
  content TEXT NOT NULL UNIQUE
) STRICT;
CREATE INDEX IF NOT EXISTS main.words_content_index ON words (content ASC);
-- main.pages
CREATE TABLE IF NOT EXISTS main.pages (
  rowid INTEGER NOT NULL PRIMARY KEY REFERENCES urls(rowid) ON UPDATE CASCADE ON DELETE RESTRICT,
  mod_time INTEGER,
  size INTEGER NOT NULL CHECK(size >= 0),
  text TEXT NOT NULL,
  plaintext TEXT NOT NULL,
  title TEXT NOT NULL,
  links TEXT NOT NULL -- type: JSON, sorted list of unique urls(rowid)
) STRICT;
DROP TRIGGER IF EXISTS main.word_occurrences_check_links_insert;
CREATE TRIGGER main.word_occurrences_check_links_insert BEFORE
INSERT ON main.pages FOR EACH ROW BEGIN
SELECT raise(ABORT, 'invalid JSON')
WHERE json_valid(NEW.links) & 3 = 0;
SELECT raise(ABORT, 'not list of integers')
FROM json_each(NEW.links)
WHERE type != 'integer';
SELECT raise(ABORT, 'duplicated values')
FROM json_each(NEW.links)
GROUP BY value
HAVING count(*) > 1;
SELECT raise(ABORT, 'unsorted list')
WHERE json(NEW.links) != (
    SELECT json_group_array(value)
    FROM (
        SELECT value
        FROM json_each(NEW.links)
        ORDER BY value
      )
  );
SELECT raise(ABORT, 'not urls(rowid)')
FROM json_each(NEW.links) AS links
  LEFT OUTER JOIN main.urls ON main.urls.rowid = links.value
WHERE main.urls.rowid IS NULL;
END;
DROP TRIGGER IF EXISTS main.word_occurrences_check_links_update;
CREATE TRIGGER main.word_occurrences_check_links_update BEFORE
UPDATE OF links ON main.pages FOR EACH ROW BEGIN
SELECT raise(ABORT, 'invalid JSON')
WHERE json_valid(NEW.links) & 3 = 0;
SELECT raise(ABORT, 'not list of integers')
FROM json_each(NEW.links)
WHERE type != 'integer';
SELECT raise(ABORT, 'duplicated values')
FROM json_each(NEW.links)
GROUP BY value
HAVING count(*) > 1;
SELECT raise(ABORT, 'unsorted list')
WHERE json(NEW.links) != (
    SELECT json_group_array(value)
    FROM (
        SELECT value
        FROM json_each(NEW.links)
        ORDER BY value
      )
  );
SELECT raise(ABORT, 'not urls(rowid)')
FROM json_each(NEW.links) AS links
  LEFT OUTER JOIN main.urls ON main.urls.rowid = links.value
WHERE main.urls.rowid IS NULL;
END;
-- main.word_occurrences
CREATE TABLE IF NOT EXISTS main.word_occurrences (
  page_id INTEGER NOT NULL REFERENCES pages(rowid) ON UPDATE CASCADE ON DELETE RESTRICT,
  word_id INTEGER NOT NULL REFERENCES words(rowid) ON UPDATE CASCADE ON DELETE RESTRICT,
  type TEXT NOT NULL CHECK(type IN ('text', 'title')),
  positions TEXT NOT NULL,
  -- type: JSON, sorted list of unique nonnegative integers
  frequency INTEGER NOT NULL GENERATED ALWAYS AS (json_array_length(positions)) STORED,
  PRIMARY KEY (page_id, word_id, type)
) STRICT,
WITHOUT ROWID;
DROP TRIGGER IF EXISTS main.word_occurrences_check_positions_insert;
CREATE TRIGGER main.word_occurrences_check_positions_insert BEFORE
INSERT ON main.word_occurrences FOR EACH ROW BEGIN
SELECT raise(ABORT, 'invalid JSON')
WHERE json_valid(NEW.positions) & 3 = 0;
SELECT raise(ABORT, 'not list of integers')
FROM json_each(NEW.positions)
WHERE type != 'integer';
SELECT raise(ABORT, 'duplicated values')
FROM json_each(NEW.positions)
GROUP BY value
HAVING count(*) > 1;
SELECT raise(ABORT, 'negative values')
FROM json_each(NEW.positions)
WHERE value < 0;
SELECT raise(ABORT, 'unsorted list')
WHERE json(NEW.positions) != (
    SELECT json_group_array(value)
    FROM (
        SELECT value
        FROM json_each(NEW.positions)
        ORDER BY value
      )
  );
END;
DROP TRIGGER IF EXISTS main.word_occurrences_check_positions_update;
CREATE TRIGGER main.word_occurrences_check_positions_update BEFORE
UPDATE OF positions ON main.word_occurrences FOR EACH ROW BEGIN
SELECT raise(ABORT, 'invalid JSON')
WHERE json_valid(NEW.positions) & 3 = 0;
SELECT raise(ABORT, 'not list of integers')
FROM json_each(NEW.positions)
WHERE type != 'integer';
SELECT raise(ABORT, 'duplicated values')
FROM json_each(NEW.positions)
GROUP BY value
HAVING count(*) > 1;
SELECT raise(ABORT, 'negative values')
FROM json_each(NEW.positions)
WHERE value < 0;
SELECT raise(ABORT, 'unsorted list')
WHERE json(NEW.positions) != (
    SELECT json_group_array(value)
    FROM (
        SELECT value
        FROM json_each(NEW.positions)
        ORDER BY value
      )
  );
END;