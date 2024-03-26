-- main.urls
CREATE TABLE IF NOT EXISTS main.urls (
  rowid INTEGER NOT NULL PRIMARY KEY,
  content TEXT NOT NULL UNIQUE,
  redirect_id INTEGER DEFAULT NULL REFERENCES urls(rowid) ON UPDATE CASCADE ON DELETE RESTRICT
) STRICT;
CREATE INDEX IF NOT EXISTS main.urls_content_index ON urls (content ASC);
DROP TRIGGER IF EXISTS main.update_urls;
CREATE TRIGGER main.update_urls BEFORE
UPDATE OF rowid ON main.urls FOR EACH ROW BEGIN
UPDATE pages
SET links = (
    SELECT json_group_array(value)
    FROM (
        SELECT iif(value = OLD.rowid, NEW.rowid, OLD.rowid) AS value
        FROM json_each(links)
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
SELECT RAISE(ABORT, 'cannot delete referenced URLs')
FROM pages
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
  text TEXT NOT NULL,
  plaintext TEXT NOT NULL,
  title TEXT NOT NULL,
  links TEXT NOT NULL CHECK(json_valid(links) & 3) -- type: JSON
) STRICT;
-- main.word_occurrences
CREATE TABLE IF NOT EXISTS main.word_occurrences (
  page_id INTEGER NOT NULL REFERENCES pages(rowid) ON UPDATE CASCADE ON DELETE RESTRICT,
  word_id INTEGER NOT NULL REFERENCES words(rowid) ON UPDATE CASCADE ON DELETE RESTRICT,
  positions TEXT NOT NULL CHECK(json_valid(positions) & 3),
  -- type: JSON
  frequency INTEGER NOT NULL GENERATED ALWAYS AS (json_array_length(positions)) STORED,
  PRIMARY KEY (page_id, word_id)
) STRICT,
WITHOUT ROWID;