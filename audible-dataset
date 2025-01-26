-- Data Cleaning with SQL

-- dataset: https://www.kaggle.com/datasets/snehangsude/audible-dataset?select=audible_uncleaned.csv



-- Look at the data

SELECT *
FROM audible_audiobooks.audible_uncleaned;


-- Create copy of data to work on

CREATE TABLE audible_staging AS 
TABLE audible_audiobooks.audible_uncleaned;




-- DUPLICATES

-- No duplicates found

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY name, author, narrator, time, releasedate, language, price) AS row_num
FROM audible_audiobooks.audible_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;



-- STANDARDIZE AUTHOR AND NARRATOR

-- Remove 'Writtenby:' from author field and 'Narratedby:' from narrator field

SELECT author, narrator
FROM audible_audiobooks.audible_staging;

UPDATE audible_audiobooks.audible_staging
SET 
	author = TRIM(LEADING 'Writtenby:' FROM author),
	narrator = TRIM(LEADING 'Narratedby:' FROM narrator);


-- Fix author and narrator fields with Regular Expressions

UPDATE audible_audiobooks.audible_staging
SET author = 
REGEXP_REPLACE(
	REGEXP_REPLACE(
		REGEXP_REPLACE(
			REGEXP_REPLACE(
				REGEXP_REPLACE(
					REGEXP_REPLACE(
						REGEXP_REPLACE(
							REGEXP_REPLACE(
								REGEXP_REPLACE(
    								REGEXP_REPLACE(
       									REGEXP_REPLACE(
            								REGEXP_REPLACE(author, 
                							'([a-z])([A-Z])', '\1 \2', 'g'),      -- adds space between lowercase and uppercase letters 
            							'([A-Za-z])\.(?=[A-Za-z])', '\1. ', 'g'), -- adds space after periods when followed by letters (e.g. initials)
          							',([A-Za-z])',', \1','g'),                    -- adds space after commas when followed by letters
        						'([A-Za-z])([0-9])', '\1 \2', 'g'),               -- adds space between letters and numbers
    						'([0-9])([A-Za-z])', '\1 \2', 'g'),                   -- adds space between numbers and letters
    					',([A-Za-zÅåÄäÖö])', ', \1', 'g'),                        -- adds space after commas when followed by accented characters
  					'([ÁÉÍÓÚáéíóú])([A-Z])', '\1 \2', 'g'),                       -- adds space between accented letters and uppercase letters
				'([А-Яа-я])([А-Я])','\1 \2','g'),                                 -- adds space between Cyrillic characters (Russian, etc.)
			'([А-Яа-я])\.(?=[А-Яа-я])', '\1. ', 'g'),                             -- adds space after periods when followed by Cyrillic letters
		',([А-Яа-я])', ', \1', 'g'),                                              -- adds space after commas when followed by Cyrillic letters
 	'Mc ', 'Mc', 'g'),                                                            -- removes space that was previously added to surnames such as 'McCool'
 'BKFKStudio', 'BKFK Studio', 'g');                                               -- adds space between 'BKFK' and 'Studio'

UPDATE audible_audiobooks.audible_staging
SET narrator = 
REGEXP_REPLACE(
	REGEXP_REPLACE(
		REGEXP_REPLACE(
			REGEXP_REPLACE(
				REGEXP_REPLACE(
					REGEXP_REPLACE(
						REGEXP_REPLACE(
							REGEXP_REPLACE(
								REGEXP_REPLACE(
    								REGEXP_REPLACE(
       									REGEXP_REPLACE(
            								REGEXP_REPLACE(narrator, 
                							'([a-z])([A-Z])', '\1 \2', 'g'),      -- adds space between lowercase and uppercase letters 
            							'([A-Za-z])\.(?=[A-Za-z])', '\1. ', 'g'), -- adds space after periods when followed by letters (e.g. initials)
          							',([A-Za-z])',', \1','g'),                    -- adds space after commas when followed by letters
        						'([A-Za-z])([0-9])', '\1 \2', 'g'),               -- adds space between letters and numbers
    						'([0-9])([A-Za-z])', '\1 \2', 'g'),                   -- adds space between numbers and letters
    					',([A-Za-zÅåÄäÖö])', ', \1', 'g'),                        -- adds space after commas when followed by accented characters
  					'([ÁÉÍÓÚáéíóú])([A-Z])', '\1 \2', 'g'),                       -- adds space between accented letters and uppercase letters
				'([А-Яа-я])([А-Я])','\1 \2','g'),                                 -- adds space between Cyrillic characters (Russian, etc.)
			'([А-Яа-я])\.(?=[А-Яа-я])', '\1. ', 'g'),                             -- adds space after periods when followed by Cyrillic letters
		',([А-Яа-я])', ', \1', 'g'),                                              -- adds space after commas when followed by Cyrillic letters
 	'Mc ', 'Mc', 'g'),                                                            -- removes space that was previously added to surnames such as 'McCool'
 'BKFKStudio', 'BKFK Studio', 'g');                                               -- adds space between 'BKFK' and 'Studio'                                                          -- removes space that was previously added to surnames such as 'McCool'



-- Trim trailing commas from name, author and narrator columns
    
UPDATE audible_audiobooks.audible_staging
SET 
	name = TRIM(TRAILING ',' FROM name),
	author = TRIM(TRAILING ',' FROM author),
	narrator = TRIM(TRAILING ',' FROM narrator);


-- Standardize the use of 'translator' and 'illustrator' in author column

-- Values 'traductor', 'traducteur', and 'Übersetzer' are changed to 'translator'
-- Values ' ilustrador' is changed to 'illustrator'
    
UPDATE audible_audiobooks.audible_staging
SET author = 
	REPLACE(
		REPLACE(
			REPLACE(
				REPLACE(author, 
				'traductor', 'translator'), 
			'traducteur', 'translator'), 
		'Übersetzer', 'translator'), 
	'ilustrador', 'illustrator');


-- Fix typo: 'nonymous' in narrator columns is changed to 'anonymous'

UPDATE audible_audiobooks.audible_staging
SET narrator = REPLACE(narrator, 'nonymous', 'anonymous')
WHERE narrator = 'nonymous';



-- STANDARDIZE LANGUAGE

-- Have a look at all language variations

SELECT DISTINCT(language)
FROM audible_audiobooks.audible_staging;


-- Capitalize first letter of each word

UPDATE audible_audiobooks.audible_staging
SET language = INITCAP(language);


-- Remove underscore in 'Mandarin_Chinese' value

UPDATE audible_audiobooks.audible_staging
SET language = REPLACE(language, '_', ' ');



-- FORMAT DATE

-- TO_DATE(releasedate, 'DD/MM/YY'): converts the string stored in the date column (which is in DD/MM/YY format) into a proper DATE type
-- TO_CHAR(..., 'YYYY-MM-DD'): formats the DATE type into the YYYY-MM-DD format
-- formatted_date: alias for the result of the transformation

-- Check that dates formatting works as expected before update

SELECT releasedate, TO_CHAR(TO_DATE(releasedate , 'DD/MM/YY'), 'YYYY-MM-DD') AS formatted_date
FROM audible_audiobooks.audible_staging;


-- Update releasedate column

UPDATE audible_audiobooks.audible_staging
SET releasedate = TO_CHAR(TO_DATE(releasedate , 'DD/MM/YY'), 'YYYY-MM-DD');



-- STANDARDIZE TIME

-- Add new columns: hours and mins

ALTER TABLE audible_audiobooks.audible_staging
ADD COLUMN hours VARCHAR(50), 
ADD COLUMN mins VARCHAR(50);


-- Split values that include both hours and minutes into respective columns

UPDATE audible_audiobooks.audible_staging
SET 
    hours = SPLIT_PART(time, 'and', 1),
    mins = SPLIT_PART(time, 'and', 2)
WHERE time LIKE '%and%';


-- Remove ' hrs', ' hr', ' mins' and ' min' text from columns

UPDATE audible_audiobooks.audible_staging 
SET 
	hours = REPLACE(REPLACE(hours, ' hrs', ''), ' hr', ''),
	mins = REPLACE(REPLACE(mins, ' mins', ''), ' min', '')
WHERE time LIKE '%and%';


-- Transfer minutes values from time to mins column

SELECT time, hours, mins
FROM audible_audiobooks.audible_staging
WHERE time NOT LIKE '%and%';

UPDATE audible_audiobooks.audible_staging
SET mins = time
WHERE time NOT LIKE '%and%';

UPDATE audible_audiobooks.audible_staging
SET mins = REPLACE(REPLACE(mins, ' mins', ''), ' min', '')
WHERE time NOT LIKE '%and%';


-- Move values with only hours to correct column

SELECT time, hours, mins
FROM audible_audiobooks.audible_staging
WHERE mins LIKE '%hr%';

UPDATE audible_audiobooks.audible_staging
SET 
	hours = mins,
	mins = '0'
WHERE mins LIKE '%hr%';

SELECT time, hours, mins
FROM audible_audiobooks.audible_staging
WHERE hours LIKE '%hr%';

UPDATE audible_audiobooks.audible_staging
SET hours = REPLACE(REPLACE(hours, ' hrs', ''), ' hr', '')
WHERE hours LIKE '%hr%';


-- Replace NULL values with '0'

UPDATE audible_audiobooks.audible_staging
SET hours = '0'
WHERE hours IS NULL;


-- Round 'Less than 1 minute' values to '1'

SELECT time, hours, mins
FROM audible_audiobooks.audible_staging
WHERE mins LIKE 'Less%';

UPDATE audible_audiobooks.audible_staging
SET mins = '1'
WHERE mins LIKE 'Less%';


-- Convert hours and mins to numeric

ALTER TABLE audible_audiobooks.audible_staging
ADD COLUMN hours_numeric NUMERIC(5, 0),
ADD COLUMN mins_numeric NUMERIC(5, 0);

UPDATE audible_audiobooks.audible_staging
SET 
	hours_numeric = CAST(hours AS NUMERIC),
	mins_numeric = CAST(mins AS NUMERIC);


-- Caluculate total time in mins

ALTER TABLE audible_audiobooks.audible_staging
ADD COLUMN total_time_mins NUMERIC(10, 0);

UPDATE audible_audiobooks.audible_staging
SET total_time_mins = ((hours_numeric * 60) + mins_numeric);

SELECT time, hours, hours_numeric, mins, mins_numeric, total_time_mins
FROM audible_audiobooks.audible_staging;


-- Add two new columns: price_temp and price_numeric

ALTER TABLE audible_audiobooks.audible_staging
ADD COLUMN price_temp VARCHAR(50),
ADD COLUMN price_numeric NUMERIC(10, 2);


-- Remove commas used to separate thousands from price_temp

UPDATE audible_audiobooks.audible_staging
SET price_temp = REPLACE(price, ',', '');


-- Update price_temp instances with value 'Free' to '0'

UPDATE audible_audiobooks.audible_staging
SET price_temp = '0'
WHERE price_temp = 'Free';


-- Convert temp_price to numeric format

UPDATE audible_audiobooks.audible_staging
SET price_numeric = CAST(price_temp AS NUMERIC);


-- Check that price_numeric values are correct

SELECT price, price_temp, price_numeric
FROM audible_audiobooks.audible_staging
ORDER BY price_numeric;



-- FORMAT RATINGS

-- Split stars column into three columns: 1) average rating, 2) max  rating, and 3) nr of ratings

SELECT DISTINCT(stars)
FROM audible_audiobooks.audible_staging;


-- Add column for average rating and populate it

ALTER TABLE audible_audiobooks.audible_staging
ADD COLUMN avg_rating VARCHAR(50);

UPDATE audible_audiobooks.audible_staging
SET avg_rating = SPLIT_PART(stars, ' out of 5 stars', 1)
WHERE stars != 'Not rated yet';


-- Update avg_rating to NULL for instances that have not yet been rated

UPDATE audible_audiobooks.audible_staging
SET avg_rating = NULL
WHERE stars = 'Not rated yet';


-- Convert avg_rating to numeric format

UPDATE audible_audiobooks.audible_staging
SET avg_rating = CAST(avg_rating AS NUMERIC);


-- Add column for max number of stars and set value to 5

ALTER TABLE audible_audiobooks.audible_staging
ADD COLUMN max_rating NUMERIC(2,0);

UPDATE audible_audiobooks.audible_staging
SET max_rating = 5;


-- Add columns for number of ratings and populate it

ALTER TABLE audible_audiobooks.audible_staging
ADD COLUMN nr_of_ratings VARCHAR(50);

UPDATE audible_audiobooks.audible_staging
SET nr_of_ratings = SPLIT_PART(stars, 'stars', 2);

SELECT stars, nr_of_ratings
FROM audible_audiobooks.audible_staging
WHERE stars = 'Not rated yet';


-- Set nr_of_ratings to '0' for those books that have not been rated yet

UPDATE audible_audiobooks.audible_staging
SET nr_of_ratings = '0'
WHERE stars = 'Not rated yet';

UPDATE audible_audiobooks.audible_staging
SET nr_of_ratings = REPLACE(REPLACE(nr_of_ratings, ' ratings', ''), ' rating', '');


-- Remove commas used to separate thousands from nr_of_ratings

UPDATE audible_audiobooks.audible_staging
SET nr_of_ratings = REPLACE(nr_of_ratings, ',', '');


-- Convert nr_of_ratings to numeric format

UPDATE audible_audiobooks.audible_staging
SET nr_of_ratings = CAST(nr_of_ratings AS NUMERIC);


-- Check that values are correct

SELECT stars, avg_rating, max_rating, nr_of_ratings
FROM audible_audiobooks.audible_staging;



-- TIDY UP

-- Create a final version of the table

SELECT *
FROM audible_audiobooks.audible_staging;

CREATE TABLE audible_cleaned AS 
TABLE audible_audiobooks.audible_staging;


-- Remove unnecessary columns (price, price_temp, hours, mins, stars)

ALTER TABLE audible_audiobooks.audible_cleaned
DROP COLUMN price,
DROP COLUMN price_temp,
DROP COLUMN hours,
DROP COLUMN mins,
DROP COLUMN time,
DROP COLUMN stars;


-- Rename price_numeric as price, hours_numeric as hours, mins_numeric as mins

ALTER TABLE audible_audiobooks.audible_cleaned
RENAME COLUMN price_numeric TO price;

ALTER TABLE audible_audiobooks.audible_cleaned
RENAME COLUMN hours_numeric TO hours;

ALTER TABLE audible_audiobooks.audible_cleaned
RENAME COLUMN mins_numeric TO mins;


-- Final version

SELECT *
FROM audible_audiobooks.audible_cleaned;


--DROP TABLE audible_staging;
--DROP TABLE audible_cleaned;















