-- PART 1: CLEANING

-- Step 1: Select all rows from the layoffs table
SELECT * 
FROM world_layoffs.layoffs;

-- Step 2: Create a staging table for data cleaning
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

-- Insert data into the staging table
INSERT INTO world_layoffs.layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- Step 3: Remove duplicates by row number
SELECT company, industry, total_laid_off, `date`,
    ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, `date`) AS row_num
FROM world_layoffs.layoffs_staging;

-- Check for duplicate rows
SELECT *
FROM (
    SELECT company, industry, total_laid_off, `date`,
        ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, `date`) AS row_num
    FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Remove duplicates based on all relevant columns
SELECT *
FROM (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
    FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Create a new staging table to store data with row numbers
CREATE TABLE world_layoffs.layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT,
    row_num INT
);

-- Insert data with row numbers into the new staging table
INSERT INTO world_layoffs.layoffs_staging2
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Delete duplicates from the new staging table
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- Step 4: Standardize Data

-- Update empty industry values to NULL
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Populate NULL industry values with data from matching companies
UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Standardize 'Crypto Currency' and 'CryptoCurrency' to 'Crypto'
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Standardize 'United States' country values by trimming trailing periods
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Standardize date format
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Change the date column to a proper DATE type
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 5: Remove useless data (rows with NULL values in both total_laid_off and percentage_laid_off)
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Drop the row_num column after cleaning
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

-- PART 2: EXPLORATION

-- General exploration of the cleaned data
SELECT * FROM world_layoffs.layoffs_staging2;

-- EASIER QUERIES

-- Maximum total layoffs
SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Maximum and minimum percentage layoffs
SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

-- Companies with 100% layoffs
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1;

-- Companies with 100% layoffs, ordered by funds raised
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- SOMEWHAT TOUGHER QUERIES

-- Companies with the biggest single layoff on a single day
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging2
ORDER BY total_laid_off DESC
LIMIT 5;

-- Companies with the most total layoffs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC
LIMIT 10;

-- Total layoffs by location
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY SUM(total_laid_off) DESC
LIMIT 10;

-- Total layoffs by country
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY SUM(total_laid_off) DESC;

-- Total layoffs by year
SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY YEAR(date) ASC;

-- Total layoffs by industry
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

-- Total layoffs by stage
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY SUM(total_laid_off) DESC;

-- TOUGHER QUERIES

-- Top 3 companies with the most layoffs by year
WITH Company_Year AS (
    SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
    FROM world_layoffs.layoffs_staging2
    GROUP BY company, YEAR(date)
),
Company_Year_Rank AS (
    SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Rolling total of layoffs per month
SELECT SUBSTRING(date,1,7) AS dates, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- Rolling total of layoffs per month with cumulative sum
WITH DATE_CTE AS (
    SELECT SUBSTRING(date,1,7) AS dates, SUM(total_laid_off) AS total_laid_off
    FROM world_layoffs.layoffs_staging2
    GROUP BY dates
    ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) AS rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;



