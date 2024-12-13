-- Data Cleaning Project

# 1. Remove Duplicates
# 2. Standarizing the Data
# 3. Null Values
# 4. Remove any columns which are not relevent


# 1. Remove Duplicates
-- Creating a staging so our raw data is untouched
CREATE TABLE layoffs_staging
LIKE world_layoffs_data_cleaning_project.layoffs
;
SELECT * 
FROM world_layoffs_data_cleaning_project.layoffs_staging;

# Adding Data
INSERT layoffs_staging
SELECT *
FROM layoffs ;

# Checking the data
SELECT *
FROM layoffs_staging ;

# Checking Duplicates
SELECT *,
-- Adding row numbers to heck if we have any duplicate
	-- if there is a duplicate, it will be indicated as 2 in the result
ROW_NUMBER() OVER (
					PARTITION BY company,
                    location, industry, 
                    total_laid_off, percentage_laid_off, 
                    'date', stage, country, 
                    funds_raised_millions) as 'row_num'
FROM layoffs_staging ;

#Filtering row_number - using CTE
WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER (
					PARTITION BY company,
                    location, industry, 
                    total_laid_off, percentage_laid_off, 
                    'date', stage, country, 
                    funds_raised_millions) as 'row_num'
FROM layoffs_staging 
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

--  As we have created a CTE, we cannot delete any data
	-- So we created layoffs_staging2 to remove duplicate data
    
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

-- Inserting the data
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
					PARTITION BY company,
                    location, industry, 
                    total_laid_off, percentage_laid_off, 
                    'date', stage, country, 
                    funds_raised_millions) as 'row_num'
FROM layoffs_staging ;
-- Checking the data
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;
-- Deleting the Duplicates (row_num = 2)
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num = 2; -- Checking if the data is deleted

# 2. Standarizing the Data
SELECT DISTINCT(TRIM(company)) # observed some uneven spacing in company 
FROM layoffs_staging2;

SELECT company, TRIM(company) # comparing
FROM layoffs_staging2;

UPDATE layoffs_staging2 #Updating the changes
SET company = TRIM(company);

# Exploring industry column
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;
	-- Observtation
		-- 3 similar Crypto, Crypto Currency and CryptoCurrency
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

# Updating
UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE 'Crypto%';

# Exploring Country column
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
	-- Observtation
		-- 2 values United States and United States.
SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';

# Updating
UPDATE layoffs_staging2
SET country = "United States"
WHERE country LIKE 'United States%';        

# Checking
SELECT *
FROM layoffs_staging2
WHERE country = 'United States.';

# Exploring Date column
	-- Observation
		-- type is text not int
-- Converting			
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')        
FROM layoffs_staging2  
;   
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y')  ;
-- Checking
SELECT *
FROM layoffs_staging2
;
# Changing data type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

# Exploring Null 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';
	-- Observation
		-- Found common company with one has industry as NULL and other with location
-- Updating the NULL cells 
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''
;
SELECT *
FROM layoffs_staging2 as t1
JOIN layoffs_staging2 as t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;    

UPDATE layoffs_staging2 as t1
JOIN layoffs_staging2 as t2
	ON t1.company = t2.company
SET t1.industry = t2.industry    
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;    

-- Checking
SELECT t1.industry, t2.industry
FROM layoffs_staging2 as t1
JOIN layoffs_staging2 as t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; 

SELECT *
FROM layoffs_staging2 
WHERE company = 'Airbnb'; 

SELECT *
FROM layoffs_staging2 
WHERE industry = ''
OR industry IS NULL; -- Observed 'Bally's Interactive' still has NULL in industry as it is a Unique Value

SELECT *
FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- By observing the data, there a gray area to decide to keep the data or the delete.
	-- Preference is to do an additional research to check if we can find the missing data
	
-- For this project, we will be Deleting the NULL data where both the value for 
	-- for total_laid_off and percentage_laid_off  is NULL

DELETE
FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- We created column `row_num` for our reference
	-- Now as we don't need it now, we will be deleting it

ALTER TABLE layoffs_staging2
DROP column row_num;

SELECT *
FROM layoffs_staging2; -- Final Clean Data














