use health_data;

-- creating the table name and the column names for the excel data to upload cleanly into
CREATE TABLE health (
    pageview_id VARCHAR(255),
    user_id VARCHAR(255),
    known_diagnosis VARCHAR(255),
    page_category VARCHAR(255),
    device_type VARCHAR(50),
    page_topic_description VARCHAR(255),
    session_start_time DOUBLE,
    asset_loaded_time DOUBLE,
    time_of_day INT,
    return_visitor INT,
    asset_shown VARCHAR(10),
    conversion INT
);


-- loading the dataset (csv comma delimited form) into sql
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DATASET.csv'
INTO TABLE health
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- testing to see if the files uploaded correctly
SELECT COUNT(*) FROM health;

-- testing to see what the output looks like. making sure everything is included and there are no problems
SELECT * FROM health LIMIT 10;

-- testing to see if all pageview_id rows are unique. if yes -> use as primary key. if no -> add another data column to make a composite key
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT pageview_id) AS unique_pageviews
FROM health;

-- turning pageview_id into the primary key because it has 100,000/100,000 unique rows
ALTER TABLE health ADD PRIMARY KEY (pageview_id);

-- code to test for duplicates in critical columns of data if needed. (change column name for each query to test)
SELECT return_visitor, COUNT(*)
FROM health
GROUP BY return_visitor
HAVING COUNT(*) > 1;


-- checking to see the distribution of asset types shown to users
SELECT asset_shown, COUNT(*) AS views
FROM health
GROUP BY asset_shown
ORDER BY views DESC;


-- PART 2 --------------------------------------------------------------------------------------------------------------------------------------------------------

-- (1)checking conversion rates for all three asset types
SELECT asset_shown, COUNT(*) AS total_views, SUM(conversion) AS total_conversions, 
ROUND(SUM(conversion) / COUNT(*) * 100, 2) AS conversion_rate
FROM health
GROUP BY asset_shown
ORDER BY conversion_rate DESC;


-- (2a)conversion rate by asset and return visitor
-- 0 = New visitor, 1 = Returning visitor
SELECT asset_shown, return_visitor, COUNT(*) AS total_views, SUM(conversion) AS total_conversions, ROUND(SUM(conversion) / COUNT(*) * 100, 2) AS conversion_rate,
    CASE 
        WHEN asset_shown = 'A' THEN SUM(conversion) * 5.00
        WHEN asset_shown = 'B' THEN SUM(conversion) * 7.00
        WHEN asset_shown = 'C' THEN SUM(conversion) * 2.50
        ELSE 0
    END AS total_revenue,
    ROUND(CASE 
        WHEN SUM(conversion) > 0 THEN 
            CASE 
                WHEN asset_shown = 'A' THEN (SUM(conversion) * 5.00) / SUM(conversion)
                WHEN asset_shown = 'B' THEN (SUM(conversion) * 7.00) / SUM(conversion)
                WHEN asset_shown = 'C' THEN (SUM(conversion) * 2.50) / SUM(conversion)
                ELSE 0
            END
        ELSE 0
    END, 2) AS revenue_per_conversion
FROM health
GROUP BY asset_shown, return_visitor
ORDER BY asset_shown, revenue_per_conversion DESC;


-- (2b)Conversion rate by asset and page category
WITH CategoryTotals AS (
    SELECT page_category, COUNT(*) AS total_category_views
    FROM health
    GROUP BY page_category
)
SELECT h.asset_shown, h.page_category, COUNT(*) AS total_views, SUM(h.conversion) AS total_conversions, 
ROUND(SUM(h.conversion) / COUNT(*) * 100, 2) AS conversion_rate, SUM(h.conversion) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END AS total_revenue,  -- Current total revenue generated
    -- Theoretical Conversions if the asset was shown to every user in this category
    ROUND((total_category_views * (SUM(h.conversion) / COUNT(*))), 0) AS potential_total_conversions,
    -- Theoretical Total Revenue if this asset was shown to every user this category
    ROUND((total_category_views * (SUM(h.conversion) / COUNT(*))) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END, 2) AS potential_total_revenue
FROM health h
JOIN CategoryTotals ct ON h.page_category = ct.page_category
GROUP BY h.asset_shown, h.page_category, total_category_views
ORDER BY potential_total_revenue DESC;


-- (2c)potential revenue by asset and page topic description
WITH TopicTotals AS (
    -- Get total views across ALL assets for each page topic
    SELECT page_topic_description, COUNT(*) AS total_topic_views
    FROM health
    GROUP BY page_topic_description
)
SELECT h.asset_shown, h.page_topic_description, COUNT(*) AS total_views, SUM(h.conversion) AS total_conversions, 
ROUND(SUM(h.conversion) / COUNT(*) * 100, 2) AS conversion_rate, SUM(h.conversion) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END AS total_revenue,  
    -- Theoretical Conversions if this asset was shown to every user in this topic
    ROUND((tt.total_topic_views * (SUM(h.conversion) / COUNT(*))), 0) AS potential_total_conversions,
    -- Theoretical Total Revenue if this asset was the only one shown in this topic
    ROUND((tt.total_topic_views * (SUM(h.conversion) / COUNT(*))) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END, 2) AS potential_total_revenue
FROM health h
JOIN TopicTotals tt ON h.page_topic_description = tt.page_topic_description
GROUP BY h.asset_shown, h.page_topic_description, tt.total_topic_views
ORDER BY potential_total_revenue DESC;


-- (3a)asset conversion rates by user type (new(0) user vs returning user(1))
SELECT asset_shown, return_visitor, COUNT(*) AS total_views, SUM(conversion) AS total_conversions, ROUND(SUM(conversion) / COUNT(*) * 100, 2) AS conversion_rate
FROM health
GROUP BY asset_shown, return_visitor
ORDER BY return_visitor, asset_shown DESC;


-- (3b)asset conversion rates by page category
SELECT asset_shown, page_category, COUNT(*) AS total_views, SUM(conversion) AS total_conversions, ROUND(SUM(conversion) / COUNT(*) * 100, 2) AS conversion_rate
FROM health
GROUP BY asset_shown, page_category
ORDER BY asset_shown, conversion_rate DESC;


-- (3c)asset conversion rates by page topic description
SELECT asset_shown, page_topic_description, COUNT(*) AS total_views,
SUM(conversion) AS total_conversions, ROUND(SUM(conversion) / COUNT(*) * 100, 2) AS conversion_rate
FROM health
GROUP BY asset_shown, page_topic_description
ORDER BY asset_shown, conversion_rate DESC;


-- PART 3 ----------------------------------------------------------------------------------------------------------------------------------------------------------

-- finding the best asset for each user type by finding current and potential total revenue
WITH UserTotals AS (
    SELECT return_visitor, COUNT(*) AS total_user_views
    FROM health
    GROUP BY return_visitor
)
SELECT h.asset_shown, h.return_visitor, COUNT(*) AS total_views, 
SUM(h.conversion) AS total_conversions, ROUND(SUM(h.conversion) / COUNT(*) * 100, 2) AS conversion_rate,  
    -- Actual revenue generated
    SUM(h.conversion) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END AS total_revenue,  
    -- Potential revenue if the best asset for this user type was shown to ALL users
    ROUND((ut.total_user_views * (SUM(h.conversion) / COUNT(*))) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END, 2) AS potential_total_revenue
FROM health h
JOIN UserTotals ut ON h.return_visitor = ut.return_visitor
GROUP BY h.asset_shown, h.return_visitor, ut.total_user_views
ORDER BY potential_total_revenue DESC;


-- finding best asset for each page category
WITH CategoryTotals AS (
    SELECT page_category, COUNT(*) AS total_category_views
    FROM health
    GROUP BY page_category
)
SELECT h.asset_shown, h.page_category, COUNT(*) AS total_views,  
SUM(h.conversion) AS total_conversions, ROUND(SUM(h.conversion) / COUNT(*) * 100, 2) AS conversion_rate,  
    SUM(h.conversion) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END AS total_revenue,  
    -- Potential revenue if the best asset was always shown in this category
    ROUND((ct.total_category_views * (SUM(h.conversion) / COUNT(*))) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END, 2) AS potential_total_revenue
FROM health h
JOIN CategoryTotals ct ON h.page_category = ct.page_category
GROUP BY h.asset_shown, h.page_category, ct.total_category_views
ORDER BY potential_total_revenue DESC;


-- finding the best asset for each page topic description
WITH TopicTotals AS (
    SELECT page_topic_description, COUNT(*) AS total_topic_views
    FROM health
    GROUP BY page_topic_description
)
SELECT h.asset_shown, h.page_topic_description, COUNT(*) AS total_views,  
SUM(h.conversion) AS total_conversions, ROUND(SUM(h.conversion) / COUNT(*) * 100, 2) AS conversion_rate,  
    -- Actual revenue generated
    SUM(h.conversion) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END AS total_revenue,  
    -- Potential revenue if the best asset was always shown in this topic
    ROUND((tt.total_topic_views * (SUM(h.conversion) / COUNT(*))) * 
        CASE 
            WHEN h.asset_shown = 'A' THEN 5.00
            WHEN h.asset_shown = 'B' THEN 7.00
            WHEN h.asset_shown = 'C' THEN 2.50
            ELSE 0
        END, 2) AS potential_total_revenue
FROM health h
JOIN TopicTotals tt ON h.page_topic_description = tt.page_topic_description
GROUP BY h.asset_shown, h.page_topic_description, tt.total_topic_views
ORDER BY potential_total_revenue DESC;

-- The end!


-- NOTE: A few of these queries might seem similar or repetitive, but they all answer slightly different questions or have more/less specific outputs.