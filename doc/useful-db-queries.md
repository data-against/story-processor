Useful queries for the logging database
=======================================

Connect to db: `dokku postgres:connect mc-story-processor-db`

**Stories fetched by date**

SELECT processed_date::date as day, count(*) as stories from stories where project_id=23 and above_threshold is True group by 1 order by 1  DESC;

**Stories unprocessed by queued day**

SELECT queued_date::date as day, count(*) as stories from stories where processed_date is null group by 1 order by 1  DESC;

**Stories above/below threshold by day**

SELECT processed_date::date as day, above_threshold,  count(*) as stories from stories group by 1, 2, 3 order by 2 DESC limit 10;

**Above-threshold stories for a project by day**

SELECT published_date::date as day, above_threshold,  count(*) as stories from stories where project_id=23 and above_threshold is True group by 1, 2 order by 1 DESC limit 10;

**Latest above threshold stories for a project**

SELECT published_date, above_threshold,  stories_id from stories where project_id=23 and above_threshold is True order by published_date DESC limit 20;

**Chart 1: Stories above threshold sent to main server** 
SELECT processed_date::date AS day, COUNT(1) AS stories, source
FROM stories
WHERE processed_date IS NOT NULL
AND processed_date >= CURRENT_DATE - INTERVAL '45 days'
AND above_threshold = TRUE
GROUP BY 1, source
ORDER BY 1 DESC;

**Chart 2: Stories by published dayStories discovered on each platform based on the guessed date of publication, grouped by the data source they originally came from**
SELECT published_date::date AS day, COUNT(1) AS stories
FROM stories
WHERE published_date IS NOT NULL
AND published_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1 DESC;

**Chart 3: Stories by processed day**
SELECT processed_date::date AS day, COUNT(1) AS stories
FROM stories
WHERE processed_date IS NOT NULL
AND processed_date >= CURRENT_DATE - INTERVAL '45 days'
GROUP BY 1
ORDER BY 1 DESC;

**Specific Project Stories where above threshold true**
SELECT * FROM Stories
WHERE project_id = 72 
AND above_threshold = TRUE 
AND posted_date IS NOT NULL;

**Binned Model Scores By Project**
SELECT ROUND(CAST(model_score AS numeric), 1) AS value, COUNT(1) AS frequency
FROM stories
WHERE project_id = 72 
AND model_score IS NOT NULL
GROUP BY 1
ORDER BY 1;


**Below story count**
SELECT COUNT(1) 
FROM stories 
WHERE project_id = 72 
AND above_threshold = FALSE;


**above story count**
SELECT COUNT(1) 
FROM stories 
WHERE project_id = 72 
AND posted_date IS NOT NULL 
AND above_threshold = TRUE;
