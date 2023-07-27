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