web: gunicorn -w 1 -k gevent --timeout 500 processor.server:app
worker: celery -A processor worker -l info --concurrency=4
fetcher-wm: python -m scripts.queue_wayback_stories
fetcher-ga: python -m scripts.queue_googlealerts_stories
fetcher-nc: python -m scripts.queue_newscatcher_stories
fetcher-mc: python -m scripts.queue_mediacloud_stories
