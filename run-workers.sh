#!/bin/sh
celery -A processor worker -l info --concurrency=4 &
celery -A processor beat --loglevel=info
