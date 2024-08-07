#!/bin/sh
celery -A processor beat --loglevel=info
