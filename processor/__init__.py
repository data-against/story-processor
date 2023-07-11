import os
import logging
import sys
from dotenv import load_dotenv
import mediacloud.api
import mediacloud_legacy.api
from flask import Flask
from sentry_sdk.integrations.logging import ignore_logger
from sentry_sdk import init
from typing import Dict
from sqlalchemy import create_engine

VERSION = "3.3.1"
SOURCE_GOOGLE_ALERTS = "google-alerts"
SOURCE_MEDIA_CLOUD = "media-cloud"
SOURCE_NEWSCATCHER = "newscatcher"
SOURCE_WAYBACK_MACHINE = "wayback-machine"
PLATFORMS = [SOURCE_GOOGLE_ALERTS, SOURCE_MEDIA_CLOUD, SOURCE_NEWSCATCHER, SOURCE_WAYBACK_MACHINE]

load_dotenv()  # load config from .env file (local) or env vars (production)

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
path_to_log_dir = os.path.join(base_dir, 'logs')

# set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger(__name__)
logger.info("------------------------------------------------------------------------")
logger.info("Starting up Feminicide Story Processor v{}".format(VERSION))
# supress annoying "not enough comments" and "using custom extraction" notes# logger = logging.getLogger(__name__)
loggers_to_skip = ['trafilatura.core', 'trafilatura.metadata', 'readability.readability']
for item in loggers_to_skip:
    logging.getLogger(item).setLevel(logging.WARNING)

# read in environment variables
MC_API_TOKEN = os.environ.get('MC_API_TOKEN', None)  # sensitive, so don't log it
if MC_API_TOKEN is None:
    logger.error("  ❌ No MC_API_TOKEN env var specified. Pathetically refusing to start!")
    sys.exit(1)

MC_LEGACY_API_KEY = os.environ.get('MC_LEGACY_API_KEY', None)
if MC_LEGACY_API_KEY is None:
    logger.warning("  ⚠️ No MC_LEGACY_API_KEY env var specified. Will continue without support for that.")

BROKER_URL = os.environ.get('BROKER_URL', None)
if BROKER_URL is None:
    logger.warning("  ⚠️ No BROKER_URL env var specified. Using sqlite, which will perform poorly")
    BROKER_URL = "db+sqlite:///results.sqlite"
logger.info("  Queue at {}".format(BROKER_URL))

SENTRY_DSN = os.environ.get('SENTRY_DSN', None)  # optional
if SENTRY_DSN:
    from sentry_sdk.integrations.celery import CeleryIntegration
    init(dsn=SENTRY_DSN, release=VERSION,
         integrations=[CeleryIntegration()])
    ignore_logger('trafilatura.utils')
    logger.info("  SENTRY_DSN: {}".format(SENTRY_DSN))
else:
    logger.info("  Not logging errors to Sentry")

FEMINICIDE_API_URL = os.environ.get('FEMINICIDE_API_URL', None)
if FEMINICIDE_API_URL is None:
    logger.error("  ❌ No FEMINICIDE_API_URL is specified. Bailing because we can't list projects to run!")
    sys.exit(1)
else:
    logger.info("  Config server at at {}".format(FEMINICIDE_API_URL))

FEMINICIDE_API_KEY = os.environ.get('FEMINICIDE_API_KEY', None)
if FEMINICIDE_API_KEY is None:
    logger.error("  ❌ No FEMINICIDE_API_KEY is specified. Bailing because we can't send things to the main server without one")
    sys.exit(1)

SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', None)
if SQLALCHEMY_DATABASE_URI is None:
    logger.warning("  ⚠️ ️No SQLALCHEMY_DATABASE_URI is specified. Using sqlite which will perform poorly")
    SQLALCHEMY_DATABASE_URI = 'sqlite:///data.db'
    engine = create_engine(SQLALCHEMY_DATABASE_URI)  # use defaults (probably in test mode)
else:
    # bumped pool size up for parallel tasks - the max will be sum of `pool_size` and `max_overflow`
    engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=20, max_overflow=30)


ENTITY_SERVER_URL = os.environ['ENTITY_SERVER_URL']
if ENTITY_SERVER_URL is None:
    logger.warning("  ⚠️ No ENTITY_SERVER_URL is specified. You won't get entities in the stories sent to the  main server.")


NEWSCATCHER_API_KEY = os.environ['NEWSCATCHER_API_KEY']
if NEWSCATCHER_API_KEY is None:
    logger.warning("  ⚠️ No NEWSCATCHER_API_KEY is specified. We won't be fetching from Newscatcher.")

SLACK_APP_TOKEN = os.environ.get('SLACK_APP_TOKEN', None) 
if SLACK_APP_TOKEN is None:
    logger.warning("  ⚠️ No SLACK_APP_TOKEN env var specified. We won't be sending slack updates.")

SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN', None)  
if SLACK_BOT_TOKEN is None:
    logger.warning("  ⚠️ No SLACK_BOT_TOKEN env var specified. We won't be sending slack updates.")

SLACK_CHANNEL_ID = os.environ.get('SLACK_CHANNEL_ID', None)  
if SLACK_CHANNEL_ID is None:
    logger.warning("  ⚠️ No CHANNEL_ID env var specified. We won't be sending slack updates.")

def get_mc_client() -> mediacloud.api.DirectoryApi:
    """
    A central place to get the Media Cloud client
    :return: an media cloud client with the API key from the environment variable
    """
    return mediacloud.api.DirectoryApi(MC_API_TOKEN)


def get_mc_legacy_client() -> mediacloud_legacy.api.AdminMediaCloud:
    """
    A central place to get the Media Cloud legacy client
    :return: an admin media cloud client with the API key from the environment variable
    """
    return mediacloud_legacy.api.AdminMediaCloud(MC_LEGACY_API_KEY)


def create_flask_app() -> Flask:
    """
    Create and configure the Flask app. Standard practice is to do this in a factory method like this.
    :return: a fully configured Flask web app
    """
    return Flask(__name__)


def is_email_configured() -> bool:
    return (os.environ.get('SMTP_USER_NAME', None) is not None) and \
            (os.environ.get('SMTP_PASSWORD', None) is not None) and \
            (os.environ.get('SMTP_ADDRESS', None) is not None) and \
            (os.environ.get('SMTP_PORT', None) is not None) and \
            (os.environ.get('SMTP_FROM', None) is not None) and \
            (os.environ.get('NOTIFY_EMAILS', None) is not None)


def get_email_config() -> Dict:
    return dict(
        user_name=os.environ.get('SMTP_USER_NAME', None),
        password=os.environ.get('SMTP_PASSWORD', None),
        address=os.environ.get('SMTP_ADDRESS', None),
        port=os.environ.get('SMTP_PORT', None),
        from_address=os.environ.get('SMTP_FROM', None),
        notify_emails=os.environ.get('NOTIFY_EMAILS', "").split(",")
    )

def get_slack_config() -> Dict:
    return dict(
        app_token=SLACK_APP_TOKEN,
        bot_token=SLACK_BOT_TOKEN,
        channel_id=SLACK_CHANNEL_ID,
    )