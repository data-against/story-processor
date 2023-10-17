import datetime as dt
import logging

from dateutil.parser import parse
from sqlalchemy import BigInteger, Boolean, DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import processor

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class Story(Base):
    __tablename__ = 'stories'

    id: Mapped[int] = mapped_column(primary_key=True)
    stories_id: Mapped[int] = mapped_column(BigInteger)
    project_id: Mapped[int] = mapped_column(Integer)
    model_id: Mapped[int] = mapped_column(Integer)
    model_score: Mapped[float] = mapped_column(Float)
    model_1_score: Mapped[float] = mapped_column(Float)
    model_2_score: Mapped[float] = mapped_column(Float)
    published_date: Mapped[dt.datetime] = mapped_column(DateTime)
    queued_date: Mapped[dt.datetime] = mapped_column(DateTime)
    processed_date: Mapped[dt.datetime] = mapped_column(DateTime)
    posted_date: Mapped[dt.datetime] = mapped_column(DateTime)
    above_threshold: Mapped[bool] = mapped_column(Boolean)
    source: Mapped[str] = mapped_column(String)
    url: Mapped[str] = mapped_column(String)

    def __repr__(self):
        return '<Story id={} source={}>'.format(self.id, self.source)

    @staticmethod
    def from_source(story, source):
        db_story = Story()
        if source == processor.SOURCE_MEDIA_CLOUD:  # backwards compatability
            db_story.stories_id = story['stories_id']
        db_story.url = story['url']
        db_story.source = source
        # carefully parse date, with fallback to today so we at least get something close to right
        use_fallback_date = False
        try:
            if not isinstance(story['publish_date'], dt.datetime):
                db_story.published_date = parse(story['publish_date'])
            elif story['publish_date'] is not None:
                db_story.published_date = story['publish_date']
            else:
                use_fallback_date = True
        except Exception:
            use_fallback_date = True
        if use_fallback_date:
            db_story.published_date = dt.datetime.now()
            logger.warning("Used today as publish date for story that didn't have date ({}) on it: {}".format(
                story['publish_date'], db_story['url']))
        return db_story


class ProjectHistory(Base):
    __tablename__ = 'projects'

    id: Mapped[int] = mapped_column(primary_key=True)
    last_processed_id: Mapped[int] = mapped_column(BigInteger)
    last_publish_date: Mapped[dt.datetime] = mapped_column(DateTime)
    last_url: Mapped[str] = mapped_column(String)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime)

    def __repr__(self):
        return '<ProjectHistory id={}>'.format(self.id)
