from typing import Optional
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Engine

from processor import SQLALCHEMY_DATABASE_URI

DEFAULT_ENGINE = 'sqlite:///data.db'

# act like singletons
_engine: Optional[Engine] = None
_Session_Maker: Optional[sessionmaker] = None


def _get_engine(reset_pool: bool = False) -> Engine:
    """
    :param force_reset: close the engine and make a new one; helpful for when you know you'll need to get an engine
    a *long* time after it was initially created... which is leading to EOF errors
    :return:
    """
    global _engine
    if _engine:
        if reset_pool:
            _engine.dispose()  # this closes the existing pool and automatically recreates it
        return _engine
    db_uri = DEFAULT_ENGINE if SQLALCHEMY_DATABASE_URI is None else SQLALCHEMY_DATABASE_URI
    if db_uri is DEFAULT_ENGINE:
        _engine = create_engine(db_uri)  # use defaults (probably in test mode)
    else:
        _engine = create_engine(db_uri,
                                # max connections is pool_size + max_overflow, but max_overflow ones don't sleep after
                                # being used
                                pool_size=20, max_overflow=30,
                                # make sure connections actually work when we first make them, rather than when we
                                # first use them
                                pool_pre_ping=True
                                )
    return _engine


def _get_session_maker(reset_pool: bool = False) -> sessionmaker:
    global _Session_Maker
    if _Session_Maker is None:
        _Session_Maker = sessionmaker(bind=_get_engine(reset_pool))
    return _Session_Maker


def get_session_maker(reset_pool: bool = False) -> sessionmaker:
    return _get_session_maker(reset_pool)
