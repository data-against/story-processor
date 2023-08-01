from sqlalchemy.orm import sessionmaker

from processor import engine

_Session_Maker = None


def _get_session_maker() -> sessionmaker:
    global _Session_Maker
    if _Session_Maker is None:
        _Session_Maker = sessionmaker(bind=engine)
    return _Session_Maker


def get_session_maker() -> sessionmaker:
    return _get_session_maker()
