import processor.database as database
from processor.database.stories_db import delete_old_stories

Session = database.get_session_maker()
with Session() as session:
    delete_old_stories(session)

