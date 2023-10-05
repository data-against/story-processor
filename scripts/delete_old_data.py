from processor.database.stories_db import delete_old_stories
import processor.database as database

Session = database.get_session_maker()
with Session() as session:
    delete_old_stories(session)

