from celery.schedules import crontab

import processor.database as database
from processor.celery import app
from processor.database.stories_db import delete_old_stories


@app.task(name="processor.tasks.delete_old_data.delete_old_stories_task")
def delete_old_stories_task(age: int = 62):
    Session = database.get_session_maker()
    with Session() as session:
        delete_old_stories(session, age)


app.conf.beat_schedule.update(
    {
        "delete_old_stories_task": {
            "task": "processor.tasks.delete_old_data.delete_old_stories_task",
            "schedule": crontab(day_of_week="*", hour="0", minute="0"),
            "args": (30,),
        },
    }
)
