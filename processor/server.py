import logging

from flask import render_template, jsonify
import json
from typing import Dict, List
from itertools import chain

import processor.database as database
from processor import create_flask_app, VERSION, PLATFORMS
from processor.projects import load_project_list
import processor.database.stories_db as stories_db

logger = logging.getLogger(__name__)

app = create_flask_app()


# render helper, see https://stackoverflow.com/questions/34646055/encoding-json-inside-flask-template
def as_pretty_json(value: Dict) -> str:
    return json.dumps(value, indent=4, separators=(',', ': '))
app.jinja_env.filters['as_pretty_json'] = as_pretty_json


@app.route("/", methods=['GET'])
def home():
    projects = load_project_list()
    return render_template('home.html', projects=projects, version=VERSION)


@app.route("/api/update-config", methods=['POST'])
def update_config():
    config = load_project_list(force_reload=True)
    return jsonify(config)


@app.route("/projects/<project_id_str>", methods=['GET'])
def a_project(project_id_str: int):
    # pull out the project info
    project_id = int(project_id_str)
    project = [p for p in load_project_list(download_if_missing=True) if p['id'] == project_id][0]
    Session = database.get_session_maker()
    with Session() as session:
        # show overall ingest over last two weeks
        data_for_graph = _prep_for_stacked_graph(
            [stories_db.stories_by_published_day(session, platform=p, project_id=project_id, limit=30) for p in PLATFORMS],
            PLATFORMS)
        # show some recent story results
        stories_above = stories_db.recent_stories(session, project_id, True)
        stories_below = stories_db.recent_stories(session, project_id, False)
        # some other stats
        unposted_above_story_count = stories_db.unposted_above_story_count(session, project_id)
        posted_above_story_count = stories_db.posted_above_story_count(session, project_id)
        below_story_count = stories_db.below_story_count(session, project_id)
    try:
        above_threshold_pct = 100 * (unposted_above_story_count + posted_above_story_count) / below_story_count
    except ZeroDivisionError:
        above_threshold_pct = 100
    # render it all
    return render_template('project.html',
                           ingest_data=data_for_graph,
                           unposted_above_story_count=unposted_above_story_count,
                           above_threshold_pct=above_threshold_pct,
                           posted_above_story_count=posted_above_story_count,
                           below_story_count=below_story_count,
                           project=project,
                           stories_above=stories_above,
                           stories_below=stories_below,
                           )


def _platform_history(date_type: str, project_id: int = None) -> List[Dict]:
    if date_type == "processed":
        func = stories_db.stories_by_processed_day
    elif date_type == "posted":
        func = stories_db.stories_by_posted_day
    elif date_type == "published":
        func = stories_db.stories_by_published_day
    else:
        raise RuntimeError("invalid `date_type` of '{}'".format(date_type))
    Session = database.get_session_maker()
    with Session() as session:
        data_results = [func(session, project_id=project_id, platform=p) for p in PLATFORMS]
    return _prep_for_stacked_graph(data_results, PLATFORMS)
@app.route("/api/platform-history/<date_type>", methods=['GET'])
def platform_history(date_type: str):
    return jsonify(_platform_history(date_type))
@app.route("/api/projects/<project_id_str>/platform-history/<date_type>", methods=['GET'])
def project_platform_history(project_id_str: str, date_type: str):
    return jsonify(_platform_history(date_type, int(project_id_str)))


def _processed_result_history(project_id: int = None) -> List[Dict]:
    Session = database.get_session_maker()
    with Session() as session:
        data_results = [stories_db.stories_by_processed_day(session, project_id=project_id, above_threshold=True),
                        stories_db.stories_by_processed_day(session, project_id=project_id, above_threshold=False)]
    return _prep_for_stacked_graph(data_results, ['above threshold', 'below threshold'])
@app.route("/api/processed-result-history", methods=['GET'])
def processed_result_history():
    return jsonify(_processed_result_history())
@app.route("/api/projects/<project_id_str>/processed-result-history", methods=['GET'])
def project_processed_result_history(project_id_str: str):
    return jsonify(_processed_result_history(int(project_id_str)))


@app.route("/api/projects/<project_id_str>/binned-model-scores", methods=['GET'])
def project_model_scores(project_id_str):
    project_id = int(project_id_str)
    Session = database.get_session_maker()
    with Session() as session:
        results = stories_db.project_binned_model_scores(session, project_id)
        data = []
        # to go from Decimal to something that can be json serialized
        for row in results:
            data.append(dict(value=float(row['value']), frequency=int(row['frequency'])))
    return jsonify(data)


def _prep_for_stacked_graph(counts: List[List], names: List[str]) -> List[Dict]:
    cleaned_data = [{r['day'].strftime("%Y-%m-%dT00:00:00"): r['stories'] for r in series} for series in counts]
    dates = set(chain(*[series.keys() for series in cleaned_data]))
    stories_by_day_data = []
    for d in dates:  # need to make sure there is a pair of entries for each date
        for idx, series in enumerate(cleaned_data):
            stories_by_day_data.append(dict(
                date=d,
                type=names[idx],
                count=series[d] if d in series else 0
            ))
    return stories_by_day_data


if __name__ == "__main__":
    app.debug = True
    app.run()
