import requests
import feedparser
from datetime import datetime
from timer_utils import Timer
from redis_utils import RedisHelper
from logger_utils import bootstrap_logger

fetch_logger = bootstrap_logger(__name__)

BASE_URL = 'http://export.arxiv.org/api/query?'


def fetch_wrapper(categories, start_index=0, papers_per_call=1000):
    category_list = [categories] if isinstance(categories, str) else categories
    join_string = "+OR+"
    cat_string = join_string.join(
        ['cat:' + category for category in category_list])

    response = fetch(cat_string, start_index, papers_per_call)
    return get_parsed_response(response.text, papers_per_call)


def fetch(category_string, start_index, papers_per_call):
    response = None
    base_url = BASE_URL
    query_string = f"search_query={category_string}&start={start_index}&sortBy=lastUpdatedDate&max_results={papers_per_call}"
    raw_url = base_url+query_string
    fetch_logger.info(f"Fetching data for url - {raw_url}")
    try:
        response = requests.get(raw_url)
        if response.status_code != 200:
            return Exception(f"Could not fetch data from url {raw_url}")
        return response
    except Exception as e:
        fetch_logger.exception(e)


def get_parsed_response(response, papers_per_call):
    try:
        response = feedparser.parse(response)
        stop_fetch = False
        pause_fetch = False
        if len(response.entries) == 0:
            pause_fetch = True
        if len(response.entries) < papers_per_call and len(response.entries) > 0:
            stop_fetch = True
        fetch_logger.info(f"Fetched {len(response.entries)} entries")
        return response, pause_fetch, stop_fetch
    except Exception as e:
        fetch_logger.exception(e)


def parse_wrapper(response_buffer, redis_helper=None, replace_version=True):
    if not redis_helper:
        redis_helper = RedisHelper()
    if not isinstance(response_buffer, list):
        response_buffer = [response_buffer]
    commit_buffer = []
    try:
        for response in response_buffer:
            paper_versions = redis_helper.get_paper_versions_from_redis()
            temp_commit_buffer, versions_buffer = parse(
                response, paper_versions, replace_version)
            commit_buffer.extend(temp_commit_buffer)
            redis_helper.update_paper_versions(versions_buffer)
        fetch_logger.info(f"Commit Buffer length = {len(commit_buffer)}")
        return commit_buffer
    except Exception as e:
        fetch_logger.exception(e)


def parse(response, paper_versions, replace_version=True):
    def _get_entry_data(entry):
        home_link = entry['links'][0]['href'].strip()
        pdf_link = entry['links'][-1]['href'].strip()
        paper_id, paper_version = (lambda pdf_links: (
            pdf_links[0], int(pdf_links[-1])))(pdf_link.split('/')[-1].split('v'))
        updated_date = datetime.strptime(
            entry['updated'], '%Y-%m-%dT%H:%M:%SZ')
        tags = [tag['term'] for tag in entry.tags]
        authors = [author['name'] for author in entry['authors']]
        published_date = datetime.strptime(
            entry['published'], '%Y-%m-%dT%H:%M:%SZ')
        summary = entry['summary'].strip()
        title = entry['title'].strip()

        entry_contents = {
            'paper_id': paper_id,
            'title': title,
            'latest_version': paper_version,
            'summary': summary,
            'authors': authors,
            'pdf': pdf_link,
            'home': home_link,
            'tags': tags,
            'published_date': published_date,
            'updated_date': updated_date
        }
        return entry_contents

    update = 0
    indb = 0
    insert = 0
    commit_buffer = []
    versions_buffer = paper_versions
    available_ids = set(versions_buffer.keys())
    for entry in response.entries:
        entry_contents = _get_entry_data(entry)
        paper_version = entry_contents['latest_version']
        paper_id = entry_contents['paper_id']

        if paper_id in available_ids:
            if paper_version <= paper_versions[paper_id]:
                indb += 1
                continue
            else:
                commit_buffer.append({**entry_contents, 'update': True})
                versions_buffer[paper_id] = paper_versions
                update += 1
                continue
        versions_buffer[paper_id] = paper_version
        available_ids.add(paper_id)
        commit_buffer.append({**entry_contents})
        insert += 1
    fetch_logger.info(
        f"Parsing Stats - Total={len(commit_buffer)} :: In DB={indb} :: Update Entries={update} :: Insert Entries={insert}")
    return commit_buffer, versions_buffer
