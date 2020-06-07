import requests
import feedparser
import json
from datetime import datetime
from timer_utils import Timer
from redis_utils import RedisHelper, STOP_SIGNAL, PAUSE_SIGNAL
from logger_utils import bootstrap_logger

fetch_logger = bootstrap_logger(__name__)


class Arxiv:

    BASE_URL = 'http://export.arxiv.org/api/query?'

    def __init__(self, redis_helper, papers_per_call=1000, replace_version=True):
        self.redis_helper = redis_helper
        self.papers_per_call = papers_per_call
        self.replace_version = replace_version

    def run_once(self, categories, index=0):
        if not categories or index < 0:
            return False
        fetched_response = self.fetch_wrapper(categories, index)
        if not fetched_response:
            self.redis_helper.send_pause_signal()
            return True

        parsed_response, pause_fetch, stop_fetch, response_length = self.get_parsed_response(fetched_response.text)

        self.redis_helper.set_signals(PAUSE_SIGNAL, STOP_SIGNAL, values=[pause_fetch, stop_fetch])

        if (pause_fetch or stop_fetch) and response_length == 0:
            return True

        commit_buffer = self.parse_wrapper(parsed_response)
        update_success = self.redis_helper.update_commit_buffer(commit_buffer)
        return update_success

    def fetch_wrapper(self, categories, index):
        category_list = [categories] if isinstance(
            categories, str) else categories
        join_string = "+OR+"
        cat_string = join_string.join(
            ['cat:' + category for category in category_list])

        return self._fetch(cat_string, index)

    def _fetch(self, category_string, index):
        response = None
        base_url = self.BASE_URL
        query_string = f"search_query={category_string}&start={index}&sortBy=lastUpdatedDate&max_results={self.papers_per_call}"
        raw_url = base_url+query_string
        fetch_logger.info(f"Fetching data for url - {raw_url}")
        try:
            response = requests.get(raw_url)
            if response.status_code != 200:
                raise Exception(f"Could not fetch data from url {raw_url}")
            return response
        except Exception as e:
            fetch_logger.exception(e)

    def get_parsed_response(self, response):
        try:
            response = feedparser.parse(response)
            stop_fetch = 0
            pause_fetch = 0
            if len(response.entries) == 0:
                pause_fetch = 1
            if len(response.entries) < self.papers_per_call and len(response.entries) > 0:
                stop_fetch = 1
            fetch_logger.info(f"Fetched {len(response.entries)} entries")
            return response, pause_fetch, stop_fetch, len(response.entries)
        except Exception as e:
            fetch_logger.exception(e)

    def parse_wrapper(self, parsed_response):
        def _get_paper_versions_from_redis():
            versions_dict = self.redis_helper.get_paper_versions_from_redis()
            paper_versions = {}
            for _id, version in versions_dict.items():
                paper_id = _id.decode('utf-8')
                paper_version = int(version.decode('utf-8'))
                paper_versions[paper_id] = paper_version
            return paper_versions

        def _update_paper_versions_to_redis(versions_buffer):
            self.redis_helper.update_paper_versions(versions_buffer)

        if parsed_response:
            if not isinstance(parsed_response, list):
                parsed_response = [parsed_response]
            commit_buffer = []
            try:
                for response in parsed_response:
                    paper_versions = _get_paper_versions_from_redis()
                    temp_commit_buffer, versions_buffer = self._parse(
                        response, paper_versions)
                    _update_paper_versions_to_redis(versions_buffer)
                    if temp_commit_buffer:
                        commit_buffer.extend(temp_commit_buffer)
                    else:
                        self.redis_helper.send_stop_signal()

                fetch_logger.info(f"Commit Buffer length = {len(commit_buffer)}")
                return commit_buffer

            except Exception as e:
                print(e)
                fetch_logger.exception(e)

    def _parse(self, response, paper_versions):
        def _get_entry_data(entry):
            home_link = entry['links'][0]['href'].strip()
            pdf_link = entry['links'][-1]['href'].strip()
            paper_id, paper_version = (lambda pdf_links: (pdf_links[0], int(pdf_links[-1])))(pdf_link.split('/')[-1].split('v'))
            updated_date = datetime.strptime(entry['updated'], '%Y-%m-%dT%H:%M:%SZ').isoformat()
            tags = [tag['term'] for tag in entry.tags]
            authors = [author['name'] for author in entry['authors']]
            published_date = datetime.strptime(entry['published'], '%Y-%m-%dT%H:%M:%SZ').isoformat()
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
        versions_buffer = paper_versions.copy()
        available_ids = set(versions_buffer.keys())
        for entry in response.entries:
            entry_contents = _get_entry_data(entry)
            paper_version = entry_contents['latest_version']
            paper_id = entry_contents['paper_id']

            if paper_id in available_ids:
                if paper_version <= versions_buffer[paper_id]:
                    indb += 1
                    continue
                else:
                    commit_buffer.append(json.dumps({**entry_contents, 'update': True}))
                    versions_buffer[paper_id] = paper_version
                    update += 1
                    continue
            versions_buffer[paper_id] = paper_version
            available_ids.add(paper_id)
            commit_buffer.append(json.dumps({**entry_contents}))
            insert += 1
        fetch_logger.info(f"Parsing Stats - Total={len(response.entries)} :: In DB={indb} :: Update Entries={update} :: Insert Entries={insert}")
        return commit_buffer, versions_buffer
