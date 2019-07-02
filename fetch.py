import requests
import feedparser
import time
import pickle
import sys
import os
import traceback as tb
from datetime import datetime


class ArxivDl:

    def __init__(self, start_index=0, papers_per_call=100, sleep_time=5, max_papers=10000, replace_version=True):
        self.base_url = 'http://export.arxiv.org/api/query?'
        self.search_query = 'cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML'
        self.papers_per_call = papers_per_call
        self.start_index = start_index
        self.max_papers = max_papers
        self.sleep_time = sleep_time
        self.paper_versions = {}
        self.available_ids = set()
        self.papers_in_db = []
        self.replaceVersion = replace_version
        self.missed_idx = []
        self.stop_call = False

    def start(self):
        try:
            print("\nSearching for metadata")

            if os.path.exists('metadata'):
                with open('metadata', 'rb') as file:
                    self.paper_versions = pickle.load(file)
                    self.available_ids = set(self.paper_versions.keys())

            print(f'Found {len(self.available_ids)} papers in database')

            print(f"\nFetching papers from index {self.start_index}.....\n")

            while True:
                self._fetchPapers()
                if self.stop_call:
                    break
                self.start_index += self.max_papers
                time.sleep(60)

        except Exception:
            tb.print_exc()

        finally:
            print(f"\nParsed {len(self.papers_in_db)} new papers")

            with open('metadata', 'wb') as file:
                pickle.dump(self.paper_versions, file)

            return self.start_index, self.papers_in_db

    def _fetchPapers(self):

        for index in range(self.start_index, self.start_index + self.max_papers, self.papers_per_call):

            query_string = f"search_query={self.search_query}&start={index}&sortBy=lastUpdatedDate&" \
                f"max_results={self.papers_per_call}"

            final_url = self.base_url + query_string

            res = requests.get(final_url)
            if res.status_code != 200:
                res = self._retry(final_url)
                if not res:
                    print(
                        f"\nMaximum number of attempts reached. Skipping papers from {index} to {index + self.papers_per_call}")
                    self.missed_idx.append(index)
                    continue

            versions, papers = self.parseResponse(res.text)
            sys.stdout.write(f"\rFetched {len(papers)} from index {index} to {index + self.papers_per_call}")

            if len(papers) < self.papers_per_call:
                print(f'\nFound {len(papers)} papers. Arxiv may be rate limiting.'
                      f'Will go to sleep for 3 minutes. Start from index {index}')
                self.stop_call = True

            self.papers_in_db.extend(papers)
            self.paper_versions = {**self.paper_versions, **versions}

            if self.stop_call:
                self.start_index = index
                break

            time.sleep(self.sleep_time)

    def parseResponse(self, response):
        papers = []
        versions = {}

        parsed_response = feedparser.parse(response)

        if len(parsed_response.entries) == 0:
            return versions, papers

        for entry in parsed_response.entries:
            link = entry['links'][-1]['href']
            _id, version = link.split('/')[-1].split('v')

            replace = self._checkIdVersion(_id, version)

            if not replace:
                # versions[_id] = version
                continue

            authors = [author['name'] for author in entry['authors']]
            published_date = entry['published']
            updated_date = entry['updated']
            summary = entry['summary']
            title = entry['title']

            papers.append({'paper_id': _id,
                           'title': title,
                           'version': int(version),
                           'summary': summary,
                           'authors': authors,
                           'link': link,
                           'published_date': datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ'),
                           'updated_date': datetime.strptime(updated_date, '%Y-%m-%dT%H:%M:%SZ')})

            self.available_ids.add(_id)
            versions[_id] = version

        return versions, papers

    def _retry(self, final_url):
        for i in range(3):
            sys.stdout.write(f'\n\rAttempt {i + 1}...')
            res = requests.get(final_url)

            if res.status_code == 200:
                print("\nSuccessful")
                return res

        return False

    def _checkIdVersion(self, _id, version):
        if _id in self.available_ids:
            if version != self.paper_versions[_id] and self.replaceVersion == True:
                return True
            else:
                return False

        return True

    def getPapers(self):
        return self.papers_in_db
