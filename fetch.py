import requests
import feedparser
import time
import threading
import pickle
import sys
import os
from tqdm.auto import tqdm


class Paper:

    def __init__(self, _id, title, version, summary, authors, link, published_date, updated_date, replace_version=True,
                 affiliation=None, no_of_pages=None):
        self._id = _id
        self.title = title
        self.version = version
        self.summary = summary
        self.authors = authors
        self.link = link
        self.replaceVersion = replace_version
        self.affiliation = affiliation
        self.published_date = published_date
        self.updated_date = updated_date
        self.no_of_pages = no_of_pages


class GetPapers:

    def __init__(self, papers_per_call=100, sleep_time=5, max_papers=10000, replace_version=True):
        self.base_url = 'http://export.arxiv.org/api/query?'
        self.search_query = 'cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML'
        self.papers_per_call = papers_per_call
        self.start_index = 0
        self.max_papers = max_papers
        self.sleep_time = sleep_time
        self.paper_versions = {}
        self.available_ids = set()
        self.papers_in_db = []
        self.replaceVersion = replace_version
        self.missed_idx = []
        self.stop_call = False

    def start(self):
        
        db = None
        id_db = None

        try:
            print("Creating file for serializing papers")
            db = open('papers.db','wb+')

            if os.path.exists('ids'):
                with open('ids','rb') as file:
                    self.available_ids = pickle.load(file)

            print(f'Found {len(self.available_ids)} papers in database')

            print("Fetching papers.....\n")

            while not self.stop_call:
                self._fetchPapers()
                self.start_index += self.max_papers
                time.sleep(self.sleep_time)
        
        except Exception as e:
            print(e)
        
        finally:
            if db != None:
                db.close()
            
            print(f"\nParsed {len(self.papers_in_db)} new papers")

            with open('ids', 'wb') as file:
                pickle.dump(self.available_ids,file)

    def _fetchPapers(self):
        
        for index in tqdm(range(self.start_index, self.start_index + self.max_papers, self.papers_per_call)):

            query_string = f"search_query={self.search_query}&start={index}&sortBy=lastUpdatedDate&" \
                f"max_results={self.papers_per_call}"

            final_url = self.base_url + query_string

            res = requests.get(final_url)
            if res.status_code != 200:
                res = self._retry(final_url)
                if not res:
                    print(f"\nMaximum number of attempts reached. Skipping papers from {index} to {index + 100}")
                    self.missed_idx.append(index)
                    continue

            versions, papers = self.parseResponse(res.text)
            self.papers_in_db.extend(papers)
            self.paper_versions = {**self.paper_versions,**versions}

            if self.stop_call: break

    def parseResponse(self, response):
        papers = []
        versions = {}

        parsed_response = feedparser.parse(response)

        if len(parsed_response.entries) < self.papers_per_call:
            self.stop_call = True

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

            papers.append(Paper(_id,
                                title,
                                int(version),
                                summary,
                                authors,
                                link,
                                published_date,
                                updated_date,
                                self.replaceVersion))

            self.available_ids.add(_id)
            versions[_id] = version

        return versions, papers

    def _retry(self, final_url):
        print(
            "\nNo response from server. Will try 3 more times or skip this 100 papers. You can download and parse them manually.")
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
