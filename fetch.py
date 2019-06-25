import requests
import feedparser
import time
import threading
import pickle
import sys
import os
import traceback as tb


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


class ArxivDl:

    def __init__(self, start_index = 0,papers_per_call=100, sleep_time=5, max_papers=10000, replace_version=True):
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

        db = None
        try:
            print("\nCreating file for serializing papers")
            db = open('papers.db', 'wb+')

            if os.path.exists('metadata'):
                with open('metadata', 'rb') as file:
                    self.paper_versions = pickle.load(file)
                    self.available_ids = set(self.paper_versions.keys())

            print(f'\nFound {len(self.available_ids)} papers in database')

            print("\nFetching papers.....\n")

            while not self.stop_call:
                self._fetchPapers()
                self.start_index += self.max_papers
                time.sleep(60)

        except Exception:
            tb.print_exc()

        finally:
            if db is not None:
                pickle.dump(self.papers_in_db,db)
                db.close()

            print(f"\nParsed {len(self.papers_in_db)} new papers")

            with open('metadata', 'wb') as file:
                pickle.dump(self.paper_versions, file)
            

    def _fetchPapers(self):

        for index in range(self.start_index, self.start_index + self.max_papers, self.papers_per_call):

            query_string = f"search_query={self.search_query}&start={index}&sortBy=lastUpdatedDate&" \
                f"max_results={self.papers_per_call}"

            final_url = self.base_url + query_string

            res = requests.get(final_url)
            if res.status_code != 200:
                res = self._retry(final_url)
                if not res:
                    print(f"\nMaximum number of attempts reached. Skipping papers from {index} to {index + self.papers_per_call}")
                    self.missed_idx.append(index)
                    continue
            
            versions, papers = self.parseResponse(res.text)
            self.papers_in_db.extend(papers)
            self.paper_versions = {**self.paper_versions, **versions}

            sys.stdout.write(f"\rFetched {len(papers)} from index {index} to {index+self.papers_per_call}")
            time.sleep(self.sleep_time)

            if self.stop_call: 
                print(f"Arxiv may be rate limiting! Retry after sometime from index {index}")
                break

    def parseResponse(self, response):
        papers = []
        versions = {}

        parsed_response = feedparser.parse(response)
        
        if len(parsed_response.entries) < self.papers_per_call:
            print(f'\n\nGot {len(parsed_response.entries)} papers insted of {self.papers_per_call} papers. Will be '
                  f'terminating in next iteration.')
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
