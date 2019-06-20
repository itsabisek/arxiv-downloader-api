import requests
import feedparser
import time
import threading
import pickle
# from tqdm.auto import tqdm
import sys
import os



class Paper:

    def __init__(self, _id, title, version, summary, authors, link,affiliation,published_date,updated_date,no_of_pages,replaceVersion=True):
        self._id = _id
        self.title = title
        self.version = version
        self.summary = summary
        self.authors = authors
        self.link = link
        self.replaceVersion = True
        self.affiliation = affiliation
        self.published_date = published_date
        self.updated_date = updated_date
        self.no_of_pages = no_of_pages


class GetPapers:

    def __init__(self, papers_per_call=100, sleep_time=5, max_papers=10000,replaceVersion=True):
        self.base_url = 'http://export.arxiv.org/api/query?'
        self.search_query = 'cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML'
        self.papers_per_call = papers_per_call
        self.start_index = 0
        self.max_papers = max_papers
        self.sleep_time = sleep_time
        self.paper_id_version = {}
        self.replaceVersion = replaceVersion
        self.missed_idx = []

    def start(self):

        print("Starting fetching papers.....\n")
        for index in range(self.start_index, self.max_papers, self.papers_per_call):

            query_string = f"search_query={self.search_query}&start={index}&sortBy=lastUpdatedDate&" \
                                                                                f"max_results={self.papers_per_call} "

            final_url = self.base_url + query_string
            print(final_url)
            res = requests.get(final_url)
            print(res.status_code)
            if res.status_code != 200:
                res = self._retry(final_url)
                if not res:
                    print(f"\nMaximum number of attempts reached. Skipping papers from {index} to {index+100}")
                    self.missed_idx.append(index)
                    continue
            
            _ids,papers = self.parseResponse(res.text)
            self._pickle_papers(papers)
            self.paper_ids = {**self.paper_ids,**_ids}
            sys.stdout.write(f"\r{index + 100} papers are parsed")
            break

    def parseResponse(self, response):
        papers = []
        id_versions = {}
        
        parsed_response = feedparser.parse(response)
        
        for entry in parsed_response.entries:
            link = entry['links'][-1]['href']
            authors = [author['name'] for author in entry['authors']]
            affiliation = entry['affiliation']
            published_date = entry['published']
            updated_date = entry['updated']
            summary = entry['summary']
            no_of_pages = int(entry['arxiv_comment'].split(' ')[0])
            title = entry['title']
            _id,version = link.split('/')[-1].split('v')

            papers.append(Paper(_id,
                                title,
                                int(version),
                                summary,
                                authors,
                                link,
                                affiliation,
                                published_date,
                                updated_date,
                                no_of_pages,
                                self.replaceVersion))

            id_versions[_id] = version
        
        return id_versions,papers

    def _pickle_papers(self,papers):
        
        if not os.path.exists('papers.db'):
            print("No papers are serialized. Serializing...")
            with open('papers.db','wb') as db:
                pickle.dump(papers,db)
        else:
            print("Serializing papers...")
            papers_in_db = []
            with open('papers.db','rb') as db:
                papers_in_db = pickle.load(db)

            with open('papers.db','wb') as db:
                pickle.dump(papers_in_db.extend(papers),db)
        

    def _retry(self,final_url):
        print("No response from server. Will try 3 more times or skip this 100 papers. You can download and parse them manually.")
        for i in range(3):
            sys.stdout.write(f'\rAttempt {i+1}...')
            res = requests.get(final_url)

            if res.status_code == 200:
                print("\nSuccessful")
                return res
        
        return False
