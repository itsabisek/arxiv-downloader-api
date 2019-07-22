import requests
import feedparser
import time
import sys
from datetime import datetime
import pymongo as mongo
from urllib.parse import quote_plus


class Parser:

    def __init__(self, category, start_index=0, papers_per_call=1000, retry=5, replace_version=True):
        self.base_url = 'http://export.arxiv.org/api/query?'
        # self.search_query = 'cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML'
        self.category = category
        self.papers_per_call = papers_per_call
        self.start_index = start_index
        self.available_ids = set()
        self.insert_papers = []
        self.update_papers = {}
        self.paper_versions = {}
        self.counter = retry
        self.connection = None
        self.updated = False
        self.stop_call = False

    def start(self, start_index=0):
        start_index = start_index
        username = quote_plus("abisek")
        password = quote_plus("abisek24")
        mongo_string = 'mongodb+srv://%s:%s@cluster0-fiaze.mongodb.net/arxivdl?authSource=admin&retryWrites=true&w' \
                       '=majority' % (username, password)

        print("Initializing database connection")
        self.connection = mongo.MongoClient(mongo_string)
        print("Connection Established. Fetching metadata...")
        metadata = self.connection.arxivdl.papers

        cursor = metadata.find()
        print(f"Found {cursor.count()} entries in database")
        for entry in cursor:
            self.available_ids.add(entry['paper_id'])
            self.paper_versions[entry['paper_id']] = entry['latest_version']

        while not self.stop_call:
            print(f"Index: {start_index}", end=" ")
            insert_papers, update_papers, pause = self.fetch(start_index)
            total_papers = len(insert_papers) + len(update_papers)
            time.sleep(5)

            if pause and not self.stop_call:
                if self.counter > 0:
                    self.pause()

                else:
                    print("Maximum attempts reached.Stopping")
                    return
            else:
                start_index += self.papers_per_call

            if len(insert_papers) != 0:
                self.insert_papers.extend(insert_papers)
            if len(update_papers) != 0:
                self.update_papers = {**self.update_papers, **update_papers}

    def fetch(self, start_index):
        base_url = self.base_url
        query_string = f"search_query=cat:{self.category}&start={start_index}&sortBy=lastUpdatedDate&" \
            f"max_results={self.papers_per_call}"
        final_url = base_url + query_string

        response = requests.get(final_url)
        if response.status_code != 200:
            raise InvalidResponse("Got 404 Status Code")

        return self.parse(response.text)

    def parse(self, response):
        pause = False
        update = 0
        indb = 0
        insert_papers = []
        update_papers = {}
        parsed_response = feedparser.parse(response)

        if len(parsed_response.entries) == 0:
            return insert_papers, update_papers, True

        if len(parsed_response.entries) < self.papers_per_call:
            pause = True

        for entry in parsed_response.entries:
            home_link = entry['links'][0]['href']
            pdf_link = entry['links'][-1]['href']
            paper_id, paper_version = pdf_link.split('/')[-1].split('v')
            updated_date = datetime.strptime(entry['updated'], '%Y-%m-%dT%H:%M:%SZ')

            if paper_id in self.available_ids:
                if int(paper_version) <= self.paper_versions[paper_id]:
                    indb += 1
                    continue
                else:
                    update_papers[paper_id] = {'latest_version': int(paper_version), 'updated_date': updated_date,
                                               'pdf': pdf_link.strip(), 'home': home_link.strip()}
                    self.paper_versions[paper_id] = int(paper_version)
                    update += 1
                    continue

            self.available_ids.add(paper_id)
            self.paper_versions[paper_id] = int(paper_version)

            tags = [tag['term'] for tag in entry.tags]
            authors = [author['name'] for author in entry['authors']]
            published_date = datetime.strptime(entry['published'], '%Y-%m-%dT%H:%M:%SZ')
            summary = entry['summary']
            title = entry['title']

            insert_papers.append({'paper_id': paper_id.strip(),
                                  'title': title.strip(),
                                  'latest_version': int(paper_version),
                                  'summary': summary.strip(),
                                  'authors': authors,
                                  'pdf': pdf_link.strip(),
                                  'home': home_link.strip(),
                                  'tags': tags,
                                  'published_date': published_date,
                                  'updated_date': updated_date})

        print(f"Fetched: {len(parsed_response.entries)} In DB: {indb} Update: {len(update_papers)} Insert: {len(insert_papers)}")

        assert len(insert_papers) == len(parsed_response.entries) - indb - len(update_papers)
        assert len(update_papers) == update

        if len(insert_papers) == 0 and len(update_papers) == 0:
            print("All papers already in database.Quitting...")
            self.stop_call = True

        return insert_papers, update_papers, pause

    def updateToDb(self):
        papers_db = self.connection.arxivdl.papers
        updated_papers = 0
        if len(self.insert_papers) != 0:
            insert_results = papers_db.insert_many(self.insert_papers)
            updated_papers += len(insert_results.inserted_ids)

        if updated_papers > 0:
            self.updated = True
        print(f"Inserted {updated_papers} papers to database")

        updated_papers = 0
        if len(self.update_papers) != 0:
            for paper_id, paper in self.update_papers.items():
                update_results = papers_db.update_one({'paper_id': paper_id}, {'$set': paper})
                updated_papers += update_results.modified_count

        if updated_papers > 0:
            self.updated = True
        print(f"Updated {updated_papers} papers in database")


    def pause(self, counter=None):
        if not counter:
            counter = self.counter
        print(f"Fetching Paused due to rate limiting.Sleeping for {counter} minutes")
        minutes = counter
        seconds = 0
        print("\n")
        while minutes != -1:
            sys.stdout.write(f"\rTime left : {minutes:02d}:{seconds:02d}")
            time.sleep(1)
            if seconds == 0:
                minutes -= 1
                seconds = 59
            else:
                seconds -= 1
        print("\n")

    def stop(self):
        print("Closing database connections\n")
        if self.connection is not None:
            self.connection.close()


class InvalidResponse(Exception):
    pass
