import os
import pickle
import time

from fetch import ArxivDl

with open('categories', 'rb') as file:
    categories = pickle.load(file)

for category in categories.keys():

    papers_to_save = []
    start_index = 0
    counter = 0

    print(f"\nFetching papers from {categories[category]}......")

    while True:
        parser = ArxivDl(start_index=start_index, search_query=category)
        start_index, papers = parser.start()
        papers_to_save.extend(papers)

        if len(papers) == 0:
            counter += 1
        elif len(papers) != 0 and counter > 0:
            counter = 0

        if counter != 10:
            print(f"\nCurrently {len(papers)} papers fetched. Will continue after 10 minutes")
            time.sleep(600)
            continue
        break

    print(f"\nPickle-ing {len(papers_to_save)} papers")
    file_name = os.path.join(os.path.abspath(os.curdir),'dumps',categories[category]+'.papers')
    with open(file_name,'wb+') as file:
        pickle.dump(papers_to_save,file_name)
