# import os
# import pickle
# import time
#
# from fetch import ArxivDl
#
# with open('categories', 'rb') as file:
#     categories = pickle.load(file)
#
# for category in categories.keys():
#
#     papers_to_save = []
#     start_index = 17000
#     counter = 0
#
#     print(f"\nFetching papers from {categories[category]}......")
#
#     while True:
#         parser = ArxivDl(start_index=start_index, search_query=category)
#         start_index, papers = parser.start()
#         papers_to_save.extend(papers)
#
#         if len(papers) == 0:
#             counter += 1
#         elif len(papers) != 0 and counter > 0:
#             counter = 0
#
#         if counter != 10:
#             print(f"\nCurrently {len(papers)} papers fetched. Will continue after {counter+1} minutes")
#             time.sleep(counter+1 * 60)
#             continue
#         break
#
#     print(f"\nPickle-ing {len(papers_to_save)} papers")
#     file_name = os.path.join(os.path.abspath(os.curdir),'dumps',categories[category]+'.papers')
#     with open(file_name,'wb+') as file:
#         pickle.dump(papers_to_save,file_name)

from fetch_new import Parser
import pickle
import traceback as tb

# with open('categories', 'rb') as file:
#     categories = pickle.load(file)
# try:
#     for tag, name in categories.items():
#         print(f"Fetching papers from {name}")
#         parser = Parser(tag)
#         parser.start()
#         print(f"Fetched {no_of_papers}. Updating them to database....")
#         parser.updateToDb()
#         print(f"Updated {mo_of_papers} to database")

parser = Parser('cs.AI')
try:
    parser.start()
    no_of_papers = len(parser.papers_in_db)
    print(f"Fetched {no_of_papers}. Updating them to database....")
    parser.updateToDb()

except Exception:
    tb.print_exc()

finally:
    parser.stop()
